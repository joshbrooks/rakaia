"""What the JSONL spike claims beyond the shared contracts (#229).

The two conformance suites in `test_jsonl_store_contract.py` prove the store
*is* a `StreamServerStore`. They cannot prove the four things this layout was
chosen for, because they are questions no in-memory or database backend has:
crash recovery from a torn line, a refused batch leaving nothing behind,
roll-over by id range, and a second process seeing the log at all.
"""

from __future__ import annotations

import json

import pytest

from rakaia.jsonl_store import JsonlStreamStore
from rakaia.offsets import ForeignOffset
from rakaia.types import AppendOptions, SequenceConflict


@pytest.fixture
def root(tmp_path):
    return tmp_path / "streams"


@pytest.fixture
def store(root):
    return JsonlStreamStore(root, segment_size=4)


def test_segments_roll_over_by_id_range(store, root):
    """Ids 1..3 land in the first segment, 4..7 in the second (size 4).

    The filename is the id range, which is what lets a read seek and a retention
    policy delete a whole file.
    """
    store.create("s")
    for i in range(7):
        store.append("s", f'{{"n": {i}}}'.encode())

    segments = sorted(p.name for p in (root / "s").glob("*.jsonl"))
    assert segments == ["000000000000.jsonl", "000000000004.jsonl"]

    first = (root / "s" / "000000000000.jsonl").read_text().strip().split("\n")
    assert [json.loads(line)["id"] for line in first] == [1, 2, 3]


def test_the_log_is_readable_as_text(store, root):
    """A JSON payload is stored as itself, not as base64 of itself."""
    store.create("s")
    store.append("s", b'{"who": "kea"}', AppendOptions(label="created"))

    line = json.loads((root / "s" / "000000000000.jsonl").read_text().strip())
    assert line["data"] == '{"who": "kea"}'
    assert line["label"] == "created"
    assert line["b64"] is False


def test_a_second_store_over_the_same_directory_sees_the_log(store, root):
    """The claim that retires the in-memory store: another process can read it.

    A separate `JsonlStreamStore` instance shares no state with the first — no
    dict, no `asyncio.Event`, nothing but the directory. This is what an
    in-memory store cannot do at any speed, and what pointing the root at a
    tmpfs mount gets you for free.
    """
    store.create("s")
    store.append("s", b'{"n": 1}')

    other = JsonlStreamStore(root, segment_size=4)
    messages, _ = other.read("s")
    assert [m.data for m in messages] == [b'{"n": 1}']

    other.append("s", b'{"n": 2}')
    assert len(store.read("s")[0]) == 2


def test_the_head_survives_a_lost_meta_file(store, root):
    """`meta.json` is a cache; the log is authoritative.

    Deleting the metadata must cost a scan, not the stream — which is the whole
    reason the head is recoverable from the last complete line.
    """
    store.create("s")
    for i in range(5):
        store.append("s", f'{{"n": {i}}}'.encode())
    head = store.get_current_offset("s")

    (root / "s" / "meta.json").unlink()

    recovered = JsonlStreamStore(root, segment_size=4)
    assert recovered.get_current_offset("s") == head
    assert len(recovered.read("s")[0]) == 5


def test_a_torn_trailing_line_is_ignored_and_overwritten(store, root):
    """A crash mid-append leaves a partial line. It is not a parse error.

    The next append continues past it, so the log self-heals rather than
    needing a repair pass.
    """
    store.create("s")
    store.append("s", b'{"n": 1}')
    with (root / "s" / "000000000000.jsonl").open("a") as fh:
        fh.write('{"id": 2, "offset": "000000')  # power cut, mid-write

    messages, _ = store.read("s")
    assert [m.data for m in messages] == [b'{"n": 1}']

    store.append("s", b'{"n": 2}')
    assert [m.data for m in store.read("s")[0]] == [b'{"n": 1}', b'{"n": 2}']


def test_a_refused_batch_leaves_no_prefix_on_disk(store, root):
    """#214, asked of a filesystem.

    The batch is refused by its *third* item, so a store that looped `append`
    would have already written the first two — and, worse, would have left them
    on disk with no transaction to roll them back.
    """
    store.create("s")
    store.append("s", b'{"n": 0}', AppendOptions(seq="005"))

    with pytest.raises(SequenceConflict):
        store.append_many(
            "s",
            [
                (b'{"n": 1}', AppendOptions(seq="006")),
                (b'{"n": 2}', AppendOptions(seq="007")),
                (b'{"n": 3}', AppendOptions(seq="001")),  # <= last accepted
            ],
        )

    assert len(store.read("s")[0]) == 1
    lines = (root / "s" / "000000000000.jsonl").read_text().strip().split("\n")
    assert len(lines) == 1


def test_a_whole_batch_is_one_write(store):
    """The batch is atomic *and* cheap: one lock, one write per segment."""
    store.create("s")
    results = store.append_many(
        "s", [(f'{{"n": {i}}}'.encode(), AppendOptions(label="n")) for i in range(6)]
    )
    assert len(results) == 6
    assert [m.data for m in store.read("s")[0]] == [
        f'{{"n": {i}}}'.encode() for i in range(6)
    ]


def test_offsets_resume_above_the_retired_high_mark(store):
    """#34, asked of a filesystem: delete-and-recreate must not reissue offsets."""
    store.create("s")
    store.append("s", b'{"n": 1}')
    store.append("s", b'{"n": 2}')
    retired = store.get_current_offset("s")

    store.delete("s")
    store.create("s")

    assert store.get_current_offset("s") == retired
    result = store.append("s", b'{"n": 3}')
    assert result.message is not None
    assert result.message.offset > retired


def test_an_in_memory_offset_is_refused_rather_than_resolved(store):
    """The offset-format hazard from the scoping note, made visible.

    This store issues `PLAIN` tokens, so it can still tell the *compound* ones
    apart. What it could not tell apart is another `PLAIN`-issuing store's
    cursor — which is the open question in #229, not something this test can
    close.
    """
    store.create("s")
    store.append("s", b'{"n": 1}')
    with pytest.raises(ForeignOffset):
        store.read("s", "0000000000000000_0000000000000008")


@pytest.mark.asyncio
async def test_the_protocol_server_runs_on_it(store):
    """The point of the spike, end to end.

    `create_app` is typed against `StreamServerStore` and has only ever been run
    on the in-memory store and the Django one. Here it serves a real HTTP round
    trip with nothing under it but a directory of text files.
    """
    from tests.asgi_client import asgi_client

    async with asgi_client(store) as ac:
        created = await ac.put("/s", headers={"content-type": "application/json"})
        assert created.status_code in (200, 201)

        posted = await ac.post(
            "/s", content=b'{"n": 1}', headers={"content-type": "application/json"}
        )
        assert posted.status_code in (200, 201, 204)

        got = await ac.get("/s")
        assert got.status_code == 200
        assert json.loads(got.content) == [{"n": 1}]
