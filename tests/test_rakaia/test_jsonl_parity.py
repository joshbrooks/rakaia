"""The JSONL store held to the durable store's own tests (#229).

`test_django_store.py` covers what the shared conformance suites do not: that
`replay()` over the durable store produces what it produces over the in-memory
one, that a batch is byte-identical to a loop, that an expired stream is really
reaped rather than 404'd forever, and that a fresh instance sees what an earlier
one wrote. Those are backend-independent claims that simply have no home in a
contract, so they are ported here rather than pointed at.

Where the durable store's version inspects ORM rows, this one inspects the
files, for the same reason: a store method can agree with itself about state it
never persisted.
"""

from __future__ import annotations

import json
import time

import pytest

from rakaia import CollectingExecutor
from rakaia.effects import Upsert
from rakaia.jsonl_store import JsonlStreamStore
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay
from rakaia.seed import seed_stream
from rakaia.subscription import poll
from rakaia.types import AppendOptions, InvalidJson, StreamNotFound

PATH = "s"


def _importable_handler(event):
    """A module-level handler: the registry warns about closures, and rightly —
    a registration it cannot re-import is one it cannot rehydrate."""
    return Upsert(model_label="x.X", lookup={"id": event["id"]}, defaults={})


@pytest.fixture
def root(tmp_path):
    return tmp_path / "streams"


@pytest.fixture
def store(root):
    return JsonlStreamStore(root, fsync=False)


class TestReplay:
    """`replay()` is framework-tier and store-agnostic. Prove it here."""

    def _register(self) -> HandlerRegistry:
        reg = HandlerRegistry()

        def h(event):
            return Upsert(
                model_label="x.X",
                lookup={"id": event["id"]},
                defaults={"name": event["name"]},
            )

        reg.register("h", PATH, h, 0, None)
        return reg

    def test_replay_over_the_jsonl_store_matches_memory(self, root):
        events = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        reg = self._register()

        mem = seed_stream(PATH, events)
        mem_ex = CollectingExecutor()
        replay(mem, PATH, mem_ex, handler_registry=reg)

        disk = seed_stream(PATH, events, store=JsonlStreamStore(root, fsync=False))
        disk_ex = CollectingExecutor()
        replay(disk, PATH, disk_ex, handler_registry=reg)

        assert [(e.lookup, e.defaults) for e in disk_ex.effects] == [
            (e.lookup, e.defaults) for e in mem_ex.effects
        ]

    def test_replay_survives_the_process_that_wrote_the_log(self, root):
        """The half of replay parity the in-memory store cannot have.

        The log is written by one store instance and replayed by another that
        shares nothing with it — which is what a rebuild after a restart is.
        """
        events = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        seed_stream(PATH, events, store=JsonlStreamStore(root, fsync=False))

        ex = CollectingExecutor()
        replay(
            JsonlStreamStore(root, fsync=False),
            PATH,
            ex,
            handler_registry=self._register(),
        )

        assert [e.defaults for e in ex.effects] == [{"name": "a"}, {"name": "b"}]


def _records(root, path: str = PATH) -> list[dict]:
    out = []
    for segment in sorted((root / path).glob("*.jsonl")):
        out.extend(json.loads(line) for line in segment.read_text().split("\n") if line)
    return out


def test_append_many_is_byte_identical_to_an_append_loop(root):
    """The acceptance criterion the durable store carries, asked of the files.

    Build one stream each way and compare what is actually on disk, ignoring
    only the two fields that cannot match — the entry id, which is per-stream,
    and the wall-clock append timestamp.
    """
    batch = [
        (b'{"id": 1}', None),
        (b'{"id": 2}', AppendOptions(label="update", metadata={"user": 7})),
        (b'{"id": 3}', AppendOptions(event_ts=1_600_000_000.5)),
    ]

    store = JsonlStreamStore(root, fsync=False)
    store.create("loop")
    for data, options in batch:
        store.append("loop", data, options)
    store.create("bulk")
    returned = store.append_many("bulk", batch)

    def comparable(path):
        # `id` and `offset` are per-stream, and `ts` is wall-clock append time,
        # so the two streams cannot agree on them. Nor can they agree on an
        # `event_ts` neither item set — the store defaults it to append time,
        # which is exactly what makes the two builds differ. It is asserted
        # separately below, where the comparison is meaningful.
        return [
            {k: v for k, v in r.items() if k not in {"id", "offset", "ts", "event_ts"}}
            for r in _records(root, path)
        ]

    assert comparable("bulk") == comparable("loop")

    # The envelope timestamp: an explicit one is preserved exactly by both
    # paths, and an absent one is defaulted to a float by both rather than left
    # null by one of them.
    assert _records(root, "bulk")[2]["event_ts"] == 1_600_000_000.5
    assert _records(root, "loop")[2]["event_ts"] == 1_600_000_000.5
    assert all(
        isinstance(r["event_ts"], float)
        for path in ("bulk", "loop")
        for r in _records(root, path)
    )
    assert [r.message.offset for r in returned if r.message] == [
        "00000000000000000001",
        "00000000000000000002",
        "00000000000000000003",
    ]
    assert [json.loads(m.data) for m in store.read("bulk")[0]] == [
        {"id": 1},
        {"id": 2},
        {"id": 3},
    ]


def test_a_batch_and_a_loop_agree_on_a_flattened_json_array(root):
    """JSON mode flattens one level, and a batch must flatten it the same way a
    loop does — the divergence #214 closed for the other two stores."""
    store = JsonlStreamStore(root, fsync=False)
    store.create("loop", content_type="application/json")
    store.create("bulk", content_type="application/json")

    store.append("loop", b'[{"n": 1}, {"n": 2}]')
    store.append_many("bulk", [(b'[{"n": 1}, {"n": 2}]', None)])

    assert [m.data for m in store.read("bulk")[0]] == [
        m.data for m in store.read("loop")[0]
    ]
    assert len(store.read("bulk")[0]) == 2


class TestExpiryReaping:
    """An expired stream must actually be deleted, not merely reported absent.

    The durable store raised 404s forever without ever removing the row, twice.
    The file-backed failure is the same shape and worse to diagnose: the
    directory stays on disk, so the stream is invisible to the API and visible
    to `ls`.
    """

    def test_a_refused_append_still_reaps_the_expired_stream(self, root):
        store = JsonlStreamStore(root, fsync=False)
        store.create(PATH, ttl_seconds=0)
        time.sleep(0.01)

        with pytest.raises(StreamNotFound):
            store.append(PATH, b'{"id": 1}')

        assert not (root / PATH).exists(), "the expired stream's directory survived"

    def test_a_create_whose_body_is_rejected_still_reaps(self, root):
        """`create`'s reap has to survive the rejection of its own body."""
        store = JsonlStreamStore(root, fsync=False)
        store.create(PATH, content_type="application/json", ttl_seconds=0)
        time.sleep(0.01)

        with pytest.raises(InvalidJson):
            store.create(
                PATH, content_type="application/json", initial_data=b"not json"
            )

        assert not (root / PATH).exists()

    def test_a_reaped_stream_does_not_reissue_its_offsets(self, root):
        """Reaping must retire the high mark, or a recreate hands out offsets a
        subscriber has already consumed."""
        store = JsonlStreamStore(root, fsync=False)
        store.create(PATH, ttl_seconds=1)
        store.append(PATH, b'{"n": 1}')
        store.append(PATH, b'{"n": 2}')
        retired = store.get_current_offset(PATH)

        time.sleep(1.05)
        assert store.get(PATH) is None  # reaped by the read

        store.create(PATH)
        result = store.append(PATH, b'{"n": 3}')
        assert result.message is not None
        assert result.message.offset > retired


def test_durability_across_instances(root):
    """The property the in-memory store lacks, stated as the durable store
    states it."""
    JsonlStreamStore(root, fsync=False).create(PATH)
    JsonlStreamStore(root, fsync=False).append(PATH, b'{"id": 42}')
    messages, _ = JsonlStreamStore(root, fsync=False).read(PATH)
    assert [json.loads(m.data) for m in messages] == [{"id": 42}]


class TestSubscriberCursors:
    """`poll()` needs `get_current_offset`, and reads a saved cursor back.

    A subscriber cursor outliving the process is the whole reason a durable
    store exists, so it is worth one test that the cursor survives the store
    instance that issued it.
    """

    def test_a_cursor_survives_the_instance_that_issued_it(self, root):
        writer = JsonlStreamStore(root, fsync=False)
        writer.create(PATH)
        writer.append(PATH, b'{"n": 1}')

        first = poll(JsonlStreamStore(root, fsync=False), PATH, None)
        assert first.status == "fresh"
        cursor = first.cursor

        assert poll(JsonlStreamStore(root, fsync=False), PATH, cursor).caught_up

        writer.append(PATH, b'{"n": 2}')
        later = poll(JsonlStreamStore(root, fsync=False), PATH, cursor)
        assert later.status == "advanced"
        assert [json.loads(m.data) for m in later.messages] == [{"n": 2}]

    def test_a_cursor_beyond_the_head_reads_as_rewound(self, root):
        """The log was rebuilt shorter — a real event for a file-backed store,
        where "rebuild the log" is `rm -rf` and replay."""
        store = JsonlStreamStore(root, fsync=False)
        store.create(PATH)
        for i in range(5):
            store.append(PATH, json.dumps({"n": i}).encode())
        cursor = poll(store, PATH, None).cursor

        # A rebuild that does not preserve the retired high mark: the directory
        # goes, and with it the watermark.
        for f in (root / PATH).iterdir():
            f.unlink()
        (root / PATH).rmdir()
        (root / ".retired" / PATH).unlink(missing_ok=True)

        rebuilt = JsonlStreamStore(root, fsync=False)
        rebuilt.create(PATH)
        rebuilt.append(PATH, b'{"n": 0}')

        assert poll(rebuilt, PATH, cursor).rewound


class TestTheMetaStreamRegistry:
    """The registry is the framework's own use of a `WritableStore`.

    Its docstring says surviving a *process* restart needs a durable
    meta-stream, and names `DjangoStreamStore` as where that belongs. A
    directory of text files is the other answer, and needs no database.
    """

    def test_registrations_survive_a_new_registry_over_the_same_files(self, root):
        first = HandlerRegistry(store=JsonlStreamStore(root, fsync=False))
        first.register("h", PATH, _importable_handler, 0, None)

        second = HandlerRegistry(store=JsonlStreamStore(root, fsync=False))
        assert second.resolve("h", 0) is not None
