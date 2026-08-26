"""What the JSONL store costs, in files touched (#229).

The durable store has `test_append_query_cost.py` and `test_offset_allocation.py`
holding its query counts flat: an append costs what the previous append cost, and
`append_many` does not grow a query per item. Neither property is visible in the
conformance suites, and both are the difference between a store that works and a
store that works on a stream of six events.

The unit here is a file touched rather than a query issued, and the counters are
monkeypatched around the store's own two I/O seams — the segment reader and the
segment writer — so a test measures what actually reached the filesystem.
"""

from __future__ import annotations

import json

import pytest

from rakaia.jsonl_store import JsonlStreamStore
from rakaia.types import AppendOptions

PATH = "s"


@pytest.fixture
def store(tmp_path):
    s = JsonlStreamStore(tmp_path / "streams", segment_size=10)
    s.create(PATH)
    return s


@pytest.fixture
def segments_read(monkeypatch):
    """Every segment file the store reads, in order."""
    seen: list[str] = []
    original = JsonlStreamStore._read_segment

    def counting(segment):
        seen.append(segment.name)
        return original(segment)

    monkeypatch.setattr(JsonlStreamStore, "_read_segment", staticmethod(counting))
    return seen


@pytest.fixture
def writes(monkeypatch):
    """Every `_flush` call and how many records it carried."""
    seen: list[int] = []
    original = JsonlStreamStore._flush

    def counting(self, path, buffer):
        if buffer:
            seen.append(len(buffer))
        return original(self, path, buffer)

    monkeypatch.setattr(JsonlStreamStore, "_flush", counting)
    return seen


def _fill(store, n: int) -> None:
    store.append_many(
        PATH, [(json.dumps({"n": i}).encode(), AppendOptions()) for i in range(n)]
    )


def test_a_resume_read_skips_the_segments_it_is_past(store, segments_read):
    """The point of segmenting: a reader at the tail does not re-read the head.

    Fifty events at ten to a segment. Resuming from offset 45 wants entries 46
    to 50, which straddle the last two segments — so it may touch those two and
    none of the four before them. A store that scanned would read all six, and
    the cost of a resume would grow with the age of the stream rather than with
    the amount of new data.
    """
    _fill(store, 50)
    segments_read.clear()

    messages, _ = store.read(PATH, "00000000000000000045")

    assert [m.data for m in messages] == [
        json.dumps({"n": i}).encode() for i in range(45, 50)
    ]
    assert segments_read == ["000000000040.jsonl", "000000000050.jsonl"], (
        f"a resume read touched {len(segments_read)} segments: {segments_read}"
    )


def test_a_resume_read_is_flat_in_the_age_of_the_stream(store, segments_read):
    """The same tail costs the same read whether it sits at event 10 or 1000.

    This is the property the filename index exists for: what a resume costs is
    set by how much is new, not by how much came before it.
    """
    _fill(store, 10)
    segments_read.clear()
    store.read(PATH, "00000000000000000005")
    early = len(segments_read)

    _fill(store, 990)
    segments_read.clear()
    store.read(PATH, "00000000000000000995")
    late = len(segments_read)

    assert early == late, f"early read touched {early}, late read {late}"
    assert late <= 2, f"a five-event tail touched {late} segments"


def test_a_full_read_still_reads_everything(store, segments_read):
    """Skipping is for resumes. A read with no offset has to see it all —
    the skip rule must not quietly drop the head of the stream."""
    _fill(store, 25)
    segments_read.clear()

    messages, _ = store.read(PATH)

    assert len(messages) == 25
    assert len(segments_read) == 3


def test_a_read_from_before_the_first_segment_boundary_reads_it(store, segments_read):
    """The off-by-one that a `>=` here would cause: resuming from offset 10
    must still read the segment that *starts* at 10."""
    _fill(store, 25)
    segments_read.clear()

    messages, _ = store.read(PATH, "00000000000000000010")

    assert len(messages) == 15
    assert segments_read == ["000000000010.jsonl", "000000000020.jsonl"]


@pytest.mark.parametrize("batch", [1, 10, 100])
def test_append_many_is_flat_in_the_batch_size(store, writes, batch):
    """One flush for the whole batch, whatever its size.

    The durable store's equivalent is `test_append_many_is_flat_in_the_batch_size`,
    holding its query count independent of the item count. Here the promise is
    the same shape: a batch is one lock, one buffer and one write per segment it
    spans — not one per item.
    """
    _fill(store, batch)

    assert len(writes) == 1, f"a batch of {batch} took {len(writes)} flushes"
    assert writes[0] == batch


def test_a_batch_writes_once_per_segment_it_spans(store, writes):
    """A batch crossing a roll-over pays one write per file, not one per record."""
    _fill(store, 35)

    assert len(writes) == 1  # one flush...
    assert writes[0] == 35


def test_a_refused_batch_writes_nothing(store, writes):
    """The cost side of all-or-nothing: a refusal must not reach the filesystem."""
    from rakaia.types import SequenceConflict

    store.append(PATH, b'{"n": 0}', AppendOptions(seq="005"))
    writes.clear()

    with pytest.raises(SequenceConflict):
        store.append_many(
            PATH,
            [
                (b'{"n": 1}', AppendOptions(seq="006")),
                (b'{"n": 2}', AppendOptions(seq="001")),
            ],
        )

    assert writes == []


def test_an_append_costs_what_the_previous_append_cost(store, writes, segments_read):
    """No aggregate over the log. The fiftieth append reads and writes what the
    second did — the head comes from the metadata file, not from a scan."""
    store.append(PATH, b'{"n": 0}')
    writes.clear()
    segments_read.clear()
    store.append(PATH, b'{"n": 1}')
    second = (len(writes), len(segments_read))

    _fill(store, 200)
    writes.clear()
    segments_read.clear()
    store.append(PATH, b'{"n": 2}')
    later = (len(writes), len(segments_read))

    assert second == later == (1, 0), f"second={second} later={later}"
