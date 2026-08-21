"""Every write path records its consequences through one method, in one order.

#183: the durable store had four entry points that write an event, each
repeating the same nine steps — expire, transaction, lock, admit, write, record
the producer, save the sequence, close if asked, touch the clock. Two of them
were near line-for-line copies. The order of those steps is a *rule*, and it was
written down nowhere, so the only way to learn it was to read three
implementations. That had already gone wrong: one entry point admitted less than
the others (#154), and another never read `closed_by` (#167).

Admission became shared first (#167 for a single write, #181 for a batch). What
was left was the recording half, which is what `_record_write` now owns and this
file pins. The two claims here are different in kind:

* **Routing** — every write path goes *through* that method. This is the tripwire
  a fifth entry point trips: hand-rolling the four steps still passes every
  behavioural test in the suite, exactly as the two copies did for months.
* **Order** — the steps happen in the order the method documents, asserted from
  the outside where the order is observable.
"""

from __future__ import annotations

import pytest
from django.db import connection
from django.test.utils import CaptureQueriesContext

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import Stream, StreamEntry, StreamProducer
from rakaia.types import AppendOptions, ProducerDuplicate, ProducerStaleEpoch

pytestmark = pytest.mark.django_db


def _producer(pid: str, epoch: int, seq: int, **kw) -> AppendOptions:
    return AppendOptions(producer_id=pid, producer_epoch=epoch, producer_seq=seq, **kw)


class TestEveryWritePathRecordsThroughOneMethod:
    """The routing claim, per entry point.

    Asserted by counting calls rather than by checking the rows: the rows are
    identical whether a path calls the shared method or repeats its four steps
    inline, which is precisely why the copies survived. What must be true is that
    there is only one copy left.
    """

    @pytest.fixture
    def counted(self, monkeypatch: pytest.MonkeyPatch) -> list[dict]:
        calls: list[dict] = []
        original = DjangoStreamStore._record_write

        def recording(self, stream, **kwargs):
            calls.append(kwargs)
            return original(self, stream, **kwargs)

        monkeypatch.setattr(DjangoStreamStore, "_record_write", recording)
        return calls

    def test_a_plain_append_records_through_it(self, counted: list[dict]) -> None:
        store = DjangoStreamStore()
        store.create("s", content_type="application/json")

        store.append("s", b'{"n": 1}')

        assert len(counted) == 1

    def test_a_fenced_append_records_through_it(self, counted: list[dict]) -> None:
        store = DjangoStreamStore()
        store.create("s", content_type="application/json")

        store.append("s", b'{"n": 1}', _producer("p", 1, 0))

        assert len(counted) == 1

    def test_a_batch_records_through_it_once_for_the_whole_batch(
        self, counted: list[dict]
    ) -> None:
        """Once, not once per item: the recording is per *write*, and a batch is
        one write. Per-item would restore the query cost `append_many` exists to
        avoid."""
        store = DjangoStreamStore()
        store.create("s", content_type="application/json")

        store.append_many(
            "s", [(b'{"n": 1}', AppendOptions()), (b'{"n": 2}', AppendOptions())]
        )

        assert len(counted) == 1

    def test_a_refused_write_records_nothing(self, counted: list[dict]) -> None:
        """Nothing landed, so there is nothing whose consequences to record —
        and advancing the producer here would reject the retry that legitimately
        follows."""
        store = DjangoStreamStore()
        store.create("s", content_type="application/json")
        store.close_stream("s")

        result = store.append("s", b'{"n": 1}')

        assert result.stream_closed and result.message is None
        assert counted == []


class TestAFencedAppendIsAnAppend:
    """The 39 duplicated lines, gone.

    `_append_with_producer_sync` ran its own copy of the same nine steps. It is
    now `append`, and this is what stops the copy coming back: the assertion is
    about the call, because a reintroduced copy would produce identical rows.
    """

    def test_the_fenced_entry_point_delegates(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        store = DjangoStreamStore()
        store.create("s", content_type="application/json")
        seen: list[tuple] = []
        original = DjangoStreamStore.append

        def recording(self, path, data, options=None):
            seen.append((path, data, options))
            return original(self, path, data, options)

        monkeypatch.setattr(DjangoStreamStore, "append", recording)

        opts = _producer("p", 1, 0)
        store._append_with_producer_sync("s", b'{"n": 1}', opts)

        assert seen == [("s", b'{"n": 1}', opts)]

    def test_it_still_fences(self) -> None:
        """Delegating must not have dropped the fencing — the reason the method
        looked separate. `decide_append` consults the producer's state for any
        options carrying the triple, whichever door they came through."""
        store = DjangoStreamStore()
        store.create("s", content_type="application/json")

        first = store._append_with_producer_sync("s", b'{"n": 1}', _producer("p", 1, 0))
        replay = store._append_with_producer_sync(
            "s", b'{"n": 1}', _producer("p", 1, 0)
        )
        stale = store._append_with_producer_sync("s", b'{"n": 2}', _producer("p", 0, 1))

        assert first.message is not None
        assert isinstance(replay.producer_result, ProducerDuplicate)
        assert isinstance(stale.producer_result, ProducerStaleEpoch)
        assert StreamEntry.objects.count() == 1, "only the first write landed"


class TestTheOrderIsTheRule:
    """The steps whose position is observable from outside.

    Not all four are. `_record_write` runs inside one transaction, so swapping
    the close and the TTL touch produces identical rows — that ordering is
    rationale for a reader, argued in the method's docstring, and deliberately
    not asserted here. Claiming a test for it would be worse than having none:
    the two steps below *are* pinned, and each was checked by breaking it.
    """

    def test_a_closing_append_lands_its_own_event(self) -> None:
        """The close and the event it rides on are one atomic step: the caller
        gets a message *and* `stream_closed`, not one or the other."""
        store = DjangoStreamStore()
        store.create("s", content_type="application/json")

        result = store.append("s", b'{"n": 1}', AppendOptions(close=True))

        assert result.message is not None, "the closing append's own event landed"
        assert result.stream_closed is True
        assert StreamEntry.objects.count() == 1
        assert Stream.objects.get(stream_id="s").closed is True

    def test_a_refused_fence_leaves_the_producer_where_it_was(self) -> None:
        """Step 1 is conditional on the write, not on the attempt. If a refused
        attempt advanced the row, the producer's next legitimate sequence would
        look like a duplicate."""
        store = DjangoStreamStore()
        store.create("s", content_type="application/json")
        store.append("s", b'{"n": 1}', _producer("p", 1, 0))
        before = StreamProducer.objects.get(producer_id="p").last_seq

        gap = store.append("s", b'{"n": 9}', _producer("p", 1, 7))

        assert gap.message is None
        assert StreamProducer.objects.get(producer_id="p").last_seq == before
        # …and the sequence it skipped to is still available.
        assert store.append("s", b'{"n": 2}', _producer("p", 1, 1)).message is not None

    def test_the_sequence_is_saved_only_when_it_moves(self) -> None:
        """Step 2 is conditional, and the cost is the only way to see it.

        Saving unconditionally writes `None` over `None` for an append that sent
        no `Stream-Seq` — same row, one extra statement, on the majority of
        appends. So this counts statements rather than reading the column: a test
        that only checked the value would pass with the condition deleted.
        """
        store = DjangoStreamStore()
        store.create("s", content_type="application/json")
        store.append("s", b'{"n": 0}')  # warm any lazy per-connection queries

        with CaptureQueriesContext(connection) as without:
            store.append("s", b'{"n": 1}')
        with CaptureQueriesContext(connection) as with_seq:
            store.append("s", b'{"n": 2}', AppendOptions(seq="5"))

        def stream_updates(ctx) -> list[str]:
            return [
                q["sql"]
                for q in ctx.captured_queries
                if (q["sql"] or "").upper().startswith("UPDATE")
                and "rakaia_stream" in (q["sql"] or "")
                and "last_seq" in (q["sql"] or "")
            ]

        assert stream_updates(without) == []
        assert len(stream_updates(with_seq)) == 1
        assert Stream.objects.get(stream_id="s").last_seq == "5"

    def test_a_batch_commits_one_producer_row_for_many_items(self) -> None:
        """Step 1, batched: the last accepted outcome per producer is the only
        state a later writer can be fenced against, so that is what is written.
        One row, carrying the last item's sequence."""
        store = DjangoStreamStore()
        store.create("s", content_type="application/json")

        store.append_many(
            "s", [(b'{"n": %d}' % i, _producer("p", 1, i)) for i in range(3)]
        )

        rows = StreamProducer.objects.filter(producer_id="p")
        assert rows.count() == 1
        assert rows.get().last_seq == 2
