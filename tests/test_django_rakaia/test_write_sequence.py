"""What a durable append leaves behind, asserted from the outside.

#183: the durable store had four entry points that write an event, each
repeating the same steps — admit, write, record the producer, save the sequence,
close if asked, touch the clock — and the copies drifted (#154, #167). #321 moved
all of the deciding into `rakaia.append_decision.plan_appends`, which is pinned
by table tests in `tests/test_rakaia/test_append_decision.py`; every write path
is now a plan applied by one method, so the routing tripwire that lived here has
nothing left to catch. What stays is the behaviour the plan promises, observed on
the database.
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


class TestAFencedAppendIsAnAppend:
    """A fenced append is `append` carrying the producer triple; there is no
    second entry point for it to drift from (#183, then #323 removed the
    async one the server used)."""

    def test_it_still_fences(self) -> None:
        """`decide_append` consults the producer's state for any options
        carrying the triple."""
        store = DjangoStreamStore()
        store.create("s", content_type="application/json")

        first = store.append("s", b'{"n": 1}', _producer("p", 1, 0))
        replay = store.append("s", b'{"n": 1}', _producer("p", 1, 0))
        stale = store.append("s", b'{"n": 2}', _producer("p", 0, 1))

        assert first.message is not None
        assert isinstance(replay.producer_result, ProducerDuplicate)
        assert isinstance(stale.producer_result, ProducerStaleEpoch)
        assert StreamEntry.objects.count() == 1, "only the first write landed"


class TestTheOrderIsTheRule:
    """The steps whose position is observable from outside.

    The stream row's own changes — sequence, close, TTL — are one save since
    #321, so there is no order among them left to observe. The two below *are*
    observable, and each was checked by breaking it.
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
