"""What the durable offset allocation costs, and what it must keep costing.

`Stream.get_next_offset_block` is the single allocation path behind every
durable write surface — the store's single and bulk append, `@stream_model`
signals, the protocol views — so a query it issues is a query every append pays.
These cases pin the count *and* the properties the count must not be bought
with: monotonicity, the one watermark lock, and correct seeding on an install
that predates the high-water table.

The seeding case is why the count was what it was. Migration 0005 creates
`StreamOffsetWatermark` but does **not** backfill it, so on an upgraded install
the watermark starts at 0 while `entries` already holds offsets. The
`max(entries, watermark)` aggregate is what stopped the first post-migration
allocation from reissuing offset 1 over the top of existing rows. It is
load-bearing exactly once per stream, and it used to run on every append
forever.
"""

from __future__ import annotations

import pytest
from django.db import connection
from django.test.utils import CaptureQueriesContext

from django_rakaia.models import Stream, StreamEntry, StreamEvent, StreamOffsetWatermark
from django_rakaia.offsets import format_offset

pytestmark = pytest.mark.django_db


def _aggregate_queries(ctx: CaptureQueriesContext) -> list[str]:
    """The captured queries that aggregate over `rakaia_streamentry`.

    Matched on `MAX(` plus the entry table rather than on an exact SQL string,
    so the assertion survives a backend whose quoting differs (SQLite's
    double quotes vs Postgres's) and still fails if the aggregate comes back
    under any spelling.
    """
    return [
        q["sql"]
        for q in ctx.captured_queries
        if "MAX(" in q["sql"].upper() and "rakaia_streamentry" in q["sql"]
    ]


class TestAllocationQueryCount:
    def test_a_steady_state_allocation_does_not_aggregate_over_entries(self):
        # The point of the whole change. Once the watermark has been advanced
        # even once it is authoritative, so re-deriving the high mark from the
        # entry table is a scan of a growing table for an answer already held
        # in a single row.
        stream = Stream.objects.create(stream_id="s")
        stream.get_next_offset_block(1)  # advance the watermark off zero

        with CaptureQueriesContext(connection) as ctx:
            stream.get_next_offset_block(1)

        assert _aggregate_queries(ctx) == []

    def test_the_first_allocation_for_a_path_still_aggregates(self):
        # The gate's other half, stated so that removing the aggregate outright
        # fails here. A watermark still at zero cannot distinguish "brand new"
        # from "upgraded install with rows already in entries", so it has to
        # look.
        stream = Stream.objects.create(stream_id="s")

        with CaptureQueriesContext(connection) as ctx:
            stream.get_next_offset_block(1)

        assert len(_aggregate_queries(ctx)) == 1

    def test_a_steady_state_allocation_reads_the_watermark_once(self):
        # `get_or_create` then `select_for_update().get()` read the same row
        # twice: the first read exists only to make the row exist, and its
        # result was thrown away. One locked `get_or_create` does both jobs.
        stream = Stream.objects.create(stream_id="s")
        stream.get_next_offset_block(1)

        with CaptureQueriesContext(connection) as ctx:
            stream.get_next_offset_block(1)

        reads = [
            q["sql"]
            for q in ctx.captured_queries
            if q["sql"].lstrip().upper().startswith("SELECT")
            and "rakaia_streamoffsetwatermark" in q["sql"]
        ]
        assert len(reads) == 1, reads

    def test_a_steady_state_allocation_costs_two_queries(self):
        # The headline number, asserted exactly rather than as a ceiling: one
        # locked read of the watermark, one write of it. Anything else on this
        # path is paid by every append on every write surface, so a third query
        # should have to be argued for rather than slipped in.
        stream = Stream.objects.create(stream_id="s")
        stream.get_next_offset_block(1)

        with CaptureQueriesContext(connection) as ctx:
            stream.get_next_offset_block(1)

        assert len(ctx.captured_queries) == 2, [q["sql"] for q in ctx.captured_queries]


@pytest.mark.skipif(
    not connection.features.has_select_for_update,
    reason=(
        "backend has no row locks, so select_for_update() compiles to a plain "
        "SELECT and the emitted SQL cannot answer this -- run with "
        "RAKAIA_TEST_DB=postgres"
    ),
)
class TestTheLockSurvivesGetOrCreate:
    """That the *one* remaining read is still a locking one.

    Folding the plain `get_or_create` and the `select_for_update().get()` into a
    single locked `get_or_create` is only safe if Django applies the queryset's
    `FOR UPDATE` to the `get` half. It does — but nothing in the default test run
    can see that, because SQLite reports `has_select_for_update` false and Django
    then omits the clause entirely. Spying on `QuerySet.select_for_update` (as
    `test_django_store.py` does) proves the method was *called*, not that the
    clause reached the database.

    So this reads the SQL. It is the only assertion in the file that can tell a
    real lock from a call that was silently dropped, and it is the reason this
    change has to be run against Postgres before it is believed.
    """

    @pytest.mark.django_db(transaction=True)
    def test_the_watermark_read_is_a_locking_read(self):
        from django.db import transaction as db_transaction

        Stream.objects.create(stream_id="s")
        with db_transaction.atomic():
            stream = Stream.objects.get(stream_id="s")
            stream.get_next_offset_block(1)  # off zero, so the next is steady state
            with CaptureQueriesContext(connection) as ctx:
                stream.get_next_offset_block(1)

        watermark_reads = [
            q["sql"]
            for q in ctx.captured_queries
            if "rakaia_streamoffsetwatermark" in q["sql"]
            and q["sql"].lstrip().upper().startswith("SELECT")
        ]
        assert len(watermark_reads) == 1, watermark_reads
        assert "FOR UPDATE" in watermark_reads[0].upper(), watermark_reads[0]


class TestTheReadSideAgrees:
    """`Stream.current_offset` answers the same question from the same source.

    It carried its own copy of `max(entries, watermark)`, so the read side had
    the identical scan on the identical growing table — and would have kept it
    after the write side stopped. Reported here rather than left as a follow-up
    because #34/#59 already went that way once: the write side of a store
    invariant was fixed and the read side was missed, and the two then disagreed
    about where the stream head was.
    """

    def test_reading_the_head_does_not_aggregate_over_entries(self):
        stream = Stream.objects.create(stream_id="s")
        stream.get_next_offset_block(1)

        with CaptureQueriesContext(connection) as ctx:
            head = stream.current_offset

        assert head == format_offset(1)
        assert _aggregate_queries(ctx) == []

    def test_the_head_matches_what_allocation_handed_out(self):
        # The symmetry that matters: the two sides must name the same position.
        # `current_offset` is "the offset of the last event", i.e. one below the
        # next allocation.
        stream = Stream.objects.create(stream_id="s")
        stream.get_next_offset_block(4)
        assert stream.current_offset == format_offset(4)
        assert stream.get_next_offset_block(1) == 5

    def test_an_upgraded_install_reads_its_existing_entries(self):
        # The read side's version of the seeding case: entries exist, no
        # watermark row does, and the head must still be 7 rather than 0.
        stream = Stream.objects.create(stream_id="s")
        for offset in (1, 2, 7):
            _entry_at(stream, offset)

        assert stream.current_offset == format_offset(7)

    def test_a_watermark_row_sitting_at_zero_still_falls_back(self):
        # The read side has two ways to be unseeded and they are not the same
        # value: no row at all reads as `None`, a row that exists but has never
        # been advanced reads as `0`. The gate is a truthiness test so it covers
        # both — writing it as `is not None` returns a head of 0 beside entries
        # running to 7, and every other test in the file stays green.
        #
        # The zero-row state is reachable: allocation outside a transaction used
        # to autocommit the `get_or_create` and only then raise on the locked
        # read, leaving the row behind at zero.
        stream = Stream.objects.create(stream_id="s")
        StreamOffsetWatermark.objects.create(stream_path="s", high=0)
        for offset in (1, 2, 7):
            _entry_at(stream, offset)

        assert stream.current_offset == format_offset(7)

    def test_allocation_also_seeds_past_a_zero_watermark_row(self):
        # The write side's version. It has no None/0 distinction — `get_or_create`
        # hands back `high == 0` either way — but the state is the same one, and
        # the two sides agreeing on it is the property this PR is about.
        stream = Stream.objects.create(stream_id="s")
        StreamOffsetWatermark.objects.create(stream_path="s", high=0)
        for offset in (1, 2, 7):
            _entry_at(stream, offset)

        assert stream.get_next_offset_block(1) == 8

    def test_a_fresh_stream_reads_zero(self):
        stream = Stream.objects.create(stream_id="s")
        assert stream.current_offset == format_offset(0)

    def test_the_head_survives_delete_and_recreate(self):
        stream = Stream.objects.create(stream_id="s")
        stream.get_next_offset_block(3)
        Stream.objects.filter(stream_id="s").delete()

        recreated = Stream.objects.create(stream_id="s")
        assert recreated.current_offset == format_offset(3)


def _entry_at(stream: Stream, offset: int) -> None:
    """Write an entry directly at `offset`, as a pre-0005 install's rows look.

    Deliberately bypasses `get_next_offset_block` — that is the whole scenario:
    rows that exist without ever having advanced a watermark, because the
    watermark table did not exist when they were written.
    """
    StreamEntry.objects.create(
        stream=stream,
        event=StreamEvent.objects.create(data={"n": offset}, event_type="append"),
        offset=offset,
    )


class TestAllocationIsCorrect:
    """The numbers, not the query count.

    Split from the count assertions on purpose: an allocation that issues the
    right *queries* and the wrong *offsets* is worse than the one it replaced,
    and a file that only counted queries could not tell the difference.
    """

    def test_a_fresh_stream_starts_at_one(self):
        stream = Stream.objects.create(stream_id="s")
        assert stream.get_next_offset_block(1) == 1

    def test_successive_allocations_do_not_repeat_or_skip(self):
        stream = Stream.objects.create(stream_id="s")
        assert [stream.get_next_offset_block(1) for _ in range(5)] == [1, 2, 3, 4, 5]

    def test_a_block_reserves_contiguously_and_the_next_starts_past_it(self):
        stream = Stream.objects.create(stream_id="s")
        assert stream.get_next_offset_block(4) == 1
        assert stream.get_next_offset_block(1) == 5

    def test_an_upgraded_install_resumes_above_its_existing_entries(self):
        # The seeding case, with values. Migration 0005 leaves a watermark at 0
        # beside entries that already run to 7; allocating from the watermark
        # alone would hand out 1 and collide with the row already there.
        stream = Stream.objects.create(stream_id="s")
        for offset in (1, 2, 7):
            _entry_at(stream, offset)
        assert StreamOffsetWatermark.objects.filter(stream_path="s").count() == 0

        assert stream.get_next_offset_block(1) == 8

    def test_the_seeded_high_mark_persists_to_the_next_allocation(self):
        # Seeding has to *write* what it found, not just use it — otherwise the
        # second allocation aggregates again and the gate never closes.
        stream = Stream.objects.create(stream_id="s")
        _entry_at(stream, 7)
        stream.get_next_offset_block(1)

        assert StreamOffsetWatermark.objects.get(stream_path="s").high == 8
        with CaptureQueriesContext(connection) as ctx:
            assert stream.get_next_offset_block(1) == 9
        assert _aggregate_queries(ctx) == []

    def test_offsets_survive_delete_and_recreate(self):
        # #34 Defect #2, restated at this seam because the gate is new code on
        # the path that guarantees it. The watermark outlives the Stream, so a
        # path recreated after deletion must not rewind into offsets a stale
        # subscriber cursor still points at.
        stream = Stream.objects.create(stream_id="s")
        stream.get_next_offset_block(3)
        Stream.objects.filter(stream_id="s").delete()

        recreated = Stream.objects.create(stream_id="s")
        assert recreated.get_next_offset_block(1) == 4
