"""Tests for the durable, DB-backed DjangoStreamStore."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from django.db.models.query import QuerySet

from django_rakaia.django_store import DjangoStreamStore, format_offset
from django_rakaia.models import (
    Stream,
    StreamEntry,
    StreamEvent,
    StreamOffsetWatermark,
)
from rakaia import CollectingExecutor
from rakaia.effects import Effect
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay
from rakaia.seed import seed_stream
from rakaia.types import AppendOptions, SequenceConflict, StreamConfigConflict


@pytest.mark.django_db
class TestDjangoStreamStore:
    # The shared read/append/create/has surface (create-idempotence, append-
    # requires-stream, ordered round-trip, partial read, envelope round-trip,
    # current-offset) is covered once for every backend by
    # tests/store_contract.py::StoreContract (see test_store_contract.py). Only
    # DjangoStreamStore-specific behaviour lives here: ORM persistence, integer
    # offset format, row locking, durability across instances, delete/list_paths.
    def test_create_persists_stream_row(self):
        DjangoStreamStore().create("submissions")
        assert Stream.objects.filter(stream_id="submissions").exists()

    def test_create_is_idempotent(self):
        store = DjangoStreamStore()
        store.create("s")
        store.create("s")
        assert Stream.objects.filter(stream_id="s").count() == 1

    def test_append_monotonic_offset(self):
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}')
        store.append("s", b'{"id": 2}')
        offsets = list(
            StreamEntry.objects.filter(stream__stream_id="s")
            .order_by("offset")
            .values_list("offset", flat=True)
        )
        assert offsets == [1, 2]

    def test_append_locks_for_offset_allocation(self):
        # Regression guard for the concurrency bug: offset allocation must go
        # through select_for_update() so concurrent appends serialize (now on the
        # per-path StreamOffsetWatermark row, #34) instead of racing to the same
        # offset and failing the unique_together(stream, offset) constraint.
        # SQLite (the CI backend) can't reproduce the race across connections, so
        # assert the lock is acquired rather than trying to trigger the collision.
        store = DjangoStreamStore()
        store.create("s")

        original = QuerySet.select_for_update
        locked = []

        def spy(qs, *args, **kwargs):
            locked.append(True)
            return original(qs, *args, **kwargs)

        with patch.object(QuerySet, "select_for_update", spy):
            store.append("s", b'{"id": 1}')

        assert locked, "append must lock the watermark row via select_for_update()"

    def test_delete_removes_stream_and_entries(self):
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}')

        assert store.delete("s") is True
        assert not Stream.objects.filter(stream_id="s").exists()
        assert StreamEntry.objects.count() == 0

    def test_delete_returns_false_when_absent(self):
        assert DjangoStreamStore().delete("nope") is False

    def test_has_and_list_paths(self):
        store = DjangoStreamStore()
        assert store.has("s") is False
        store.create("s")
        store.create("t")
        assert store.has("s") is True
        assert set(store.list_paths()) == {"s", "t"}

    def test_create_conflicting_config_raises(self):
        # This used to assert the opposite: the durable Stream model had no
        # content_type/ttl/closed columns, so there was no stored config to
        # conflict on and a re-create with different kwargs silently ignored
        # them. Now that the store backs a protocol server it records its
        # configuration, and matches the in-memory store.
        store = DjangoStreamStore()
        store.create("s", content_type="application/json")
        with pytest.raises(StreamConfigConflict):
            store.create("s", content_type="text/plain")
        assert Stream.objects.filter(stream_id="s").count() == 1

    def test_create_is_idempotent_for_matching_config(self):
        store = DjangoStreamStore()
        first = store.create("s", content_type="application/json", ttl_seconds=60)
        again = store.create("s", content_type="application/json", ttl_seconds=60)
        assert first.pk == again.pk
        assert Stream.objects.filter(stream_id="s").count() == 1

    def test_append_to_closed_stream_reports_stream_closed(self):
        # Also previously asserted the opposite — there was no `closed` column,
        # so appends to a "closed" stream just kept succeeding.
        store = DjangoStreamStore()
        store.create("s")
        store.close_stream("s")
        result = store.append("s", b'{"id": 1}')
        assert result.stream_closed is True
        assert result.message is None

    def test_seq_conflict_raises(self):
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}', AppendOptions(seq="5"))
        with pytest.raises(SequenceConflict):
            store.append("s", b'{"id": 2}', AppendOptions(seq="5"))

    def test_seq_compares_lexicographically(self):
        """Seq values are opaque strings compared byte-wise, so "10" after "9"
        is a conflict. Padded, "10" follows "09"."""
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}', AppendOptions(seq="9"))
        with pytest.raises(SequenceConflict):
            store.append("s", b'{"id": 2}', AppendOptions(seq="10"))

        store.create("padded")
        store.append("padded", b'{"id": 1}', AppendOptions(seq="09"))
        result = store.append("padded", b'{"id": 2}', AppendOptions(seq="10"))
        assert result.message is not None

    def test_get_current_offset(self):
        store = DjangoStreamStore()
        assert store.get_current_offset("s") is None
        store.create("s")
        store.append("s", b'{"id": 1}')
        store.append("s", b'{"id": 2}')
        # Offsets are zero-padded for lexicographic sortability (#34).
        assert store.get_current_offset("s") == "00000000000000000002"

    def test_offsets_stay_monotonic_across_delete_recreate(self):
        # #34 Defect #2: a stream recreated at the same path must resume
        # numbering ABOVE the retired high mark, so an offset is never reused.
        # Otherwise a subscriber holding a cursor from the old incarnation reads
        # the recreated head as `caught_up` and silently skips the new content.
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}')
        store.append("s", b'{"id": 2}')
        before = store.get_current_offset("s")

        store.delete("s")
        store.create("s")
        store.append("s", b'{"id": 3}')
        after = store.get_current_offset("s")

        # Strictly greater under plain string (lexicographic) comparison — the
        # recreated stream's head sorts past any prior cursor.
        assert after > before

    def test_current_offset_reflects_watermark_after_empty_recreate(self):
        # #34 Defect #2, read side: get_current_offset must not regress below the
        # retired high-water in the window after a recreate but BEFORE the first
        # append. Allocation resumes above the watermark, so the reported head
        # must too — otherwise a stale cursor sorts past a head of 0 and the poll
        # spuriously reports `rewound` for a stream that only looks empty.
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}')
        store.append("s", b'{"id": 2}')
        retired = store.get_current_offset("s")

        store.delete("s")
        store.create("s")  # recreated, no append yet

        # Head stays at the retired high-water, not 0.
        assert store.get_current_offset("s") == retired

    def test_durability_across_instances(self):
        # The property the in-memory store lacks: a fresh instance sees data
        # written by an earlier one, because state lives in the database.
        DjangoStreamStore().create("s")
        DjangoStreamStore().append("s", b'{"id": 42}')
        messages, _ = DjangoStreamStore().read("s")
        assert [json.loads(m.data) for m in messages] == [{"id": 42}]

    def test_append_many_byte_identical_to_append_loop(self):
        # Acceptance criterion: append_many([...]) produces byte-identical
        # persisted state (event data/type/metadata/event_ts + contiguous
        # offsets) to a loop of append(...). Build one stream each way and
        # compare the ORM rows directly.
        batch = [
            (b'{"id": 1}', None),
            (b'{"id": 2}', AppendOptions(label="update", metadata={"user": 7})),
            (b'{"id": 3}', AppendOptions(event_ts=1_600_000_000.5)),
        ]

        store = DjangoStreamStore()
        store.create("loop")
        for data, options in batch:
            store.append("loop", data, options)
        store.create("bulk")
        returned = store.append_many("bulk", batch)

        def rows(path):
            return [
                (e.event.data, e.event.event_type, e.event.metadata, e.event.event_ts)
                for e in StreamEntry.objects.filter(stream__stream_id=path)
                .select_related("event")
                .order_by("offset")
            ]

        assert rows("bulk") == rows("loop")
        # Results returned in input order, offsets contiguous from 1.
        assert [r.message.offset for r in returned] == [
            format_offset(1),
            format_offset(2),
            format_offset(3),
        ]
        assert [
            json.loads(store.read("bulk")[0][i].data.decode()) for i in range(3)
        ] == [
            {"id": 1},
            {"id": 2},
            {"id": 3},
        ]

    def test_append_many_bounded_queries(self):
        # Acceptance criterion: a large batch runs in an N-independent, bounded
        # number of queries — O(1)-ish transactions, not the ~O(N) a loop of
        # append() does. bulk_create may split into a few batches under SQLite's
        # bind-param cap, so assert a small constant ceiling (well under the
        # ~5*N a loop of append() would issue) rather than an exact count.
        from django.db import connection
        from django.test.utils import CaptureQueriesContext

        store = DjangoStreamStore()
        store.create("s")
        batch = [(json.dumps({"id": i}).encode(), None) for i in range(1000)]
        with CaptureQueriesContext(connection) as ctx:
            store.append_many("s", batch)
        assert len(ctx.captured_queries) < 30
        assert StreamEntry.objects.filter(stream__stream_id="s").count() == 1000

    def test_append_many_locks_watermark_once(self):
        # The single-contiguous-allocation guarantee: the whole batch takes the
        # watermark select_for_update() lock exactly once (vs once per item for
        # a loop of append). Mirrors test_append_locks_for_offset_allocation.
        store = DjangoStreamStore()
        store.create("s")

        original = QuerySet.select_for_update
        locked = []

        def spy(qs, *args, **kwargs):
            locked.append(qs.model.__name__)
            return original(qs, *args, **kwargs)

        with patch.object(QuerySet, "select_for_update", spy):
            store.append_many("s", [(b'{"id": 1}', None), (b'{"id": 2}', None)])

        # The stream-row lock (writer serialization for the closed/seq/fencing
        # checks) is separate; what must not scale with the batch is the
        # watermark lock.
        assert locked.count("StreamOffsetWatermark") == 1, (
            "append_many must lock the watermark exactly once"
        )

    def test_append_many_interleaves_with_append_without_collision(self):
        # Interleaved append + append_many share the one offset-allocation path,
        # so offsets stay unique, contiguous and strictly increasing (same lock
        # semantics as today). SQLite can't reproduce a true cross-connection
        # race, so assert the allocation is contiguous rather than triggering it.
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}')
        store.append_many("s", [(b'{"id": 2}', None), (b'{"id": 3}', None)])
        store.append("s", b'{"id": 4}')

        offsets = list(
            StreamEntry.objects.filter(stream__stream_id="s")
            .order_by("offset")
            .values_list("offset", flat=True)
        )
        assert offsets == [1, 2, 3, 4]

    def test_append_many_empty_is_noop_without_stream(self):
        # Empty batch returns [] without a DB hit, so it never raises for a
        # missing stream (unlike a non-empty batch).
        store = DjangoStreamStore()
        assert store.append_many("missing", []) == []

    def test_append_many_missing_stream_raises(self):
        store = DjangoStreamStore()
        with pytest.raises(KeyError):
            store.append_many("missing", [(b'{"id": 1}', None)])

    def test_append_many_preserves_distinct_per_item_envelopes(self):
        # Sharpens the "identical to N appends" claim on the envelope: every
        # item carries its OWN label/metadata/event_ts (not a uniform batch),
        # and a raw-append item keeps the default label / empty metadata / null
        # ts — asserted row-by-row.
        store = DjangoStreamStore()
        store.create("s")
        batch = [
            (
                b'{"id": 1}',
                AppendOptions(label="create", metadata={"a": 1}, event_ts=1000.0),
            ),
            (
                b'{"id": 2}',
                AppendOptions(label="update", metadata={"b": 2}, event_ts=2000.0),
            ),
            (b'{"id": 3}', None),
        ]
        store.append_many("s", batch)
        rows = [
            (e.event.data, e.event.event_type, e.event.metadata, e.event.event_ts)
            for e in StreamEntry.objects.filter(stream__stream_id="s")
            .select_related("event")
            .order_by("offset")
        ]
        assert rows == [
            ({"id": 1}, "create", {"a": 1}, 1000.0),
            ({"id": 2}, "update", {"b": 2}, 2000.0),
            ({"id": 3}, "append", {}, None),
        ]

    def test_append_many_rolls_back_atomically_on_failure(self):
        # The whole batch is one transaction: if entry creation fails, the
        # already-bulk_created events AND the watermark advance roll back with
        # it — no partial write, no leaked offset block.
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 0}')  # seed one entry -> watermark high = 1
        high_before = StreamOffsetWatermark.objects.get(stream_path="s").high
        events_before = StreamEvent.objects.count()

        with (
            patch.object(
                StreamEntry.objects, "bulk_create", side_effect=RuntimeError("boom")
            ),
            pytest.raises(RuntimeError, match="boom"),
        ):
            store.append_many("s", [(b'{"id": 1}', None), (b'{"id": 2}', None)])

        assert StreamEvent.objects.count() == events_before
        assert StreamOffsetWatermark.objects.get(stream_path="s").high == high_before
        # The seeded entry survives; nothing from the failed batch persisted.
        assert list(
            StreamEntry.objects.filter(stream__stream_id="s")
            .order_by("offset")
            .values_list("offset", flat=True)
        ) == [1]

    def test_get_next_offset_block_rejects_non_positive_count(self):
        # The shared allocation path guards its precondition: a caller must
        # reserve at least one offset. append_many short-circuits an empty batch
        # before allocating, so this guard only trips on direct misuse.
        stream = Stream.objects.create(stream_id="s")
        with pytest.raises(ValueError, match="count must be >= 1"):
            stream.get_next_offset_block(0)


@pytest.mark.django_db
class TestDjangoStreamStoreReplay:
    def _register(self) -> HandlerRegistry:
        reg = HandlerRegistry()

        def h(event):
            return Effect(
                op="update_or_create",
                model_label="x.X",
                lookup={"id": event["id"]},
                defaults={"name": event["name"]},
            )

        reg.register("h", "s", h, 0, None)
        return reg

    def test_replay_over_django_store_matches_memory(self):
        events = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        reg = self._register()

        mem = seed_stream("s", events)
        mem_ex = CollectingExecutor()
        replay(mem, "s", mem_ex, handler_registry=reg)

        dj = seed_stream("s", events, store=DjangoStreamStore())
        dj_ex = CollectingExecutor()
        replay(dj, "s", dj_ex, handler_registry=reg)

        assert [(e.lookup, e.defaults) for e in dj_ex.effects] == [
            (e.lookup, e.defaults) for e in mem_ex.effects
        ]


@pytest.mark.django_db
def test_get_store_returns_durable_when_setting_set(settings):
    from django_rakaia.store import get_store

    settings.RAKAIA_STORE = "durable"
    assert isinstance(get_store(), DjangoStreamStore)


@pytest.mark.django_db
class TestExpiryReaping:
    def test_a_refused_append_still_reaps_the_expired_row(self):
        """The reap must run outside the write transaction.

        `_require`'s expiry delete inside `transaction.atomic()` is rolled
        back by the very `StreamNotFound` unwind that reports it — the write
        path raised 404s forever without ever actually deleting the row.
        """
        import time

        from rakaia.types import StreamNotFound

        store = DjangoStreamStore()
        store.create("s", ttl_seconds=0)
        time.sleep(0.01)
        with pytest.raises(StreamNotFound):
            store.append("s", b'{"id": 1}')
        assert not Stream.objects.filter(stream_id="s").exists()


@pytest.mark.django_db(transaction=True)
@pytest.mark.asyncio
async def test_expiry_mid_long_poll_is_a_timeout_not_an_error():
    """A stream that ages out under a waiting long-poll times out normally.

    The in-memory store reports mid-wait expiry as an ordinary timeout; the
    durable store raised `StreamNotFound`, which the handler turns into a 404
    on a connection that was legitimately waiting.
    """
    import asyncio

    from asgiref.sync import sync_to_async

    store = DjangoStreamStore()
    await sync_to_async(store.create)("s", ttl_seconds=3600)
    head = await sync_to_async(store.get_current_offset)("s")

    task = asyncio.ensure_future(store.wait_for_messages("s", head, 5.0))
    await asyncio.sleep(0.3)  # let the poll loop observe the live stream first
    await sync_to_async(Stream.objects.filter(stream_id="s").update)(
        last_activity_at=0.0
    )

    messages, timed_out, closed = await asyncio.wait_for(task, timeout=5.0)
    assert (messages, timed_out, closed) == ([], True, False)
