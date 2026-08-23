"""A stage reaches the executor as few batches as possible — and no fewer (#207).

Replay used to call ``executor.apply()`` once per event, so the batch was almost
always a single effect: the contiguous-``Update`` collapsing in the Django
executor could never engage, and nine events meant nine ``transaction.atomic()``
blocks. Buffering a stage fixes both, but only if the executor cannot tell the
difference — so most of what is worth testing here is the *flushes*, not the
batching.

Each test below trips one of the four things that force an early flush and
asserts the batch boundary is where it must be. The mutations they are pinned by
are named on each test; every one was applied and watched go red.
"""

from __future__ import annotations

from typing import Any

import pytest

from rakaia.effects import (
    ApplyReport,
    Delete,
    Effect,
    EffectCollisionError,
    ExternalEffect,
    Ref,
    Retire,
    Transition,
    Update,
    Upsert,
    check_disjoint_defaults,
)
from rakaia.executors import InMemoryProjections
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay
from rakaia.seed import seed_stream
from rakaia.store import StreamStore

MATCH = "s"


class BatchRecorder:
    """An `Executor` that records the *shape* of what it was handed.

    `CollectingExecutor` flattens, which is precisely the property under test
    here — the flat list of effects is identical either way, and the whole
    question is how many `apply()` calls carried it.
    """

    def __init__(self) -> None:
        self.batches: list[list[Effect]] = []

    def apply(self, effects) -> ApplyReport:
        batch = list(effects)
        # A real executor runs this before its first write, and the whole point
        # of the buffer's collision arm is to keep it from seeing a pair it
        # never saw before. A double that skipped it could not show that.
        check_disjoint_defaults(batch)
        self.batches.append(batch)
        return ApplyReport()

    @property
    def sizes(self) -> list[int]:
        return [len(b) for b in self.batches]

    @property
    def flat(self) -> list[Effect]:
        return [e for b in self.batches for e in b]


class FlippingExecutor(BatchRecorder):
    """A `BatchRecorder` that also reports every opted-in retire as having
    flipped one row, so `replay` has a transition to synthesise."""

    def apply(self, effects) -> ApplyReport:
        batch = list(effects)
        super().apply(batch)
        return ApplyReport(
            retire_flips=[
                (e, [dict(e.lookup)])
                for e in batch
                if isinstance(e, Retire) and e.transition is not None
            ]
        )


@pytest.fixture
def store() -> StreamStore:
    return StreamStore()


def _registry(fn, *, stage: int = 0, name: str = "h") -> HandlerRegistry:
    registry = HandlerRegistry()
    registry.register(
        name=name, event_match=MATCH, fn=fn, effective_from=0, stage=stage
    )
    return registry


def _run(store, registry, executor, *, reader=None):
    return replay(
        store,
        "s",
        executor,
        handler_registry=registry,
        event_match=MATCH,
        reader=reader,
    )


def _events(n: int) -> list[dict[str, Any]]:
    return [{"id": f"e{i}", "n": i} for i in range(n)]


class TestTheCommonCaseBecomesOneBatch:
    def test_nine_events_writing_nine_rows_are_one_apply_call(self, store):
        """The shape the issue measured: nine events, one effect each, nine
        `apply()` calls. It is one now.

        Mutation: make `run_passes` leave `ctx.buffer` at None and this reports
        nine batches of one.
        """
        seed_stream("s", _events(9), store=store)
        ex = BatchRecorder()

        _run(
            store,
            _registry(lambda ev: Upsert("app.R", {"id": ev["id"]}, {"n": ev["n"]})),
            ex,
        )

        assert ex.sizes == [9]

    def test_the_effects_are_the_same_ones_in_the_same_order(self, store):
        """Batching may not reorder or drop anything — only regroup."""
        seed_stream("s", _events(5), store=store)
        ex = BatchRecorder()

        result = _run(
            store,
            _registry(lambda ev: Upsert("app.R", {"id": ev["id"]}, {"n": ev["n"]})),
            ex,
        )

        assert [e.lookup["id"] for e in ex.flat] == ["e0", "e1", "e2", "e3", "e4"]
        assert result.effects_applied == 5


class TestWhatForcesAFlush:
    def test_a_second_write_to_the_same_column_starts_a_new_batch(self, store):
        """Two events superseding each other on one column is ordinary; the same
        two in one batch is `EffectCollisionError` with nothing applied.

        Mutation: drop the `_written.collides` arm of `_StageBuffer._conflicts`
        and this raises instead of reporting two batches.
        """
        seed_stream("s", _events(2), store=store)
        ex = BatchRecorder()

        _run(
            store,
            _registry(lambda ev: Upsert("app.R", {"id": "same"}, {"n": ev["n"]})),
            ex,
        )

        assert ex.sizes == [1, 1]

    def test_a_write_after_a_delete_starts_a_new_batch(self, store):
        """A batch is applied writes-then-deletes-then-retires, so pouring both
        events into one would hoist the second event's write above the first
        event's delete and the row would end up gone instead of present.

        Mutation: drop the `_write_order_rank` arm of `_conflicts` and the
        equivalence test below fails on the row's existence.
        """
        seed_stream("s", _events(2), store=store)
        ex = BatchRecorder()

        def fn(ev):
            if ev["n"] == 0:
                return Delete("app.R", {"id": "x"})
            return Upsert("app.R", {"id": "x"}, {"n": 1})

        _run(store, _registry(fn), ex)

        assert ex.sizes == [1, 1]

    def test_a_repeated_produces_id_starts_a_new_batch(self, store):
        """Two producers of one correlation id in a batch is
        `DuplicateProducesError`; in two events it is normal.

        Mutation: drop the `_produces` arm of `_conflicts` and this raises.
        """
        seed_stream("s", _events(2), store=store)
        ex = BatchRecorder()

        _run(
            store,
            _registry(
                lambda ev: Upsert(
                    "app.R", {"id": ev["id"]}, {"n": ev["n"]}, produces="p"
                )
            ),
            ex,
        )

        assert ex.sizes == [1, 1]

    def test_a_retire_asking_for_notifications_ends_its_batch(self, store):
        """Its transitions are synthesised from the report of the call that
        applied it, and `ReplayResult.external` is documented in handler-emission
        order — so the retire's batch must end at the retire.

        Mutation: drop the `Transition` arm of `_StageBuffer.add` and the retire
        joins the following event's batch.
        """
        seed_stream("s", _events(3), store=store)
        ex = BatchRecorder()

        def fn(ev):
            if ev["n"] == 1:
                return Retire(
                    "app.R",
                    {"id": "x"},
                    patch={"gone_at": "t"},
                    transition=Transition(kind="resolved", key_fields=("id",)),
                )
            return Upsert("app.R", {"id": ev["id"]}, {"n": ev["n"]})

        _run(store, _registry(fn), ex)

        # e0's upsert may share the retire's batch — a retire sorts *after*
        # a write, so nothing is reordered. What must not happen is e2's upsert
        # joining: the retire has to be the last effect in its batch, or its
        # transitions would be synthesised after a later event's externals.
        assert ex.sizes == [2, 1]
        assert isinstance(ex.batches[0][-1], Retire)
        assert isinstance(ex.batches[1][0], Upsert)

    def test_a_transitions_notification_still_precedes_a_later_events_own(self, store):
        """The ordering `ReplayResult.external` promises, and the only case the
        transition flush is load-bearing for.

        A retire's notifications are synthesised when its batch is applied, but
        a handler's own external effect is recorded the moment the handler
        returns. So if the retire were still sitting in the buffer while the
        next event ran, that event's notification would be filed ahead of the
        retire's — the list would come back in an order no handler produced.
        Flushing on the retire is what keeps it honest.

        Mutation: drop the `Transition` arm of `_StageBuffer.add` and the two
        come back the other way round. The rank rule does not cover this — the
        later event emits no database effect at all, so nothing forces a flush.
        """
        seed_stream("s", _events(2), store=store)

        def fn(ev):
            if ev["n"] == 0:
                return Retire(
                    "app.R",
                    {"id": "x"},
                    patch={"gone_at": "t"},
                    transition=Transition(kind="resolved", key_fields=("id",)),
                )
            return ExternalEffect(kind="email", payload={"to": "later"})

        result = _run(store, _registry(fn), FlippingExecutor())

        assert [e.kind for e in result.external] == ["resolved", "email"]

    def test_an_events_own_colliding_effects_still_raise_on_their_own(self, store):
        """A bug inside one event stays that event's bug: it is reported, and the
        events before it stay applied, exactly as before buffering.

        Mutation: drop `_self_collides` from `_StageBuffer.add` and the earlier
        event's effect is rejected along with the bad one.
        """
        seed_stream("s", _events(2), store=store)
        ex = BatchRecorder()

        def fn(ev):
            if ev["n"] == 0:
                return Upsert("app.R", {"id": "e0"}, {"n": 0})
            return [
                Upsert("app.R", {"id": "bad"}, {"n": 1}),
                Upsert("app.R", {"id": "bad"}, {"n": 2}),
            ]

        with pytest.raises(EffectCollisionError):
            _run(store, _registry(fn), ex)

        assert ex.sizes == [1], "the first event was applied before the bad one raised"
        assert ex.flat[0].lookup == {"id": "e0"}


class TestAReaderBearingPassIsNotBatched:
    def test_stage_one_still_applies_per_event(self, store):
        """A stage > 0 handler is handed a reader that goes straight to storage,
        so a buffered write would be invisible to it — silently. Those passes
        keep applying per event.

        Mutation: batch every stage instead of `stage in (None, 0)` and the
        stage-1 sizes become one batch of three.
        """
        seed_stream("s", _events(3), store=store)
        proj = InMemoryProjections()
        ex = BatchRecorder()

        registry = HandlerRegistry()
        registry.register(
            name="h0",
            event_match=MATCH,
            fn=lambda ev: Upsert("app.R", {"id": ev["id"]}, {"n": ev["n"]}),
            effective_from=0,
            stage=0,
        )
        registry.register(
            name="h1",
            event_match=MATCH,
            fn=lambda ev, _reader: Upsert("app.S", {"id": ev["id"]}, {"n": ev["n"]}),
            effective_from=0,
            stage=1,
        )

        _run(store, registry, ex, reader=proj)

        assert ex.sizes == [3, 1, 1, 1], (
            "stage 0 batches; stage 1 hands the reader out, so it does not"
        )

    def test_a_reducer_sees_the_stages_writes(self, store):
        """The stage's buffer must be drained before the first reducer runs, or
        the reducer reads a projection missing everything the pass just wrote.

        Mutation: move `_drain` after `_run_stage_reducers` and this reads 0.
        """
        seed_stream("s", _events(4), store=store)
        proj = InMemoryProjections()
        seen: list[int] = []

        registry = HandlerRegistry()
        registry.register(
            name="h0",
            event_match=MATCH,
            fn=lambda ev: Upsert("app.R", {"id": ev["id"]}, {"n": ev["n"]}),
            effective_from=0,
            stage=0,
        )

        def reducer(reader):
            seen.append(len(list(reader.query("app.R"))))
            return Upsert("app.Total", {"k": 1}, {"n": seen[-1]})

        registry.register_reducer("r", stage=0, fn=reducer)

        replay(
            store,
            "s",
            proj,
            handler_registry=registry,
            event_match=MATCH,
            reader=proj,
        )

        assert seen == [4]


class TestBatchingChangesNoRow:
    """The rows a batched replay leaves behind equal the rows the per-event path
    left behind — checked against the executor that is also the reader, for the
    four flush cases above plus a plain run."""

    @pytest.mark.parametrize(
        ("name", "fn"),
        [
            ("plain", lambda ev: Upsert("app.R", {"id": ev["id"]}, {"n": ev["n"]})),
            ("same_column", lambda ev: Upsert("app.R", {"id": "same"}, {"n": ev["n"]})),
            (
                "delete_then_write",
                lambda ev: (
                    Delete("app.R", {"id": "x"})
                    if ev["n"] % 2 == 0
                    else Upsert("app.R", {"id": "x"}, {"n": ev["n"]})
                ),
            ),
            (
                "update_after_upsert",
                lambda ev: [
                    Upsert("app.R", {"id": ev["id"]}, {"n": ev["n"]}),
                    Update("app.R", {"id": ev["id"]}, {"m": ev["n"] * 2}),
                ],
            ),
        ],
    )
    def test_the_rows_match_applying_one_event_at_a_time(self, store, name, fn):
        seed_stream("s", _events(6), store=store)

        batched = InMemoryProjections()
        _run(store, _registry(fn), batched)

        # The control: the same effects, applied one event's worth at a time,
        # which is what replay did before #207.
        one_at_a_time = InMemoryProjections()
        for event in _events(6):
            out = fn(event)
            one_at_a_time.apply(out if isinstance(out, list) else [out])

        assert batched.rows("app.R") == one_at_a_time.rows("app.R"), name


class TestRefsWidenRatherThanBreak:
    def test_a_ref_may_now_bind_to_an_earlier_events_producer(self, store):
        """A deliberate widening, recorded because it is a behaviour change: a
        `Ref` to a `produces=` row in an earlier event of the same stage used to
        raise, because refs do not cross an `apply()` call. Within one batch it
        resolves.
        """
        seed_stream("s", _events(2), store=store)
        proj = InMemoryProjections()

        def fn(ev):
            if ev["n"] == 0:
                return Upsert("app.Parent", {"id": "p"}, {"n": 0}, produces="parent")
            return Upsert("app.Child", {"id": "c"}, {"parent_id": Ref("parent")})

        _run(store, _registry(fn), proj)

        child = next(iter(proj.rows("app.Child")))
        assert not isinstance(child["parent_id"], Ref)
