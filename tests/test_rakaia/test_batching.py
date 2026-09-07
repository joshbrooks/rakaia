"""The four flush rules, pinned at the buffer's own interface (#264).

`test_replay_batching.py` reaches these rules through a whole `replay()` — a
store, a seeded stream, a handler registry — which is the right shape for
proving the orchestrator wires them up, and the wrong shape for saying what the
rules *are*. These tests hand effects straight to the buffer, so each one names
a single rule and the batch boundary it forces.

Each test names the mutation it is pinned by; every one was applied to
`rakaia.batching` and watched go red.
"""

from __future__ import annotations

from typing import Any

import pytest

from rakaia.batching import _StageBuffer
from rakaia.effects import (
    ApplyReport,
    Delete,
    Effect,
    EffectCollisionError,
    Retire,
    Transition,
    Upsert,
    check_disjoint_defaults,
)


class BatchRecorder:
    """An `Executor` that records the *shape* of each call it was handed.

    It runs the same disjointness check a real executor runs before its first
    write, so a buffer that handed it a colliding pair would raise here rather
    than quietly record one batch too few.
    """

    def __init__(self) -> None:
        self.batches: list[list[Effect]] = []

    def apply(self, effects: Any) -> ApplyReport:
        batch = list(effects)
        check_disjoint_defaults(batch)
        self.batches.append(batch)
        return ApplyReport()


def _buffer() -> tuple[_StageBuffer, BatchRecorder, list[ApplyReport | None]]:
    ex = BatchRecorder()
    reports: list[ApplyReport | None] = []
    return _StageBuffer(ex, reports.append), ex, reports


def _sizes(ex: BatchRecorder) -> list[int]:
    return [len(b) for b in ex.batches]


def test_effects_that_cannot_tell_the_difference_ride_in_one_batch() -> None:
    """The baseline the four rules are exceptions to: disjoint rows, disjoint
    columns, one rank, no transition — one `apply()` carries them all.

    Mutation: make `add` flush unconditionally and this reads [1, 1, 1].
    """
    buf, ex, _ = _buffer()

    for n in range(3):
        buf.add([Upsert("app.R", {"id": f"r{n}"}, {"n": n})])
    buf.flush()

    assert _sizes(ex) == [3]


def test_a_write_behind_a_delete_starts_a_new_batch() -> None:
    """Rule one — reorder. A batch is applied writes-then-deletes, so an
    incoming write joining a pending delete would be hoisted above it.

    Mutation: drop the `_write_order_rank` arm of `_conflicts` and this reads
    [2], with the write applied before the delete that preceded it.
    """
    buf, ex, _ = _buffer()

    buf.add([Delete("app.R", {"id": "x"})])
    buf.add([Upsert("app.R", {"id": "y"}, {"n": 1})])
    buf.flush()

    assert _sizes(ex) == [1, 1]
    assert isinstance(ex.batches[0][0], Delete)


def test_a_second_write_to_the_same_column_starts_a_new_batch() -> None:
    """Rule two — collision. Normal between events (the second supersedes the
    first) and an error inside one batch, so the boundary has to fall between.

    Mutation: drop the `_written.collides` arm of `_conflicts` and this raises
    `EffectCollisionError` out of the recorder's disjointness check.
    """
    buf, ex, _ = _buffer()

    buf.add([Upsert("app.R", {"id": "x"}, {"n": 1})])
    buf.add([Upsert("app.R", {"id": "x"}, {"n": 2})])
    buf.flush()

    assert _sizes(ex) == [1, 1]


def test_a_repeated_produces_id_starts_a_new_batch() -> None:
    """Rule three — duplicate ``produces=``. Two producers of one correlation id
    in a batch is an error; in separate events it is ordinary.

    Mutation: drop the `_produces` arm of `_conflicts` and this reads [2].
    """
    buf, ex, _ = _buffer()

    buf.add([Upsert("app.A", {"id": "a"}, {"n": 1}, produces="p")])
    buf.add([Upsert("app.B", {"id": "b"}, {"n": 1}, produces="p")])
    buf.flush()

    assert _sizes(ex) == [1, 1]


def test_a_retire_asking_for_notifications_ends_its_batch() -> None:
    """Rule four — flush *after* a transition retire. Its transitions are
    synthesised from the report of the call that applied it, so the effects
    emitted after it must not ride along and reorder the external list.

    The effect that follows is a second retire, so no other rule would separate
    them: same rank, no column written, no correlation id.

    Mutation: drop the `Transition` arm at the end of `add` and this reads [2],
    with the second retire inside the first one's batch.
    """
    buf, ex, _ = _buffer()

    buf.add(
        [
            Retire(
                "app.R",
                {"id": "x"},
                {"resolved_at": 1},
                transition=Transition(kind="resolved", key_fields=("id",)),
            )
        ]
    )
    buf.add([Retire("app.R", {"id": "y"}, {"resolved_at": 1})])
    buf.flush()

    assert _sizes(ex) == [1, 1]


def test_an_event_that_collides_with_itself_is_applied_alone() -> None:
    """A self-colliding group is a bug the executor reports for that event, and
    it must keep reporting it for that event alone — so what was buffered ahead
    of it is applied first and survives the raise.

    Mutation: drop `_self_collides` from `add` and the earlier effect is still
    pending when the group raises, so nothing has been applied at all.
    """
    buf, ex, _ = _buffer()

    buf.add([Upsert("app.R", {"id": "a"}, {"n": 1})])
    with pytest.raises(EffectCollisionError):
        buf.add(
            [
                Upsert("app.R", {"id": "x"}, {"n": 1}),
                Upsert("app.R", {"id": "x"}, {"n": 2}),
            ]
        )

    assert _sizes(ex) == [1]


def test_every_applied_batch_hands_its_report_back() -> None:
    """The buffer owns the `apply()` call, so its caller only sees a report
    through the sink it passed in — that is how a transition retire's externals
    reach the replay result.

    Mutation: stop calling the sink in `flush` and this reads [].
    """
    buf, _, reports = _buffer()

    buf.add([Upsert("app.R", {"id": "x"}, {"n": 1})])
    buf.flush()

    assert len(reports) == 1
    assert isinstance(reports[0], ApplyReport)


def test_flushing_nothing_calls_no_executor() -> None:
    """An empty buffer flushed is a no-op, which is what lets the drain at the
    end of a pass be unconditional."""
    buf, ex, reports = _buffer()

    buf.flush()
    buf.add([])
    buf.flush()

    assert ex.batches == []
    assert reports == []
