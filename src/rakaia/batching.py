"""Holding a replay pass's effects back so the executor sees few, large batches
— and flushing the moment holding one back would change the answer.

Replay used to call ``executor.apply()`` once per event, so a batch of one was
the norm: the contiguous-`Update` collapsing in the Django executor could never
engage (a run of one is never collapsed), and nine events meant nine
``transaction.atomic()`` blocks, measured at 18 of the 135 statements one form
save issues (#207). Widening the batch is only free if the executor cannot tell
the difference, so **the whole design is in the four rules that force a flush**:

* **Before a reorder.** A batch is applied writes-then-deletes-then-retires (see
  `_write_order_rank`), so an effect ranking below anything pending would be
  hoisted across an event boundary — flush before adding it.
* **Before a collision.** A second write to the same column of the same row is
  ordinary between events (event 2 supersedes event 1) and an error inside one
  batch, which `check_disjoint_defaults` raises with nothing applied. Asked
  through the same `_WrittenFields` that check uses, so there is one rule.
* **Before a duplicate ``produces=``.** Two producers of one correlation id in a
  batch is `DuplicateProducesError`; in separate events it is normal.
* **After a transition retire.** Its transitions are synthesised from the report
  of the call that applied it, and the replay result's external list is
  documented in handler-emission order, so nothing may ride along behind it.
  Opted-in retires are rare, so this costs almost nothing.

Two things the buffer deliberately does *not* preserve, both widenings rather
than changes: a `Ref` may now bind to a ``produces=`` row from an earlier event
in the same pass (it used to raise, because refs do not cross an ``apply()``
call), and the executor sees fewer, larger batches.
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Protocol

from .effects import (
    ApplyReport,
    Effect,
    Executor,
    Retire,
    Upsert,
    _write_order_rank,
    _WrittenFields,
)


def _self_collides(effects: list[Effect]) -> bool:
    """Whether one event's own effects already write the same column twice.

    That is a bug the executor reports, and it must keep reporting it *for that
    event alone* — so a self-colliding group is applied on its own, leaving
    earlier events' effects committed exactly as they were before buffering.
    """
    seen = _WrittenFields()
    for idx, eff in enumerate(effects):
        if seen.collides(eff) is not None:
            return True
        seen.record(eff, idx)
    return False


class _StageBuffer:
    """Effects held back so a pass reaches the executor as few batches as
    possible, flushed early by the four rules in this module's docstring.

    Constructed with the executor to apply through and a sink for each call's
    `ApplyReport`. The buffer owns the ``apply()`` call, so the sink is the only
    way its caller learns what a batch observed — which is how a transition
    retire's flips reach the replay result as external effects.
    """

    def __init__(
        self, executor: Executor, on_report: Callable[[ApplyReport | None], None]
    ) -> None:
        self._executor = executor
        self._on_report = on_report
        self._pending: list[Effect] = []
        self._written = _WrittenFields()
        self._produces: set[str] = set()
        self._max_rank = -1

    def add(self, effects: list[Effect]) -> None:
        """Buffer one event's effects, flushing first if they cannot join."""
        if not effects:
            return
        alone = _self_collides(effects)
        if self._pending and (alone or self._conflicts(effects)):
            self.flush()
        for eff in effects:
            self._written.record(eff, len(self._pending))
            self._pending.append(eff)
            if isinstance(eff, Upsert) and eff.produces is not None:
                self._produces.add(eff.produces)
            self._max_rank = max(self._max_rank, _write_order_rank(eff))
        if alone or any(
            isinstance(e, Retire) and e.transition is not None for e in effects
        ):
            self.flush()

    def _conflicts(self, effects: list[Effect]) -> bool:
        """Whether adding `effects` to what is pending would change behaviour.

        Checked for the group as a whole, against what is pending only — an
        event's effects collide with *each other* under the same rules the
        executor already applies to a batch, and that stays the executor's to
        report.
        """
        if min(_write_order_rank(e) for e in effects) < self._max_rank:
            return True
        if any(self._written.collides(e) is not None for e in effects):
            return True
        return any(
            isinstance(e, Upsert)
            and e.produces is not None
            and e.produces in self._produces
            for e in effects
        )

    def flush(self) -> None:
        """Apply what is buffered as one batch, and reset."""
        if not self._pending:
            return
        batch = self._pending
        self._pending = []
        self._written = _WrittenFields()
        self._produces = set()
        self._max_rank = -1
        self._on_report(self._executor.apply(batch))


class _Buffered(Protocol):
    """Anything carrying the buffer for the pass currently running.

    Named as a protocol so `_drain` can live beside the buffer it empties
    without this module having to import the replay context — the batching rules
    do not depend on anything else the orchestrator carries.
    """

    buffer: _StageBuffer | None


def _drain(ctx: _Buffered) -> None:
    """Apply whatever the pass buffered, and go back to per-event application."""
    if ctx.buffer is not None:
        buffer, ctx.buffer = ctx.buffer, None
        buffer.flush()
