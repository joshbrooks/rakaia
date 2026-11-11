"""Applying a batch of effects, in order, once for every executor (#325).

Both executors — `rakaia.executors.InMemoryProjections` and
`django_rakaia.effect_executor.DjangoExecutor` — used to carry their own copy of
the same loop: check the defaults are disjoint, keep one `RefResolver`, make one
pass per write-order rank, dispatch on the four effect types, record a
``produces=`` row, count what an upsert did, and collect the retires that asked
to notify. Most changes to one had to be made to the other as well. The loop is
here now, and an executor supplies only its four kinds of write.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from typing import Any, Literal

from .effects import (
    _WRITE_ORDER_PASSES,
    ApplyReport,
    Delete,
    Effect,
    RefResolver,
    Retire,
    Update,
    Upsert,
    _write_order_rank,
    check_disjoint_defaults,
)

UpsertOutcome = Literal["created", "written", "skipped"]
"""How an upsert resolved; see `ApplyReport` for what the three mean."""

RowAccessor = Callable[[str], Any]
"""``field -> value`` over the row an upsert landed on, with ``"pk"`` and ``"id"``
meaning the primary key. It is what a sibling effect's `Ref` reads."""


def apply_effects(
    effects: Iterable[Effect],
    *,
    upsert: Callable[[Upsert], tuple[UpsertOutcome, RowAccessor]],
    update_run: Callable[[list[Update]], None],
    delete: Callable[[Delete], None],
    retire: Callable[[Retire], list[dict[str, Any]]],
) -> ApplyReport:
    """Apply `effects` through the four writes an executor supplies.

    Writes go first, then deletes, then retires, so a batch converges whatever
    order its handlers emitted. A ``produces=`` row is recorded as soon as its
    upsert lands, and every effect reaches your function with its `Ref` values
    already filled in. `upsert` returns how the row resolved (see `UpsertOutcome`)
    and a `RowAccessor` for the row it landed on; `retire` returns the identity
    dicts of the rows it flipped.

    Consecutive `Update` effects are handed to `update_run` together, so you can
    send them as one statement; the run ends at the first effect of any other
    kind, so writes still land in the order they were emitted. Raises
    `EffectCollisionError` if the batch's defaults overlap, and
    `UnresolvedRefError` if a `Ref` has nothing to read.
    """
    effects_list = list(effects)
    check_disjoint_defaults(effects_list)
    resolver = RefResolver()
    retire_flips: list[tuple[Retire, list[dict[str, Any]]]] = []
    created = written = skipped = 0
    for rank in _WRITE_ORDER_PASSES:
        run: list[Update] = []
        for eff in effects_list:
            if _write_order_rank(eff) != rank:
                continue
            if isinstance(eff, Update):
                run.append(resolver.resolve_effect(eff))
                continue
            if run:
                update_run(run)
                run = []
            if isinstance(eff, Upsert):
                outcome, row = upsert(resolver.resolve_effect(eff))
                if outcome == "skipped":
                    skipped += 1
                else:
                    written += 1
                    if outcome == "created":
                        created += 1
                if eff.produces is not None:
                    resolver.record(eff.produces, row)
            elif isinstance(eff, Delete):
                delete(resolver.resolve_effect(eff))
            elif isinstance(eff, Retire):
                resolved = resolver.resolve_effect(eff)
                flipped = retire(resolved)
                if resolved.transition is not None:
                    retire_flips.append((resolved, flipped))
        if run:
            update_run(run)
    return ApplyReport(
        retire_flips=retire_flips,
        upserts_created=created,
        upserts_written=written,
        upserts_skipped=skipped,
    )
