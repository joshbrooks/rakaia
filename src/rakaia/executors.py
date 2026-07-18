"""
Reusable Executor implementations.

The `Executor` protocol (see effects.py) is applied by `replay()` to every batch
of effects it produces. `CollectingExecutor` records those effects instead of
applying them — the building block for a dry run and for migration
verification: run `replay()` with one and inspect `.effects` to see exactly what
a real executor would write, with zero side effects.
"""

from __future__ import annotations

from collections.abc import Iterable

from .effects import Effect


class CollectingExecutor:
    """An Executor that records effects instead of applying them.

    Example — verify that replaying a stream reproduces existing state without
    touching the database::

        ex = CollectingExecutor()
        replay(store, "submissions", ex)
        for effect in ex.effects:
            ...  # diff against the current rows
    """

    def __init__(self) -> None:
        self.effects: list[Effect] = []

    def apply(self, effects: Iterable[Effect]) -> None:
        self.effects.extend(effects)
