"""The tables a handler says it reads and writes, and the replay-time check on them.

A handler registered with ``reads=`` / ``writes=`` names what it touches, so the pipeline
can be drawn and its order checked without scanning source. Each name is either a table,
``"app.Model"`` (its rows, and every column on them), or one column, ``"app.Model.field"``.
``None`` means "not declared" and turns the check off; an empty set means "touches nothing".

The declarations are not part of the handler's identity and never reach the meta-stream:
they describe the pipeline, not what the handler does, and the drift fingerprint already
covers the code. Replay compares them with the effects a handler *emits* and the reader
calls it makes, not with what an executor finally writes — an executor that discards some
writes does not make the declaration wrong.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from .effects import Delete, Retire, Update, Upsert


class UndeclaredTableWarning(UserWarning):
    """A handler wrote or read a table or column its registration did not declare."""


def canonical_tables(
    value: str | Iterable[str] | None, what: str
) -> frozenset[str] | None:
    """``None`` stays ``None``; a string is one name; anything else becomes a frozenset.

    Each name must be ``app.Model`` or ``app.Model.field``. A bare string is taken as one
    name rather than iterated, because iterating ``"app.Model"`` yields its characters.
    """
    if value is None:
        return None
    names = frozenset([value] if isinstance(value, str) else value)
    for name in names:
        if not isinstance(name, str):
            raise TypeError(f"{what} must hold strings, got {name!r}")
        parts = name.split(".")
        if len(parts) not in (2, 3) or not all(parts):
            raise ValueError(
                f"{what} entry {name!r} must be 'app.Model' or 'app.Model.field'"
            )
    return names


def _covers(declared: frozenset[str], label: str, column: str | None = None) -> bool:
    return label in declared or (column is not None and f"{label}.{column}" in declared)


def undeclared_writes(
    writes: frozenset[str] | None, effects: Iterable[Any]
) -> list[str]:
    """What these effects write that ``writes`` does not cover, one name per miss.

    Creating or removing rows (`Upsert`, `Delete`, `Retire`) needs the table. An `Update`
    needs the table or each column it sets. Effects that name no table are not checked.
    """
    if writes is None:
        return []
    missing: list[str] = []
    for effect in effects:
        if isinstance(effect, Update):
            if effect.model_label in writes:
                continue
            missing.extend(
                f"{effect.model_label}.{column}"
                for column in (effect.defaults or {})
                if not _covers(writes, effect.model_label, column)
            )
        elif isinstance(effect, (Upsert, Delete, Retire)):
            if effect.model_label not in writes:
                missing.append(effect.model_label)
    return missing


def reads_cover(reads: frozenset[str], label: str) -> bool:
    """Whether a reader call on ``label`` is declared.

    A reader returns whole rows, so a call is covered by the table or by any one of its
    columns: which column the handler then looks at cannot be seen here.
    """
    return label in reads or any(name.startswith(label + ".") for name in reads)


class CheckedReader:
    """Wraps a projection reader and records each table read that ``reads`` does not cover.

    It forwards every call unchanged; ``missing`` is read by replay after the handler returns.
    """

    def __init__(self, reader: Any, reads: frozenset[str]) -> None:
        self._reader = reader
        self._reads = reads
        self.missing: list[str] = []

    def _note(self, label: str) -> None:
        if not reads_cover(self._reads, label):
            self.missing.append(label)

    def get(self, model_label: str, /, **lookup: Any) -> Any | None:
        self._note(model_label)
        return self._reader.get(model_label, **lookup)

    def filter(self, model_label: str, /, **lookup: Any) -> Any:
        self._note(model_label)
        return self._reader.filter(model_label, **lookup)

    def query(self, model_label: str, /) -> Any:
        self._note(model_label)
        return self._reader.query(model_label)

    def __getattr__(self, name: str) -> Any:
        # Anything beyond the three reads (a backend's own helpers) passes through.
        # Private names do not, so copying or unpickling an instance (which asks
        # for attributes before `__init__` has set `_reader`) fails cleanly
        # instead of recursing.
        if name.startswith("_"):
            raise AttributeError(name)
        return getattr(self._reader, name)
