"""
Reusable Executor implementations.

The `Executor` protocol (see effects.py) is applied by `replay()` to every batch
of effects it produces. Two implementations live here, neither of which needs a
database:

* `CollectingExecutor` records effects instead of applying them — the building
  block for a dry run and for migration verification: run `replay()` with one
  and inspect `.effects` to see exactly what a real executor would write, with
  zero side effects.
* `DictProjections` actually applies them, to dict-backed "tables", and reads
  them back through the same object — an `Executor` and a `ProjectionReader` in
  one. It is what lets a test, a demo or an example exercise the full
  effect/projection surface (`Ref`/`RefResolver`, `reconcile_*`, staged replay
  where stage 1 reads what stage 0 committed) without Django, and it is held to
  the same shared conformance contracts as `DjangoExecutor` and
  `DjangoProjectionReader` so "prove it in memory, ship it on Django" means
  something.
"""

from __future__ import annotations

from collections.abc import Iterable
from types import SimpleNamespace
from typing import Any

from .effects import ApplyReport, Effect, RefResolver, check_disjoint_defaults


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

    def apply(self, effects: Iterable[Effect]) -> ApplyReport:
        self.effects.extend(effects)
        # Records effects without applying them, so it observes no retire flips.
        return ApplyReport()


class DictProjections:
    """An in-memory `Executor` **and** `ProjectionReader` over dict-backed tables.

    It mirrors `django_rakaia.effect_executor.DjangoExecutor` where that is
    observable: all five ops, the three ordered passes (every write, then every
    delete, then every retire), one `RefResolver` per `apply()` call,
    `check_disjoint_defaults` before the first write, and `retire_flips` for a
    retire that opted into notifications. `tests/executor_contract.py` and
    `tests/projection_reader_contract.py` hold it and the Django pair to the same
    behaviour::

        proj = DictProjections()
        replay(store, "submissions", proj, reader=proj)
        proj.get("app.Balance", suku="A").total

    Deliberately not an ORM. It matches the lookup operators the effect
    primitives actually emit — exact match, ``__in`` and ``__isnull`` — and
    nothing else; no relations, no constraints, no transaction. Rows are plain
    dicts (read `rows()` to assert on them); the reader hands them out as
    `SimpleNamespace` so a handler reads ``row.suku`` exactly as it would an ORM
    instance.

    Every row is minted with a synthetic integer ``pk``, so ``Ref("x")`` — whose
    default field is the primary key, the FK case — resolves here as it does on
    Django, rather than being the one thing an in-memory run cannot rehearse.
    """

    def __init__(self) -> None:
        # model_label -> {pk: row_dict}. Every row carries a synthetic "pk".
        self.tables: dict[str, dict[int, dict[str, Any]]] = {}
        self._next_pk = 1

    # -- row helpers --------------------------------------------------------

    def rows(self, model_label: str) -> list[dict[str, Any]]:
        """Every live row of a model, pk order — for asserting/printing state."""
        table = self.tables.get(model_label, {})
        return [table[pk] for pk in sorted(table)]

    def _table(self, model_label: str) -> dict[int, dict[str, Any]]:
        return self.tables.setdefault(model_label, {})

    @staticmethod
    def _matches(row: dict[str, Any], lookup: dict[str, Any]) -> bool:
        """Django-style filter matching, for the operators effects emit."""
        for key, expected in lookup.items():
            if key.endswith("__in"):
                if row.get(key[:-4]) not in expected:
                    return False
            elif key.endswith("__isnull"):
                if (row.get(key[:-8]) is None) is not expected:
                    return False
            elif row.get(key) != expected:
                return False
        return True

    def _find(self, model_label: str, lookup: dict[str, Any]) -> list[dict[str, Any]]:
        """Matching rows in pk order — the stable order `get()` reads "first" from."""
        return [r for r in self.rows(model_label) if self._matches(r, lookup)]

    @staticmethod
    def _spared(row: dict[str, Any], spare_keys: list[dict[str, Any]]) -> bool:
        return any(all(row.get(k) == v for k, v in key.items()) for key in spare_keys)

    # -- the Executor protocol ---------------------------------------------

    def apply(self, effects: Iterable[Effect]) -> ApplyReport:
        effects_list = list(effects)
        check_disjoint_defaults(effects_list)
        # One resolver per batch: an upsert declaring produces= records its row,
        # and a sibling's Ref values are substituted before that sibling applies.
        resolver = RefResolver()
        retire_flips: list[tuple[Effect, list[dict[str, Any]]]] = []
        # Writes first, then deletes, then retires — so a reconcile batch
        # converges regardless of the order its handlers emitted, and a produces=
        # row is recorded before any later effect Refs it.
        for eff in effects_list:
            if eff.op == "update_or_create":
                self._upsert(resolver, resolver.resolve_effect(eff))
            elif eff.op == "update":
                self._update(resolver.resolve_effect(eff))
        for eff in effects_list:
            if eff.op == "delete":
                self._delete(resolver.resolve_effect(eff))
        for eff in effects_list:
            if eff.op == "retire":
                reff = resolver.resolve_effect(eff)
                flipped = self._retire(reff)
                if reff.transition_kind is not None:
                    retire_flips.append((reff, flipped))
        # op="external" is never applied — replay filters externals, and an
        # executor that sees one drops it.
        return ApplyReport(retire_flips=retire_flips)

    def _upsert(self, resolver: RefResolver, eff: Effect) -> None:
        label = eff.model_label or ""
        lookup = eff.lookup or {}
        existing = self._find(label, lookup)
        if existing:
            row = existing[0]
            row.update(eff.defaults or {})
        else:
            # Mirror update_or_create's create path: the lookup's concrete field
            # assignments plus defaults, dropping any operator lookup.
            row = {"pk": self._next_pk}
            row.update({k: v for k, v in lookup.items() if "__" not in k})
            row.update(eff.defaults or {})
            self._table(label)[self._next_pk] = row
            self._next_pk += 1
        if eff.produces is not None:
            # Let a sibling Ref bind to this row's pk (or any other column).
            resolver.record(
                eff.produces,
                lambda field, row=row: (
                    row["pk"] if field in ("pk", "id") else row.get(field)
                ),
            )

    def _update(self, eff: Effect) -> None:
        """Update-if-exists: never mints a row (the multi-owner primitive)."""
        if not eff.defaults:
            return
        for row in self._find(eff.model_label or "", eff.lookup or {}):
            row.update(eff.defaults)

    def _delete(self, eff: Effect) -> None:
        label = eff.model_label or ""
        doomed = self._find(label, eff.lookup or {})
        if eff.exclude:
            doomed = [r for r in doomed if not self._matches(r, eff.exclude)]
        if eff.spare_keys is not None:
            doomed = [r for r in doomed if not self._spared(r, eff.spare_keys)]
        table = self._table(label)
        for row in doomed:
            del table[row["pk"]]

    def _retire(self, eff: Effect) -> list[dict[str, Any]]:
        if not eff.patch:
            raise ValueError("retire effect requires a non-empty patch")
        targets = self._find(eff.model_label or "", eff.lookup or {})
        if eff.spare_keys is not None:
            targets = [r for r in targets if not self._spared(r, eff.spare_keys)]
        # The retire is open-guarded, so the rows it matches are exactly the ones
        # that flip. Capture their identities before the patch, and only when the
        # effect asked to notify.
        flipped: list[dict[str, Any]] = []
        if eff.transition_kind is not None:
            cols = list(eff.transition_key_fields or ())
            flipped = sorted(
                ({c: r.get(c) for c in cols} for r in targets),
                key=lambda identity: tuple(str(identity[c]) for c in cols),
            )
        for row in targets:
            row.update(eff.patch)
        return flipped

    # -- the ProjectionReader protocol --------------------------------------

    def get(self, model_label: str, /, **lookup: Any) -> Any | None:
        """The single row matching `lookup`, or None (never raises on absence)."""
        found = self._find(model_label, lookup)
        return SimpleNamespace(**found[0]) if found else None

    def filter(self, model_label: str, /, **lookup: Any) -> list[Any]:
        """The rows matching `lookup`."""
        return [SimpleNamespace(**r) for r in self._find(model_label, lookup)]

    def query(self, model_label: str, /) -> list[Any]:
        """Every row of the model."""
        return [SimpleNamespace(**r) for r in self.rows(model_label)]
