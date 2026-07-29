"""A tiny in-memory Executor — the `DjangoExecutor` contract, over dicts.

The point of this example is to exercise rakaia's effect/projection primitives
(`Ref`/`RefResolver`, `reconcile_aggregate(owns=)`, `reconcile_by_key`) *without*
Django, so this module stands in for `django_rakaia.effect_executor.DjangoExecutor`:
it applies a batch of `Effect`s to dict-backed "tables", assigns primary keys,
and resolves `Ref` placeholders through the real `rakaia.RefResolver` exactly the
way the Django executor does. It is deliberately small — it supports only the
lookup operators these demos use (`__in`, `__isnull`) — not a general ORM.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from rakaia import Effect, RefResolver


class DictExecutor:
    """Applies effects to in-memory tables; resolves Refs against the batch."""

    def __init__(self) -> None:
        # model_label -> {pk: row_dict}. Every row carries a synthetic "pk".
        self.tables: dict[str, dict[int, dict[str, Any]]] = {}
        self._next_pk = 1

    # -- row helpers --------------------------------------------------------

    def rows(self, model_label: str) -> list[dict[str, Any]]:
        """Every live row of a model, pk order — for asserting/printing state."""
        return [
            self.tables.get(model_label, {})[pk]
            for pk in sorted(self.tables.get(model_label, {}))
        ]

    def _table(self, model_label: str) -> dict[int, dict[str, Any]]:
        return self.tables.setdefault(model_label, {})

    @staticmethod
    def _matches(row: dict[str, Any], lookup: dict[str, Any]) -> bool:
        """Django-style filter matching for the operators these demos need."""
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
        return [
            r for r in self._table(model_label).values() if self._matches(r, lookup)
        ]

    @staticmethod
    def _spared(row: dict[str, Any], spare_keys: list[dict[str, Any]]) -> bool:
        return any(all(row.get(k) == v for k, v in key.items()) for key in spare_keys)

    # -- the Executor protocol ---------------------------------------------

    def apply(self, effects: Iterable[Effect]) -> None:
        resolver = RefResolver()
        for raw in effects:
            eff = resolver.resolve_effect(raw)  # substitute any Ref values
            table = self._table(eff.model_label or "")
            lookup = eff.lookup or {}

            if eff.op == "update_or_create":
                existing = self._find(eff.model_label, lookup)
                if existing:
                    row = existing[0]
                    row.update(eff.defaults or {})
                else:
                    row = {"pk": self._next_pk, **lookup, **(eff.defaults or {})}
                    table[self._next_pk] = row
                    self._next_pk += 1
                if eff.produces is not None:
                    # Let a sibling Ref bind to this row's pk (or any column).
                    resolver.record(
                        eff.produces,
                        lambda field, row=row: (
                            row["pk"] if field in ("pk", "id") else row.get(field)
                        ),
                    )

            elif eff.op == "update":
                # Update-if-exists: never mints a row (the multi-owner primitive).
                for row in self._find(eff.model_label, lookup):
                    row.update(eff.defaults or {})

            elif eff.op == "delete":
                doomed = self._find(eff.model_label, lookup)
                if eff.exclude:
                    doomed = [r for r in doomed if not self._matches(r, eff.exclude)]
                if eff.spare_keys is not None:
                    doomed = [r for r in doomed if not self._spared(r, eff.spare_keys)]
                for row in doomed:
                    del table[row["pk"]]

            elif eff.op == "retire":
                targets = self._find(eff.model_label, lookup)
                if eff.spare_keys is not None:
                    targets = [
                        r for r in targets if not self._spared(r, eff.spare_keys)
                    ]
                for row in targets:
                    row.update(eff.patch or {})

            elif eff.op == "external":
                # rakaia never applies external effects; replay filters them.
                continue
