"""
Django implementation of the rakaia.effects.Executor protocol.

Applies update_or_create, delete and retire Effects to the Django ORM in a
single atomic transaction. Upserts are applied first, then deletes, then
retires, so a batch that reconciles a row set (upsert the current rows, then
prune/soft-delete the stale ones) is deterministic. External effects passed to
apply() are silently dropped — the replay orchestrator decides whether external
effects reach the executor based on its include_external setting.

Pass ``skip_unchanged=True`` to avoid writing rows whose values already match.
By default (matching Django's ``update_or_create``) every upsert issues an
UPDATE, so re-materialising a large collection where one row changed rewrites
every row — churning ``auto_now`` columns, ``post_save`` signals, and
replication. The opt-in path instead fetches the row, compares the effect's
``defaults`` to the stored values, and writes only the changed columns (or skips
the write entirely when nothing changed). It trades one UPDATE per row for one
SELECT per row, so it pays off when writes are the expensive part — a big tree
with a single edit, or a reorder. It is opt-in because skipping a no-op write is
*observably* different: ``auto_now`` fields don't advance and ``post_save``
doesn't fire for an unchanged row.
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from django.apps import apps
from django.db import transaction
from django.db.models import Q

from rakaia.effects import Effect, check_disjoint_defaults


class DjangoExecutor:
    """Apply Effects via Django's ORM."""

    def __init__(self, *, skip_unchanged: bool = False) -> None:
        self._skip_unchanged = skip_unchanged

    def apply(self, effects: Iterable[Effect]) -> None:
        effects_list = list(effects)
        check_disjoint_defaults(effects_list)
        with transaction.atomic():
            # Upserts first, then deletes, then retires: reconcile batches
            # (upsert current rows, prune/soft-delete the rest) converge
            # regardless of handler order.
            for eff in effects_list:
                if eff.op == "update_or_create":
                    self._upsert(eff)
            for eff in effects_list:
                if eff.op == "delete":
                    self._delete(eff)
            for eff in effects_list:
                if eff.op == "retire":
                    self._retire(eff)

    def _upsert(self, eff: Effect) -> None:
        if eff.model_label is None or eff.lookup is None:
            raise ValueError("update_or_create effect requires model_label and lookup")
        model = apps.get_model(eff.model_label)
        defaults = eff.defaults or {}
        if not self._skip_unchanged:
            model.objects.update_or_create(**eff.lookup, defaults=defaults)
            return
        self._upsert_skip_unchanged(model, eff.lookup, defaults)

    @staticmethod
    def _upsert_skip_unchanged(model: type, lookup: dict, defaults: dict) -> None:
        try:
            row = model.objects.get(**lookup)
        except model.DoesNotExist:
            # Mirror update_or_create's create path: lookup fields + defaults,
            # dropping any lookup that isn't a concrete field assignment.
            params = {k: v for k, v in lookup.items() if "__" not in k}
            params.update(defaults)
            model.objects.create(**params)
            return
        changed = {k: v for k, v in defaults.items() if getattr(row, k) != v}
        if not changed:
            return  # nothing to write — skip the UPDATE entirely
        for field, value in changed.items():
            setattr(row, field, value)
        row.save(update_fields=list(changed))

    @staticmethod
    def _spare(qs: Any, spare_keys: list[dict[str, Any]] | None) -> Any:
        """Exclude a list of composite natural keys from a queryset.

        `filter(...).exclude(Q(**k0) | Q(**k1) | ...)` — the composite-key
        reconcile primitive. An empty list spares nothing (the queryset is
        returned unchanged, so the whole scope is affected)."""
        if not spare_keys:
            return qs
        spared = Q()
        for key in spare_keys:
            spared |= Q(**key)
        return qs.exclude(spared)

    @classmethod
    def _delete(cls, eff: Effect) -> None:
        if eff.model_label is None or eff.lookup is None:
            raise ValueError("delete effect requires model_label and lookup")
        model = apps.get_model(eff.model_label)
        qs = model.objects.filter(**eff.lookup).exclude(**(eff.exclude or {}))
        cls._spare(qs, eff.spare_keys).delete()

    @classmethod
    def _retire(cls, eff: Effect) -> None:
        if eff.model_label is None or eff.lookup is None:
            raise ValueError("retire effect requires model_label and lookup")
        if not eff.patch:
            raise ValueError("retire effect requires a non-empty patch")
        model = apps.get_model(eff.model_label)
        qs = model.objects.filter(**eff.lookup)
        cls._spare(qs, eff.spare_keys).update(**eff.patch)
