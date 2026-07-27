"""
Django implementation of the rakaia.effects.Executor protocol.

Applies update_or_create, update, delete and retire Effects to the Django ORM in
a single atomic transaction. Writes (upserts and in-place updates) are applied
first, then deletes, then retires, so a batch that reconciles a row set (upsert
the current rows, then prune/soft-delete the stale ones) is deterministic.
``op="update"`` is update-if-exists: ``filter(**lookup).update(**defaults)`` —
it modifies matching rows in place and never inserts, so a secondary owner of a
multi-owned projection row can emit it unconditionally. External effects passed
to apply() are silently dropped — the replay orchestrator decides whether
external effects reach the executor based on its include_external setting.

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

from rakaia.effects import ApplyReport, Effect, RefResolver, check_disjoint_defaults


def _row_accessor(obj: Any) -> Any:
    """A `RefResolver` accessor over an applied ORM row: `field -> value`,
    with ``"pk"``/``"id"`` mapping to the primary key."""

    def get(field: str) -> Any:
        return obj.pk if field in ("pk", "id") else getattr(obj, field)

    return get


class DjangoExecutor:
    """Apply Effects via Django's ORM.

    Pass `using` to apply against a named database alias instead of the default
    — so a full from-scratch rebuild can be replayed into a *disposable*
    database (an in-memory sqlite, or a throwaway Postgres) and verified without
    touching production, at full ORM fidelity. `using=None` targets the default
    alias, exactly as before. Pair with `DjangoProjectionReader(using=...)`.
    """

    def __init__(
        self, *, skip_unchanged: bool = False, using: str | None = None
    ) -> None:
        self._skip_unchanged = skip_unchanged
        self._using = using

    def _manager(self, model: type) -> Any:
        # `.using(None)` keeps default routing, so this is uniform whether or
        # not an alias was given.
        return model.objects.using(self._using)

    def apply(self, effects: Iterable[Effect]) -> ApplyReport:
        effects_list = list(effects)
        check_disjoint_defaults(effects_list)
        # One resolver per batch: an upsert declaring produces= records its row,
        # and a sibling's Ref values are substituted before that sibling applies.
        resolver = RefResolver()
        retire_flips: list[tuple[Effect, list[dict[str, Any]]]] = []
        with transaction.atomic(using=self._using):
            # Writes first, then deletes, then retires: reconcile batches
            # (upsert current rows, prune/soft-delete the rest) converge
            # regardless of handler order. Writes apply before deletes/retires,
            # so a produces= row is always recorded before any later effect that
            # refs it.
            for eff in effects_list:
                if eff.op == "update_or_create":
                    obj = self._upsert(resolver.resolve_effect(eff))
                    if eff.produces is not None:
                        resolver.record(eff.produces, _row_accessor(obj))
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
        return ApplyReport(retire_flips=retire_flips)

    def _upsert(self, eff: Effect) -> Any:
        if eff.model_label is None or eff.lookup is None:
            raise ValueError("update_or_create effect requires model_label and lookup")
        model = apps.get_model(eff.model_label)
        defaults = eff.defaults or {}
        if not self._skip_unchanged:
            obj, _ = self._manager(model).update_or_create(
                **eff.lookup, defaults=defaults
            )
            return obj
        return self._upsert_skip_unchanged(model, eff.lookup, defaults)

    def _update(self, eff: Effect) -> None:
        """Update-if-exists: update the rows matching `lookup` in place, never
        insert. A no-op when nothing matches or when `defaults` is empty.

        `skip_unchanged` is not wired here: `QuerySet.update()` already issues a
        single UPDATE and does not advance `auto_now` columns, so the write-churn
        concern that motivates the upsert's SELECT-first path does not apply."""
        if eff.model_label is None or eff.lookup is None:
            raise ValueError("update effect requires model_label and lookup")
        defaults = eff.defaults or {}
        if not defaults:
            return  # nothing to write — a pure no-op
        model = apps.get_model(eff.model_label)
        self._manager(model).filter(**eff.lookup).update(**defaults)

    def _upsert_skip_unchanged(self, model: type, lookup: dict, defaults: dict) -> Any:
        try:
            row = self._manager(model).get(**lookup)
        except model.DoesNotExist:
            # Mirror update_or_create's create path: lookup fields + defaults,
            # dropping any lookup that isn't a concrete field assignment.
            params = {k: v for k, v in lookup.items() if "__" not in k}
            params.update(defaults)
            return self._manager(model).create(**params)
        changed = {k: v for k, v in defaults.items() if getattr(row, k) != v}
        if not changed:
            return row  # nothing to write — skip the UPDATE entirely
        for field, value in changed.items():
            setattr(row, field, value)
        row.save(using=self._using, update_fields=list(changed))
        return row

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

    def _delete(self, eff: Effect) -> None:
        if eff.model_label is None or eff.lookup is None:
            raise ValueError("delete effect requires model_label and lookup")
        model = apps.get_model(eff.model_label)
        qs = self._manager(model).filter(**eff.lookup).exclude(**(eff.exclude or {}))
        self._spare(qs, eff.spare_keys).delete()

    def _retire(self, eff: Effect) -> list[dict[str, Any]]:
        if eff.model_label is None or eff.lookup is None:
            raise ValueError("retire effect requires model_label and lookup")
        if not eff.patch:
            raise ValueError("retire effect requires a non-empty patch")
        model = apps.get_model(eff.model_label)
        qs = self._manager(model).filter(**eff.lookup)
        qs = self._spare(qs, eff.spare_keys)
        # The retire is open-guarded, so the rows it matches are exactly the ones
        # that flip NULL->set. Capture their identities BEFORE the UPDATE (same
        # atomic txn) only when the effect asked to notify — otherwise skip the
        # SELECT entirely so a plain retire costs nothing extra.
        #
        # `select_for_update` locks the captured rows for the rest of the txn so
        # a concurrent transaction can't resolve or otherwise mutate a captured
        # row between this SELECT and the UPDATE below — without the lock, under
        # READ COMMITTED the reported flip set could diverge from the rows the
        # UPDATE actually flips (a false or missed transition). No-op on SQLite,
        # which serialises writers anyway.
        flipped: list[dict[str, Any]] = []
        if eff.transition_kind is not None:
            cols = self._identity_cols(eff)
            flipped = list(qs.select_for_update().order_by(*cols).values(*cols))
        qs.update(**eff.patch)
        return flipped

    @staticmethod
    def _identity_cols(eff: Effect) -> list[str]:
        """Columns identifying a flipped row in its transition payload. The
        producer (`reconcile_by_key`) states the full ordered identity — scope
        columns plus the natural key — in ``transition_key_fields``, so the
        executor uses it verbatim for a deterministic ``ORDER BY``/``SELECT``
        rather than re-deriving it from ``lookup`` (which can't tell a scope
        column from the open-guard/retire_filter)."""
        return list(eff.transition_key_fields or ())
