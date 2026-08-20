"""
Django implementation of the rakaia.effects.Executor protocol.

Applies ``Upsert``, ``Update``, ``Delete`` and ``Retire`` effects to the Django
ORM in a single atomic transaction. Writes (upserts and in-place updates) are
applied first, then deletes, then retires, so a batch that reconciles a row set
(upsert the current rows, then prune/soft-delete the stale ones) is
deterministic. ``Update`` is update-if-exists:
``filter(**lookup).update(**defaults)`` — it modifies matching rows in place and
never inserts, so a secondary owner of a multi-owned projection row can emit it
unconditionally. An ``ExternalEffect`` is not an ``Effect`` and never reaches an
executor: replay returns those to its caller.

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

from collections.abc import Iterable, Sequence
from typing import Any

from django.apps import apps
from django.db import transaction
from django.db.models import Q

from rakaia.effects import (
    ApplyReport,
    Delete,
    Effect,
    Exclude,
    RefResolver,
    Retire,
    SpareKeys,
    Update,
    Upsert,
    check_disjoint_defaults,
)

from .canonicalisation import DEFAULT_NORMALIZERS, Normalizer, canonical_value


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

    Pass `normalizers` to define value-equality for `skip_unchanged` the same way
    `diff_effects_against_rows(normalizers=...)` defines it for the verify side.
    Both default to `DEFAULT_NORMALIZERS`, so out of the box the two paths already
    agree; the parameter exists for the case where they otherwise could not. A
    consumer with a domain-specific normalizer (a currency rounding rule, say)
    previously had a diff that honoured it and a skip path that did not, which
    means a replay churned rows the diff called unchanged, and there was no
    argument that would have fixed it (#160). Hand the same sequence to both.
    """

    def __init__(
        self,
        *,
        skip_unchanged: bool = False,
        using: str | None = None,
        normalizers: Sequence[Normalizer] | None = None,
    ) -> None:
        self._skip_unchanged = skip_unchanged
        self._using = using
        self._normalizers = (
            DEFAULT_NORMALIZERS if normalizers is None else tuple(normalizers)
        )

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
        retire_flips: list[tuple[Retire, list[dict[str, Any]]]] = []
        created = written = skipped = 0
        with transaction.atomic(using=self._using):
            # Writes first, then deletes, then retires: reconcile batches
            # (upsert current rows, prune/soft-delete the rest) converge
            # regardless of handler order. Writes apply before deletes/retires,
            # so a produces= row is always recorded before any later effect that
            # refs it.
            # A run of consecutive `Update`s is applied together so a fanned-out
            # change costs one statement instead of one per row (#199). The run
            # is flushed the moment anything else appears, which is what keeps a
            # `Ref` in an update's lookup resolvable: it binds to a row an
            # earlier `Upsert` materialised, so an update must never be hoisted
            # above its producer.
            pending: list[Update] = []
            for eff in effects_list:
                if isinstance(eff, Upsert):
                    self._flush_updates(pending, resolver)
                    obj, outcome = self._upsert(resolver.resolve_effect(eff))
                    if outcome == "created":
                        created += 1
                        written += 1
                    elif outcome == "written":
                        written += 1
                    else:
                        skipped += 1
                    if eff.produces is not None:
                        resolver.record(eff.produces, _row_accessor(obj))
                elif isinstance(eff, Update):
                    pending.append(eff)
            self._flush_updates(pending, resolver)
            for eff in effects_list:
                if isinstance(eff, Delete):
                    self._delete(resolver.resolve_effect(eff))
            for eff in effects_list:
                if isinstance(eff, Retire):
                    reff = resolver.resolve_effect(eff)
                    flipped = self._retire(reff)
                    if reff.transition is not None:
                        retire_flips.append((reff, flipped))
        return ApplyReport(
            retire_flips=retire_flips,
            upserts_created=created,
            upserts_written=written,
            upserts_skipped=skipped,
        )

    def _upsert(self, eff: Upsert) -> tuple[Any, str]:
        """Apply one upsert; return the row and how it resolved.

        The outcome is `"created"`, `"written"` or `"skipped"` — see
        :class:`ApplyReport` for what the three mean. Only the `skip_unchanged`
        path can report `"skipped"`; `update_or_create` always writes.
        """
        model = apps.get_model(eff.model_label)
        defaults = eff.defaults or {}
        if not self._skip_unchanged:
            obj, was_created = self._manager(model).update_or_create(
                **eff.lookup, defaults=defaults
            )
            return obj, ("created" if was_created else "written")
        return self._upsert_skip_unchanged(model, eff.lookup, defaults)

    def _flush_updates(self, pending: list[Update], resolver: RefResolver) -> None:
        """Apply a run of consecutive `Update`s, collapsing what can be collapsed.

        Consecutive updates that share a model, share identical ``defaults``, and
        each match on **one equality against the same field** become a single
        ``filter(field__in=[…]).update(**defaults)``. Everything else applies one
        statement at a time, so a mixed batch degrades rather than raising.

        Why the collapse is sound rather than merely convenient:

        * `Update` is ``filter(...).update(...)`` — no signals, no ``auto_now``
          advance, no per-row return value a caller reads — so N statements that
          set the same columns to the same values are indistinguishable from one
          over the union of their rows.
        * **Two members of a group can never target the same row.** The group
          requires one equality on the same field, so different values match
          disjoint rows; and two members with the *same* value would be the same
          lookup, which `check_disjoint_defaults` has already rejected before
          anything applies. So order within a group cannot matter.
        * Order *between* groups is preserved, and a run ends at the first
          non-update, so nothing moves across an `Upsert`. Two updates with
          different defaults can match one row through different lookups —
          `check_disjoint_defaults` cannot see that, it keys on the exact lookup
          — and last-write-wins is the answer today, so their sequence is kept.

        Refs are resolved before grouping: a lookup value may be a `Ref` standing
        in for a row a sibling produced, and two effects are only alike once
        those are substituted.
        """
        if not pending:
            return
        resolved = [resolver.resolve_effect(eff) for eff in pending]
        pending.clear()

        run: list[Update] = []
        run_key: tuple[Any, ...] | None = None
        for eff in resolved:
            key = self._batch_key(eff)
            if run and key is not None and key == run_key:
                run.append(eff)
                continue
            self._apply_update_run(run)
            run, run_key = [eff], key
        self._apply_update_run(run)

    @staticmethod
    def _batch_key(eff: Update) -> tuple[Any, ...] | None:
        """A key two updates share iff they can be collapsed, else ``None``.

        ``None`` for anything that has no safe ``__in`` form: a lookup that is not
        exactly one equality (a composite key, or a ``__`` traversal, has no
        single column to gather), empty ``defaults`` (a no-op), or ``defaults``
        whose values are unhashable — a JSON column holds a dict, which cannot be
        part of a grouping key, and refusing to group is better than refusing to
        run.
        """
        defaults = eff.defaults or {}
        if not defaults or len(eff.lookup) != 1:
            return None
        (field,) = eff.lookup
        if "__" in field:
            return None
        try:
            return (eff.model_label, field, frozenset(defaults.items()))
        except TypeError:
            return None  # an unhashable default value

    def _apply_update_run(self, run: list[Update]) -> None:
        """One statement for a collapsible run, else one per effect."""
        if not run:
            return
        if len(run) == 1 or self._batch_key(run[0]) is None:
            for eff in run:
                self._update(eff)
            return
        (field,) = run[0].lookup
        values = [eff.lookup[field] for eff in run]
        model = apps.get_model(run[0].model_label)
        self._manager(model).filter(**{f"{field}__in": values}).update(
            **(run[0].defaults or {})
        )

    def _update(self, eff: Update) -> None:
        """Update-if-exists: update the rows matching `lookup` in place, never
        insert. A no-op when nothing matches or when `defaults` is empty.

        `skip_unchanged` is not wired here: `QuerySet.update()` already issues a
        single UPDATE and does not advance `auto_now` columns, so the write-churn
        concern that motivates the upsert's SELECT-first path does not apply."""
        defaults = eff.defaults or {}
        if not defaults:
            return  # nothing to write — a pure no-op
        model = apps.get_model(eff.model_label)
        self._manager(model).filter(**eff.lookup).update(**defaults)

    def _upsert_skip_unchanged(
        self, model: type, lookup: dict, defaults: dict
    ) -> tuple[Any, str]:
        try:
            row = self._manager(model).get(**lookup)
        except model.DoesNotExist:
            # Mirror update_or_create's create path: lookup fields + defaults,
            # dropping any lookup that isn't a concrete field assignment.
            params = {k: v for k, v in lookup.items() if "__" not in k}
            params.update(defaults)
            return self._manager(model).create(**params), "created"
        # Compare through the field's canonical form, not raw ``!=`` (P4): the log
        # can carry a representation the column would round or re-type — a JSON
        # float for a DecimalField, a UUID string for a UUIDField — which is not a
        # real change. This is the same `canonical_value` and the same default set
        # `diff_effects_against_rows` uses, so "unchanged" is defined identically
        # in the migration diff and here, and `skip_unchanged` does not churn a row
        # on every replay over a coercion-only difference.
        #
        # `normalizers=` is what makes that hold for a *custom* set too. Before it
        # existed the skip path was pinned to the defaults while a diff could be
        # given anything, so a consumer with its own normalizers had a verify path
        # and a write path that disagreed and no way to reconcile them (#160).
        changed = {
            k: v
            for k, v in defaults.items()
            if canonical_value(model, k, getattr(row, k), self._normalizers)
            != canonical_value(model, k, v, self._normalizers)
        }
        if not changed:
            return row, "skipped"  # nothing to write — skip the UPDATE entirely
        for field, value in changed.items():
            setattr(row, field, value)
        row.save(using=self._using, update_fields=list(changed))
        return row, "written"

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

    def _delete(self, eff: Delete) -> None:
        model = apps.get_model(eff.model_label)
        qs = self._manager(model).filter(**eff.lookup)
        if isinstance(eff.spare, Exclude):
            qs = qs.exclude(**eff.spare.lookup)
        elif isinstance(eff.spare, SpareKeys):
            qs = self._spare(qs, eff.spare.keys)
        qs.delete()

    def _retire(self, eff: Retire) -> list[dict[str, Any]]:
        if not eff.patch:
            raise ValueError("retire effect requires a non-empty patch")
        model = apps.get_model(eff.model_label)
        qs = self._manager(model).filter(**eff.lookup)
        qs = self._spare(qs, eff.spare.keys if eff.spare is not None else None)
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
        if eff.transition is not None:
            cols = list(eff.transition.key_fields)
            flipped = list(qs.select_for_update().order_by(*cols).values(*cols))
        qs.update(**eff.patch)
        return flipped
