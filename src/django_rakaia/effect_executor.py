"""
Django implementation of the rakaia.effects.Executor protocol.

Applies ``Upsert``, ``Update``, ``Delete`` and ``Retire`` effects to the Django
ORM in a single atomic transaction: writes (upserts and in-place updates)
first, then deletes, then retires, so a batch that reconciles a row set is
deterministic. ``Update`` is ``filter(**lookup).update(**defaults)``: it
modifies matching rows in place and never inserts. An ``ExternalEffect`` never
reaches an executor; replay returns those to its caller.

Pass ``skip_unchanged=True`` to avoid writing rows whose values already match.
By default every upsert issues an UPDATE (like Django's ``update_or_create``);
the opt-in path fetches the row, compares ``defaults`` to the stored values and
writes only the changed columns, trading one UPDATE per row for one SELECT per
row. It is opt-in because skipping a no-op write is observable: ``auto_now``
fields don't advance and ``post_save`` doesn't fire for an unchanged row.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from enum import Enum
from typing import Any

from django.apps import apps
from django.db import DEFAULT_DB_ALIAS, connections, transaction
from django.db.models import Q

from rakaia.applying import RowAccessor, UpsertOutcome, apply_effects
from rakaia.effects import (
    ApplyReport,
    Delete,
    Effect,
    Exclude,
    Retire,
    SpareKeys,
    Update,
    Upsert,
)

from .canonicalisation import DEFAULT_NORMALIZERS, Normalizer, canonical_value

#: Value types a collapsed `Update` may write: the ones whose Python equality is
#: safe to read as "the same write".
#:
#: The grouping key compares `defaults` with ``==`` and ``hash`` and then writes
#: one member's values to every member's rows, so a type whose ``__eq__`` is
#: looser than what the column stores could group two different writes. This is
#: an allowlist because each attempt at listing what was unsafe was beaten by a
#: fresh route; being wrong costs only a collapse that does not happen.
#:
#: ``float``, ``Decimal``, ``datetime``, ``date``, ``time`` and ``UUID`` are
#: excluded, though not because they are demonstrably dangerous:
#: `tests/test_django_rakaia/test_update_batching_equivalence.py` measures that
#: for most of them nothing would change. See `_is_collapsible_value` for the one
#: widening, an `Enum` member over one of these types.
_COLLAPSIBLE_DEFAULTS: tuple[type, ...] = (
    type(None),
    bool,
    int,
    str,
    bytes,
)


def _is_collapsible_value(value: Any) -> bool:
    """Whether a `defaults` value may take part in a collapse.

    `_COLLAPSIBLE_DEFAULTS` by exact type, plus an `Enum` member (that is,
    `models.TextChoices` / `models.IntegerChoices`) whose *value* is one of those
    types and which compares the way that type does. The widening is needed
    because `type(v)` for a choices member is the enum class, not ``str``.

    Both conditions matter. ``type(v.value) in _COLLAPSIBLE_DEFAULTS`` rules out an
    enum over a value type whose equality ignores stored representation
    (``class E(float, Enum)``) and over a ``str`` *subclass*.
    ``type(v).__eq__ is type(v.value).__eq__`` rules out an enum that overrides
    equality: a case-insensitive ``__eq__``/``__hash__`` would group members
    ``'a'`` and ``'A'`` and write one's value to the other's rows. A plain `Enum`
    with no mixin fails the second test and stays excluded, since Django has no
    column form for it.
    """
    if type(value) in _COLLAPSIBLE_DEFAULTS:
        return True
    if isinstance(value, Enum):
        value_type = type(value.value)
        return (
            value_type in _COLLAPSIBLE_DEFAULTS
            and type(value).__eq__ is value_type.__eq__
        )
    return False


def _row_accessor(obj: Any) -> Any:
    """A `RefResolver` accessor over an applied ORM row: `field -> value`,
    with ``"pk"``/``"id"`` mapping to the primary key."""

    def get(field: str) -> Any:
        return obj.pk if field in ("pk", "id") else getattr(obj, field)

    return get


class DjangoExecutor:
    """Apply Effects via Django's ORM.

    Pass `using` to apply against a named database alias instead of the default,
    for example to replay a full rebuild into a disposable database and verify it
    without touching production. `using=None` targets the default alias. Pair
    with `DjangoProjectionReader(using=...)`.

    Pass `batch_updates=True` to collapse a fanned-out `Update` into one
    statement (#199). Consecutive updates on one model collapse when each matches
    a single field by equality on a non-null value and all write the same plain
    values: `str`, `bytes`, `int`, `bool`, `None`, or a `models.TextChoices` /
    `IntegerChoices` member over one of those. Anything else (an expression such
    as `F()`, `Case`, `Q` or `Subquery`; a composite or ``__`` lookup; a null
    lookup value; an unhashable value such as a JSON dict; any other type,
    including `Decimal`, `float` and dates) is applied on its own, exactly as
    with the flag off. It is off by default because the guard deciding what may
    collapse was wrong four times and each failure wrote wrong data rather than
    raising.

    Pass `normalizers` to define value-equality for `skip_unchanged` the same way
    `diff_effects_against_rows(normalizers=...)` defines it for the verify side;
    both default to `DEFAULT_NORMALIZERS`. Hand the same sequence to both, or a
    replay churns rows the diff calls unchanged (#160).
    """

    def __init__(
        self,
        *,
        skip_unchanged: bool = False,
        using: str | None = None,
        normalizers: Sequence[Normalizer] | None = None,
        batch_updates: bool = False,
    ) -> None:
        self._skip_unchanged = skip_unchanged
        self._batch_updates = batch_updates
        self._using = using
        self._normalizers = (
            DEFAULT_NORMALIZERS if normalizers is None else tuple(normalizers)
        )

    def _manager(self, model: type) -> Any:
        # `.using(None)` keeps default routing.
        return model.objects.using(self._using)

    def apply(self, effects: Iterable[Effect]) -> ApplyReport:
        # `apply_effects` owns order, refs and report; this supplies the four
        # writes, in one transaction on the executor's alias.
        with transaction.atomic(using=self._using):
            return apply_effects(
                effects,
                upsert=self._upsert_row,
                update_run=self._update_run,
                delete=self._delete,
                retire=self._retire,
            )

    def _upsert_row(self, eff: Upsert) -> tuple[UpsertOutcome, RowAccessor]:
        obj, outcome = self._upsert(eff)
        return outcome, _row_accessor(obj)

    def _upsert(self, eff: Upsert) -> tuple[Any, UpsertOutcome]:
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

    def _update_run(self, resolved: list[Update]) -> None:
        """Apply a run of consecutive `Update`s, collapsing what can be collapsed.

        Consecutive updates that share a model, share identical ``defaults``, and
        each match on one equality against the same non-null field become a
        single ``filter(field__in=[...]).update(**defaults)``; everything else
        applies one statement at a time. The run arrives with its Refs resolved,
        since two effects are only alike once a `Ref` has been substituted.

        The collapse preserves the answer because `Update` fires no signals and
        advances no ``auto_now``, and because re-applying literal defaults to a
        row cannot change it. Two members' rows look disjoint, but one member can
        move a row into a later member's value; writing the same literal twice is
        writing it once, while ``F("n") + 1`` twice is not. That is the one
        difference measured to change stored data
        (`tests/test_django_rakaia/test_update_batching_equivalence.py`, which
        shows it needs both an expression and overlapping rows). Requiring
        idempotent defaults is deliberately broader than that, since row sets can
        overlap by routes a lookup key cannot see (``area`` vs ``area_id``, a
        `Ref` and a literal resolving to one value). Order between groups is
        preserved, and a run ends at the first non-update.
        """
        run: list[Update] = []
        run_key: tuple[Any, ...] | None = None
        for eff in resolved:
            key = self._batch_key(eff)
            if run and key is not None and key == run_key:
                run.append(eff)
                continue
            self._flush_run(run)
            run, run_key = [eff], key
        self._flush_run(run)

    @staticmethod
    def _batch_key(eff: Update) -> tuple[Any, ...] | None:
        """A key two updates share iff they may be collapsed, else ``None``.

        ``None`` for anything without a safe ``__in`` form: a lookup that is not
        exactly one equality; a **null** lookup value (``filter(f=None)`` is
        ``f IS NULL``, but Django strips ``None`` from an ``__in`` list, so the
        effect would silently write nothing); a ``defaults`` value outside
        `_COLLAPSIBLE_DEFAULTS` (this excludes every query expression, because a
        non-idempotent write makes row-set overlap observable, and types whose
        equality ignores representation the column keeps); empty ``defaults``; and
        unhashable ``defaults``, which cannot be part of a key.
        """
        defaults = eff.defaults or {}
        if not defaults or len(eff.lookup) != 1:
            return None
        ((field, value),) = eff.lookup.items()
        if "__" in field or value is None:
            return None
        if not all(_is_collapsible_value(v) for v in defaults.values()):
            return None
        try:
            # `type(v)` is in the key because `1`, `1.0`, `True` and `Decimal("1")`
            # are equal and hash alike but write different rows to a text or JSON
            # column. Exact `type(v)` rather than `isinstance`: `bool` is an `int`.
            return (
                eff.model_label,
                field,
                frozenset((k, type(v), v) for k, v in defaults.items()),
            )
        except TypeError:
            return None  # an unhashable default value

    def _flush_run(self, run: list[Update]) -> None:
        """One statement for a collapsible run, else one per effect.

        A run only grows while its `_batch_key` is not ``None``, so a
        non-collapsible effect is always a run of one. A run of one stays on plain
        equality, so the ``IN`` form is only built where it has been shown safe.
        """
        if not run:
            return
        if not self._batch_updates or len(run) == 1:
            for eff in run:
                self._update(eff)
            return
        (field,) = run[0].lookup
        model = apps.get_model(run[0].model_label)
        values = [eff.lookup[field] for eff in run]
        defaults = run[0].defaults or {}

        if not self._values_are_bindable(model, field, values):
            for eff in run:
                self._update(eff)
            return

        manager = self._manager(model)
        # One statement per chunk: an `IN` list binds one parameter per value and
        # SQLite caps those at `max_query_params`. Every chunk is inside the
        # caller's `atomic()`, so a failure mid-way rolls the whole batch back.
        limit = self._bind_limit(len(defaults))
        for start in range(0, len(values), limit):
            manager.filter(**{f"{field}__in": values[start : start + limit]}).update(
                **defaults
            )

    def _bind_limit(self, reserved: int) -> int:
        """How many lookup values one statement may carry.

        `reserved` is the number of parameters the ``SET`` clause already needs.
        Backends that declare no cap (Postgres) get a large-but-finite chunk.
        """
        declared = getattr(
            connections[self._using or DEFAULT_DB_ALIAS].features,
            "max_query_params",
            None,
        )
        return max(1, (declared or 65_536) - reserved - 1)

    def _values_are_bindable(self, model: type, field: str, values: list[Any]) -> bool:
        """Whether every value can be bound as an ``IN`` member.

        Django compiles an out-of-range integer ``exact`` lookup to "matches
        nothing" but hands the same value to the driver inside an ``IN``, where
        SQLite raises and takes the rest of the batch with it. A run containing one
        is applied per effect instead.

        Only integer columns have a range to check, and asking for one elsewhere is
        not portable: `integer_field_range` answers ``(None, None)`` for an unknown
        internal type on SQLite and raises ``KeyError`` on Postgres.
        """
        internal_type = model._meta.get_field(field).get_internal_type()
        alias = self._using or DEFAULT_DB_ALIAS
        try:
            low, high = connections[alias].ops.integer_field_range(internal_type)
        except KeyError:
            return True  # not an integer column; no range to exceed
        if low is None or high is None:
            return True
        # No `bool` special case: `True` and `False` are inside every integer range.
        return all(not isinstance(v, int) or low <= v <= high for v in values)

    def _update(self, eff: Update) -> None:
        """Update-if-exists: update the rows matching `lookup` in place, never
        insert. A no-op when nothing matches or when `defaults` is empty.

        `skip_unchanged` is not wired here: `QuerySet.update()` is already a
        single UPDATE and does not advance `auto_now` columns."""
        defaults = eff.defaults or {}
        if not defaults:
            return  # nothing to write — a pure no-op
        model = apps.get_model(eff.model_label)
        self._manager(model).filter(**eff.lookup).update(**defaults)

    def _upsert_skip_unchanged(
        self, model: type, lookup: dict, defaults: dict
    ) -> tuple[Any, UpsertOutcome]:
        try:
            row = self._manager(model).get(**lookup)
        except model.DoesNotExist:
            # Mirror update_or_create's create path, dropping any lookup that
            # isn't a concrete field assignment.
            params = {k: v for k, v in lookup.items() if "__" not in k}
            params.update(defaults)
            return self._manager(model).create(**params), "created"
        # Compare through the field's canonical form, not raw ``!=``: the log can
        # carry a representation the column would round or re-type (a JSON float
        # for a DecimalField, a UUID string for a UUIDField). Same
        # `canonical_value` and normalizers as `diff_effects_against_rows`, so
        # "unchanged" means the same in the diff and here (#160).
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

        An empty list spares nothing: the queryset is returned unchanged."""
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
        # that flip NULL->set. Capture them before the UPDATE, only when the effect
        # asked to notify, so a plain retire costs no extra SELECT.
        #
        # `select_for_update` holds the captured rows for the rest of the
        # transaction: without it, under READ COMMITTED, a concurrent change could
        # make the reported flip set diverge from the rows the UPDATE flips. A
        # no-op on SQLite, which serialises writers anyway.
        flipped: list[dict[str, Any]] = []
        if eff.transition is not None:
            cols = list(eff.transition.key_fields)
            flipped = list(qs.select_for_update().order_by(*cols).values(*cols))
        qs.update(**eff.patch)
        return flipped
