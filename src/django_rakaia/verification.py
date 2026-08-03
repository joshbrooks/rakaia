"""
Verify that a batch of Effects reproduces the current Django projection rows.

The canonical migration proof is: run ``replay()`` with a
:class:`~rakaia.executors.CollectingExecutor`, then diff each write effect's
``defaults`` against the live row it targets — "does replaying the log reproduce
the projection we already have?" — without writing anything. Every port re-derives
the same diff plus the same normalization (a UUID column read back as a
``uuid.UUID`` vs a string in the effect; a JSON float vs the column's rounded
``Decimal``). That normalization is universal, so it belongs here rather than in
each consumer's ``replay_*`` proof.

    from rakaia.executors import CollectingExecutor
    from rakaia.replay import replay
    from django_rakaia.verification import diff_effects_against_rows

    ex = CollectingExecutor()
    replay(store, "submissions", ex)
    diff_effects_against_rows(ex.effects).raise_if_diff()

By default only ``update_or_create`` and ``update`` effects are checked (the ops
that carry ``defaults``); delete/retire/external effects have no target values to
diff and are ignored.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from uuid import UUID

from django.apps import apps
from django.core.exceptions import FieldDoesNotExist
from django.db.models import DecimalField, Q

from rakaia.effects import Effect

from .projection_reader import DjangoProjectionReader

# A normalizer coerces one value into a canonical, comparable form given the
# model and the field name it belongs to. It is applied to BOTH the effect's
# expected value and the row's stored value before they are compared, so a
# normalizer that doesn't apply must return the value unchanged.
Normalizer = Callable[[type, str, Any], Any]

# Ops whose ``defaults`` describe row values worth verifying. delete/retire/
# external carry no projected column values to diff.
_WRITE_OPS = ("update_or_create", "update")


class _Unset:
    """Sentinel for "the row has no such attribute" (distinct from ``None``)."""

    def __repr__(self) -> str:  # pragma: no cover - debug aid only
        return "<unset>"


_UNSET = _Unset()


# =============================================================================
# Report structures
# =============================================================================


@dataclass(frozen=True)
class FieldDiff:
    """One field whose stored value disagrees with the effect's ``defaults``."""

    field: str
    expected: Any
    actual: Any

    def __str__(self) -> str:
        return f"{self.field}: expected {self.expected!r}, got {self.actual!r}"


@dataclass(frozen=True)
class RowDiff:
    """The verification outcome for one write effect's target row."""

    model_label: str
    lookup: dict[str, Any]
    missing: bool
    field_diffs: list[FieldDiff] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.missing and not self.field_diffs

    def __str__(self) -> str:
        head = f"{self.model_label} {self.lookup!r}"
        if self.missing:
            return f"{head}: no matching row"
        return f"{head}: " + "; ".join(str(d) for d in self.field_diffs)


@dataclass(frozen=True)
class DiffReport:
    """Aggregate result of :func:`diff_effects_against_rows`.

    ``rows`` holds one :class:`RowDiff` per write effect checked (in effect
    order); ``problems`` is the subset that disagree. ``ok`` is the assertion a
    migration proof cares about.
    """

    rows: list[RowDiff]

    @property
    def problems(self) -> list[RowDiff]:
        return [r for r in self.rows if not r.ok]

    @property
    def ok(self) -> bool:
        return not self.problems

    def raise_if_diff(self) -> None:
        """Raise :class:`VerificationError` if any checked row disagrees."""
        if self.ok:
            return
        raise VerificationError(self)

    def __str__(self) -> str:
        problems = self.problems
        if not problems:
            return f"DiffReport: {len(self.rows)} row(s) verified, no differences"
        lines = [
            f"DiffReport: {len(problems)} of {len(self.rows)} row(s) differ:",
            *(f"  - {p}" for p in problems),
        ]
        return "\n".join(lines)


class VerificationError(AssertionError):
    """Raised by :meth:`DiffReport.raise_if_diff` when a projection disagrees."""

    def __init__(self, report: DiffReport) -> None:
        self.report = report
        super().__init__(str(report))


# =============================================================================
# Default normalizers
# =============================================================================


def normalize_uuid(_model: type, _field_name: str, value: Any) -> Any:
    """Render a ``uuid.UUID`` as its canonical string.

    A ``UUIDField`` reads back as a ``uuid.UUID`` while the effect (post JSON)
    usually carries the string form; string-ifying both sides makes them agree.
    """
    if isinstance(value, UUID):
        return str(value)
    return value


def normalize_decimal(model: type, field_name: str, value: Any) -> Any:
    """Quantize a value bound for a ``DecimalField`` to the column's scale.

    The log can carry more precision than the column stores — most commonly a
    JSON number decoded as a ``float`` (``2.1``), which does not compare equal to
    the column's ``Decimal("2.10")``. Coercing through ``str`` (to dodge binary
    float noise) and quantizing to ``decimal_places`` puts both sides in the
    column's representation. No-op for non-Decimal fields or ``None``.
    """
    if value is None:
        return value
    model_field = _resolve_field(model, field_name)
    if not isinstance(model_field, DecimalField):
        return value
    if isinstance(value, float):
        dec = Decimal(str(value))
    elif isinstance(value, (int, str)):
        dec = Decimal(value)
    elif isinstance(value, Decimal):
        dec = value
    else:
        return value
    quantum = Decimal(1).scaleb(-model_field.decimal_places)
    return dec.quantize(quantum)


DEFAULT_NORMALIZERS: tuple[Normalizer, ...] = (normalize_uuid, normalize_decimal)


def canonical_value(
    model: type,
    field_name: str,
    value: Any,
    normalizers: tuple[Normalizer, ...] = DEFAULT_NORMALIZERS,
) -> Any:
    """Coerce ``value`` into the comparable form the column stores.

    Applies ``normalizers`` in order (UUID→str, Decimal→column scale by default).
    Shared by :func:`diff_effects_against_rows` and the executor's
    ``skip_unchanged`` compare, so under the **default** normalizers "unchanged"
    means the same thing in the migration diff and on the write path — a value the
    DB would round or re-type is not counted as a change (ADR 0003 / P4). The skip
    path always uses :data:`DEFAULT_NORMALIZERS`; a diff given a custom
    ``normalizers=`` set is not mirrored there. See :data:`DEFAULT_NORMALIZERS`.
    """
    for norm in normalizers:
        value = norm(model, field_name, value)
    return value


# =============================================================================
# Public entry point
# =============================================================================


def diff_effects_against_rows(
    effects: Iterable[Effect],
    *,
    reader: DjangoProjectionReader | None = None,
    normalizers: Sequence[Normalizer] | None = None,
    ops: tuple[str, ...] = _WRITE_OPS,
) -> DiffReport:
    """Diff each write effect's ``defaults`` against its live projection row.

    For every effect whose ``op`` is in ``ops`` (default: the write ops), the row
    matching ``lookup`` is fetched via ``reader``. A missing row, or any field in
    ``defaults`` whose stored value differs (after ``normalizers`` are applied to
    both sides), is recorded in the returned :class:`DiffReport`.

    ``normalizers`` defaults to :data:`DEFAULT_NORMALIZERS` (UUID + Decimal); pass
    an explicit list to extend or replace them, or ``[]`` to compare raw values.
    """
    active_reader = reader if reader is not None else DjangoProjectionReader()
    active_norms = DEFAULT_NORMALIZERS if normalizers is None else tuple(normalizers)

    rows: list[RowDiff] = []
    for eff in effects:
        if eff.op not in ops:
            continue
        if eff.model_label is None or eff.lookup is None:
            raise ValueError(
                f"Effect with op={eff.op!r} requires model_label and lookup "
                "to be verified"
            )
        model = apps.get_model(eff.model_label)
        row = active_reader.get(eff.model_label, **eff.lookup)
        if row is None:
            rows.append(RowDiff(eff.model_label, eff.lookup, missing=True))
            continue
        field_diffs = _diff_row(model, row, eff.defaults or {}, active_norms)
        rows.append(
            RowDiff(eff.model_label, eff.lookup, missing=False, field_diffs=field_diffs)
        )
    return DiffReport(rows=rows)


# =============================================================================
# Batch-fetching reader (for large sweeps)
# =============================================================================


class PreloadedProjectionReader(DjangoProjectionReader):
    """A :class:`DjangoProjectionReader` that bulk-fetches, up front, the rows a
    batch of effects will look up — so each :meth:`get` serves from an in-memory
    snapshot instead of issuing one ``SELECT`` per effect.

    :func:`diff_effects_against_rows` does one ``reader.get`` per effect, which is
    one round-trip per effect: fine for a handful, thousands on a full reconcile
    sweep. Give the *same* batch to this reader and to the diff and that collapses
    to **one query per (model, lookup-shape) group**::

        effects = list(collecting_executor.effects)
        reader = PreloadedProjectionReader(effects, using="rebuild")
        diff_effects_against_rows(effects, reader=reader).raise_if_diff()

    Semantics and limits:

    * The cache is a **point-in-time snapshot** taken at construction. Use it for
      read-only verification, *not* during live staged replay — there the rows
      change as the replay writes them, so use a plain
      :class:`DjangoProjectionReader`.
    * A :meth:`get` for a lookup not in the batch (or one that spans a relation,
      e.g. ``field__gte``) falls back to a live query and memoises the result, so
      a repeat is free.
    * :meth:`filter` and :meth:`query` are inherited unchanged — always live. Only
      the exact-match :meth:`get` path (all :func:`diff_effects_against_rows`
      uses) is preloaded.
    """

    _MISSING = object()

    def __init__(self, effects: Iterable[Effect], *, using: str | None = None) -> None:
        super().__init__(using=using)
        # (model_label, canonical-key) -> row, or None for a recorded miss ("no
        # such row" — a real cached answer). A key *absent* from the map has never
        # been fetched, so get() does a live lookup and memoises it.
        self._cache: dict[tuple[str, tuple], Any] = {}
        self._preload(effects)

    def _preload(self, effects: Iterable[Effect]) -> None:
        # Group every usable lookup by its "shape" — (model_label, sorted field
        # names) — so one query per shape fetches all of its rows at once.
        by_shape: dict[tuple[str, tuple[str, ...]], list[dict[str, Any]]] = {}
        for eff in effects:
            if eff.model_label is None or not eff.lookup:
                continue
            if any("__" in k for k in eff.lookup):
                continue  # spanning lookup — can't index by exact match; left to live get()
            shape = (eff.model_label, tuple(sorted(eff.lookup)))
            by_shape.setdefault(shape, []).append(eff.lookup)

        for (model_label, fields), lookups in by_shape.items():
            model = apps.get_model(model_label)
            # De-dup requested keys and seed each as a miss; a fetched row overwrites.
            requested: dict[tuple[str, tuple], Any] = {}
            unique_lookups: list[dict[str, Any]] = []
            for lookup in lookups:
                key = (model_label, self._key(model, lookup.items()))
                if key not in requested:
                    requested[key] = None
                    unique_lookups.append(lookup)
            if not unique_lookups:
                continue
            for row in self._fetch(model_label, fields, unique_lookups):
                key = (
                    model_label,
                    self._key(model, [(f, getattr(row, f)) for f in fields]),
                )
                if (
                    requested.get(key) is None
                ):  # first row wins, mirroring get()->first()
                    requested[key] = row
            self._cache.update(requested)

    def _fetch(
        self, model_label: str, fields: tuple[str, ...], lookups: list[dict[str, Any]]
    ) -> Iterable[Any]:
        manager = self._manager(model_label)
        if len(fields) == 1:
            # Single natural key — a ``field__in=[...]`` beats an N-clause OR.
            (field,) = fields
            return manager.filter(**{f"{field}__in": [lk[field] for lk in lookups]})
        # Composite key — OR one Q per lookup. (Seed from None, not an empty
        # ``Q()``: ``Q() | Q(...)`` matches *everything*.)
        combined: Q | None = None
        for lookup in lookups:
            clause = Q(**lookup)
            combined = clause if combined is None else combined | clause
        return manager.filter(combined)

    @staticmethod
    def _key(model: type, items: Iterable[tuple[str, Any]]) -> tuple:
        """A field-order-independent key, canonicalised through the same
        normalizers as the diff so a str-UUID lookup keys to the same slot as the
        stored ``uuid.UUID`` row it matched."""
        return tuple(
            (f, canonical_value(model, f, v))
            for f, v in sorted(items, key=lambda kv: kv[0])
        )

    def get(self, model_label: str, /, **lookup: Any) -> Any | None:
        if any("__" in k for k in lookup):
            return super().get(model_label, **lookup)  # spanning — not preloadable
        model = apps.get_model(model_label)
        key = (model_label, self._key(model, lookup.items()))
        cached = self._cache.get(key, self._MISSING)
        if cached is not self._MISSING:
            return cached  # a preloaded hit, or a recorded miss (None)
        row = super().get(
            model_label, **lookup
        )  # outside the batch — live, then memoise
        self._cache[key] = row
        return row


# =============================================================================
# Internal
# =============================================================================


def _diff_row(
    model: type,
    row: Any,
    defaults: dict[str, Any],
    normalizers: tuple[Normalizer, ...],
) -> list[FieldDiff]:
    diffs: list[FieldDiff] = []
    for name, expected in defaults.items():
        actual = getattr(row, name, _UNSET)
        exp_c = _canonical(model, name, expected, normalizers)
        act_c = (
            _UNSET if actual is _UNSET else _canonical(model, name, actual, normalizers)
        )
        if exp_c != act_c:
            diffs.append(FieldDiff(field=name, expected=expected, actual=actual))
    return diffs


def _canonical(
    model: type, field_name: str, value: Any, normalizers: tuple[Normalizer, ...]
) -> Any:
    return canonical_value(model, field_name, value, normalizers)


def _resolve_field(model: type, name: str) -> Any | None:
    """The model field named ``name``, resolving an FK's ``attname`` (``x_id``).

    ``get_field`` knows the relation name (``project``) but not its column
    (``project_id``); effects address the column, so fall back to a scan by
    ``attname`` before giving up.
    """
    try:
        return model._meta.get_field(name)
    except FieldDoesNotExist:
        for candidate in model._meta.get_fields():
            if getattr(candidate, "attname", None) == name:
                return candidate
        return None
