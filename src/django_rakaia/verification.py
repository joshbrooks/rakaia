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

By default only :class:`~rakaia.effects.Upsert` and
:class:`~rakaia.effects.Update` effects are checked (the ones that carry
``defaults``); deletes and retires have no target values to diff and are
ignored, and external effects never reach a batch of Effects at all.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Callable, Iterable, Sequence
from dataclasses import dataclass, field
from decimal import Decimal
from typing import Any
from uuid import UUID

from django.apps import apps
from django.core.exceptions import FieldDoesNotExist
from django.db.models import DateField, DateTimeField, DecimalField, Q, TimeField
from django.utils.dateparse import parse_date, parse_datetime, parse_time

from rakaia.effects import Effect, Update, Upsert

from .projection_reader import DjangoProjectionReader

# A normalizer coerces one value into a canonical, comparable form given the
# model and the field name it belongs to. It is applied to BOTH the effect's
# expected value and the row's stored value before they are compared, so a
# normalizer that doesn't apply must return the value unchanged.
Normalizer = Callable[[type, str, Any], Any]

# The effects whose ``defaults`` describe row values worth verifying. Deletes
# and retires carry no projected column values to diff.
_WRITE_EFFECTS: tuple[type, ...] = (Upsert, Update)


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


#: The three verdicts a verification sweep can reach. ``VACUOUS`` is deliberately
#: distinct from both others: "nothing was wrong" and "nothing was looked at" are
#: different facts, and a proof that conflates them is worthless as evidence.
GREEN, RED, VACUOUS = "green", "red", "vacuous"


@dataclass(frozen=True)
class DiffReport:
    """Aggregate result of :func:`diff_effects_against_rows`.

    ``rows`` holds one :class:`RowDiff` per write effect checked (in effect
    order); ``problems`` is the subset that disagree.

    **The population guard.** ``ok`` answers "did anything disagree?", which is
    vacuously ``True`` when nothing was compared. That made a sweep over an empty
    effect list — a store on the wrong backend, a replay over a renamed stream
    path, an ``event_match`` filter that stopped matching, a registry that failed
    to autodiscover — print a clean bill of health with nothing behind it. Read
    :attr:`verdict` (or :attr:`certified`) instead: it separates `GREEN` from
    `VACUOUS`, and :meth:`raise_if_diff` refuses to certify a zero population.

    ``ok`` keeps its original meaning so existing callers are unaffected.
    """

    rows: list[RowDiff]

    @property
    def problems(self) -> list[RowDiff]:
        return [r for r in self.rows if not r.ok]

    @property
    def compared(self) -> int:
        """How many rows this report actually checked.

        The population the verdict rests on. Note this counts *checked* rows, not
        input effects: deletes and retires carry no values to diff and are
        skipped, so a batch of only those compares nothing.
        """
        return len(self.rows)

    @property
    def ok(self) -> bool:
        """Whether any checked row disagreed.

        Vacuously ``True`` for an empty population — prefer :attr:`certified`.
        """
        return not self.problems

    @property
    def verdict(self) -> str:
        """`GREEN`, `RED` or `VACUOUS`.

        **Failures are checked before vacuity.** If anything disagreed then
        something was evidently compared, and reporting `RED` is strictly safer
        than hiding a real failure behind "empty".
        """
        if self.problems:
            return RED
        if self.compared == 0:
            return VACUOUS
        return GREEN

    @property
    def certified(self) -> bool:
        """Whether this report is positive evidence: a non-empty population, all
        of it matching. The assertion a migration proof wants."""
        return self.verdict == GREEN

    def raise_if_diff(self, *, allow_empty: bool = False) -> None:
        """Raise unless this report certifies the projection.

        Raises :class:`VerificationError` if any checked row disagrees, or
        :class:`VacuousVerification` if nothing was compared at all. Pass
        ``allow_empty=True`` when an empty population is genuinely expected —
        explicitly, at the call site. It never suppresses a real failure.
        """
        verdict = self.verdict
        if verdict == RED:
            raise VerificationError(self)
        if verdict == VACUOUS and not allow_empty:
            raise VacuousVerification(self)

    def __str__(self) -> str:
        problems = self.problems
        if problems:
            lines = [
                f"DiffReport: {len(problems)} of {self.compared} row(s) differ:",
                *(f"  - {p}" for p in problems),
            ]
            return "\n".join(lines)
        if self.compared == 0:
            return (
                "DiffReport: NOTHING COMPARED (rows=0) — this run certifies "
                "nothing. No write effects reached the diff: check the store "
                "backend, the stream path, and that handlers were registered."
            )
        return f"DiffReport: {self.compared} row(s) verified, no differences"


class VerificationError(AssertionError):
    """Raised by :meth:`DiffReport.raise_if_diff` when a projection disagrees."""

    def __init__(self, report: DiffReport) -> None:
        self.report = report
        super().__init__(str(report))


class VacuousVerification(AssertionError):
    """Raised by :meth:`DiffReport.raise_if_diff` when nothing was compared.

    A sibling of :class:`VerificationError`, not a subclass: they mean opposite
    things. `VerificationError` says the projection is wrong; this says the run
    produced no evidence either way, so treating it as a pass would be a false
    green. Both subclass `AssertionError`, so a proof written as
    ``except AssertionError`` still catches either.
    """

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


#: Microseconds the event encoder keeps. `DjangoJSONEncoder` renders a datetime
#: as ``o.isoformat()`` sliced to ``r[:23]``, i.e. **milliseconds, truncated not
#: rounded**, and omits the fractional part entirely when ``microsecond == 0``.
#: A `TimeField` gets the same treatment via ``r[:12]``.
_ENCODER_PRECISION_US = 1000


def _truncate_to_encoder_precision(value: Any) -> Any:
    """Drop sub-millisecond microseconds, matching what the encoder kept."""
    micro = getattr(value, "microsecond", 0)
    if not micro:
        return value
    return value.replace(
        microsecond=(micro // _ENCODER_PRECISION_US) * _ENCODER_PRECISION_US
    )


def normalize_temporal(model: type, field_name: str, value: Any) -> Any:
    """Put a datetime/date/time and its encoded payload in the same form.

    Two things go wrong without this, and both are permanent rather than
    intermittent:

    * **Precision.** The encoder truncates to milliseconds; the database stores
      microseconds. So a stored ``…05.123456`` never equals the log's
      ``…05.123`` — for essentially every value, since most have non-zero
      microseconds.
    * **Type.** A `DateField` payload is the string ``"2026-01-02"`` while the
      column reads back as `datetime.date`. Those are never equal at all.

    **Both sides are truncated to milliseconds.** Parsing the payload is not
    sufficient on its own — a parsed ``…05.123`` still won't equal a stored
    ``…05.123456``, so the column value has to come down to meet it. The cost is
    that a genuine sub-millisecond change reads as unchanged; that is accepted,
    because sub-millisecond precision cannot survive the log in the first place,
    and the alternative is a phantom difference on every row forever (#83).

    This is a **comparison** device only: it never writes a truncated value back,
    so the column keeps its full precision.

    A string the parser cannot interpret is returned unchanged — reporting a
    difference beats silently swallowing a value we failed to understand.
    """
    if value is None:
        return value

    model_field = _resolve_field(model, field_name)

    # DateTimeField subclasses DateField, so it must be tested first — otherwise
    # every timestamp would be truncated to its calendar date.
    if isinstance(model_field, DateTimeField):
        if isinstance(value, str):
            parsed = parse_datetime(value)
            if parsed is None:
                return value
            value = parsed
        if not isinstance(value, _dt.datetime):
            return value
        return _truncate_to_encoder_precision(value)

    if isinstance(model_field, DateField):
        if isinstance(value, str):
            parsed = parse_date(value)
            if parsed is None:
                return value
            return parsed
        # A datetime is also a date; keep it as-is rather than guessing.
        return value

    if isinstance(model_field, TimeField):
        if isinstance(value, str):
            parsed = parse_time(value)
            if parsed is None:
                return value
            value = parsed
        if not isinstance(value, _dt.time):
            return value
        return _truncate_to_encoder_precision(value)

    return value


#: The normalizers applied unless a caller passes its own set.
#:
#: Each exists because one representation survives the log and a different one
#: comes back from the column, so a value that never changed would otherwise
#: read as a difference forever:
#:
#: * `normalize_uuid` — ``uuid.UUID`` from the column vs. its string in the log.
#: * `normalize_decimal` — a JSON float (``2.1``) vs. the column's ``Decimal("2.10")``.
#: * `normalize_temporal` — millisecond-truncated timestamps, and dates/times
#:   that arrive as strings.
#:
#: **Truncation semantics (temporal).** `DjangoJSONEncoder` truncates — does not
#: round — a datetime to milliseconds, and omits the fractional part entirely
#: when ``microsecond == 0``; a `TimeField` gets the same treatment. Since the
#: database keeps microseconds, **both** sides are truncated to milliseconds
#: before comparing. The deliberate cost: a genuine sub-millisecond change is
#: reported as unchanged and skipped. That is the right trade because
#: sub-millisecond precision cannot survive the log at all, so a value the log
#: can never distinguish is not a difference the projection should act on —
#: whereas not truncating means a phantom difference on essentially every row
#: with a timestamp, forever (#83).
#:
#: This set is shared by `diff_effects_against_rows` and the executor's
#: ``skip_unchanged`` compare, so "unchanged" means the same thing on the read
#: and write paths. A diff given a custom ``normalizers=`` set is **not**
#: mirrored on the skip path.
DEFAULT_NORMALIZERS: tuple[Normalizer, ...] = (
    normalize_uuid,
    normalize_decimal,
    normalize_temporal,
)


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
    kinds: tuple[type, ...] = _WRITE_EFFECTS,
) -> DiffReport:
    """Diff each write effect's ``defaults`` against its live projection row.

    For every effect that is an instance of one of ``kinds`` (default: the two
    write effects), the row matching ``lookup`` is fetched via ``reader``. A missing row, or any field in
    ``defaults`` whose stored value differs (after ``normalizers`` are applied to
    both sides), is recorded in the returned :class:`DiffReport`.

    ``normalizers`` defaults to :data:`DEFAULT_NORMALIZERS` (UUID + Decimal); pass
    an explicit list to extend or replace them, or ``[]`` to compare raw values.
    """
    active_reader = reader if reader is not None else DjangoProjectionReader()
    active_norms = DEFAULT_NORMALIZERS if normalizers is None else tuple(normalizers)

    rows: list[RowDiff] = []
    for eff in effects:
        if not isinstance(eff, kinds):
            continue
        model = apps.get_model(eff.model_label)
        row = active_reader.get(eff.model_label, **eff.lookup)
        if row is None:
            rows.append(RowDiff(eff.model_label, eff.lookup, missing=True))
            continue
        defaults = getattr(eff, "defaults", None) or {}
        field_diffs = _diff_row(model, row, defaults, active_norms)
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
            if not eff.lookup:
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
