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

from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from typing import Any

from django.apps import apps
from django.db.models import Q

from rakaia.effects import Effect, Update, Upsert

from .canonicalisation import (
    DEFAULT_NORMALIZERS,
    Normalizer,
    canonical_value,
    normalize_decimal,
    normalize_temporal,
    normalize_uuid,
)
from .projection_reader import DjangoProjectionReader

# Re-exported so `from django_rakaia.verification import canonical_value` — the
# path this module's own docstring taught, and the one `django_rakaia.__init__`
# resolved — keeps working now the rule lives in `canonicalisation`. The names are
# Tier 1 (`django_rakaia.__all__`); which module they are defined in is not.
__all__ = [
    "DEFAULT_NORMALIZERS",
    "DiffReport",
    "FieldDiff",
    "Normalizer",
    "PreloadedProjectionReader",
    "RowDiff",
    "VerificationError",
    "canonical_value",
    "diff_effects_against_rows",
    "normalize_decimal",
    "normalize_temporal",
    "normalize_uuid",
]

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
