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
from django.db.models import DecimalField

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
