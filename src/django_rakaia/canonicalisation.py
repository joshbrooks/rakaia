"""What it means for a stored value to differ from the one the log carries.

A projection column and the log disagree about representation, permanently and
predictably. A `UUIDField` reads back as a `uuid.UUID` while the event carries the
string. A JSON number decodes to a `float` (``2.1``) where the column holds
``Decimal("2.10")``. `DjangoJSONEncoder` truncates a datetime to milliseconds
while the database keeps microseconds. None of those are changes, and a system
that counted them as changes would report a difference on the same rows forever.

`canonical_value` is the one answer to "are these the same value", and it lives
here because **two paths need it and neither owns it**:

* the **verify** path (`verification.diff_effects_against_rows`) asks whether
  replaying the log reproduces the rows already there;
* the **write** path (`effect_executor.DjangoExecutor(skip_unchanged=True)`) asks
  whether a write is a no-op it can skip.

Those are the same question, and if the two answer it differently the failure is
quiet: a diff reporting a clean rebuild while the executor rewrites every row
over a coercion, or the reverse. The rule previously lived in `verification.py`,
which made the write path import the verify path to reach it and left only the
verify path able to be handed a different normalizer set — so no executor could be
built that agreed with a diff run under a custom one (#160). Now both import from
here and neither depends on the other.

A **normalizer** takes ``(model, field_name, value)`` and returns the value in the
column's comparable form, unchanged if it does not apply. It is applied to *both*
sides of a comparison, never written back, so a column keeps its full precision.
"""

from __future__ import annotations

import datetime as _dt
from collections.abc import Callable
from decimal import Decimal
from typing import Any
from uuid import UUID

from django.core.exceptions import FieldDoesNotExist
from django.db.models import DateField, DateTimeField, DecimalField, TimeField
from django.utils.dateparse import parse_date, parse_datetime, parse_time

# A normalizer coerces one value into a canonical, comparable form given the
# model and the field name it belongs to. It is applied to BOTH the effect's
# expected value and the row's stored value before they are compared, so a
# normalizer that doesn't apply must return the value unchanged.
Normalizer = Callable[[type, str, Any], Any]


def _resolve_field(model: type, name: str) -> Any | None:
    """The model field named ``name``, or ``None``.

    Effects address the *column* (``project_id``), not the relation
    (``project``), and ``get_field`` resolves both: `Options._forward_fields_map`
    indexes each field under `name` **and** `attname`, with a comment in Django's
    own source saying ``get_field()`` should be able to fetch by attname. That
    holds identically in Django 4.2 (the declared floor) and 6.0.

    This used to fall back to scanning `get_fields()` by ``attname`` when
    ``get_field`` raised. The loop was unreachable across the whole supported
    range — anything it could have found, ``get_field`` had already returned —
    so it is gone rather than kept as untestable insurance.
    """
    try:
        return model._meta.get_field(name)
    except FieldDoesNotExist:
        return None


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
#: This set is the default for both `diff_effects_against_rows` and the executor's
#: ``skip_unchanged`` compare, so out of the box "unchanged" means the same thing
#: on the read and write paths. Both also take ``normalizers=``, so a custom set
#: can be given to each and they still agree — which was the point of #160. Hand
#: the same sequence to both; there is no longer a path that silently keeps these
#: defaults while the other honours something else.
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
    The single definition of value-equality behind both
    :func:`~django_rakaia.verification.diff_effects_against_rows` and the
    executor's ``skip_unchanged`` compare, so "unchanged" means the same thing in
    the migration diff and on the write path — a value the DB would round or
    re-type is not counted as a change (ADR 0003 / P4).

    Both callers default to :data:`DEFAULT_NORMALIZERS` and both accept a
    ``normalizers=`` set, so that agreement holds for a custom set too as long as
    the same sequence is given to each (#160).
    """
    for norm in normalizers:
        value = norm(model, field_name, value)
    return value
