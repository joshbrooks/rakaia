"""A payload timestamp must compare equal to the column it was encoded from (#83).

`canonical_value` normalizes the two representations that already bit people —
a `UUID` column read back as `uuid.UUID` versus a string in the effect, and a
JSON float versus the column's rounded `Decimal`. Temporal columns have exactly
the same problem and no normalizer, so a payload timestamp **never** compares
equal to the `DateTimeField` it came from.

Two distinct causes, both live:

* **Precision.** `DjangoJSONEncoder` truncates a datetime to milliseconds
  (`r[:23]`) and emits no fractional part at all when `microsecond == 0`.
  PostgreSQL stores microseconds. So a value with non-zero microseconds — nearly
  all of them — reads back from the column as `…05.123456` while the log carries
  `…05.123`.
* **Type.** A `DateField` payload is the string `"2026-01-02"`; the column reads
  back as `datetime.date`. A string and a date are never equal regardless of
  precision.

The consequences are worse than a noisy report. `canonical_value` is shared with
`DjangoExecutor(skip_unchanged=True)`, so every replay re-`UPDATE`s every row
with a temporal column — churning `auto_now` fields, re-firing `post_save`, and
re-replicating, forever, for a value that did not change. And
`diff_effects_against_rows` reports a permanent phantom difference, which trains
whoever reads it to ignore the report.

**Both sides truncate to milliseconds.** That is the load-bearing decision.
Parsing the payload alone is not enough: a parsed `…05.123` still won't equal a
stored `…05.123456`. The cost is that a genuine sub-millisecond change reads as
"unchanged" — accepted deliberately, because the alternative is a phantom diff on
essentially every row, and sub-millisecond precision cannot survive the log
anyway. The log is the source of truth, and the log only has milliseconds.
"""

from __future__ import annotations

import datetime as dt
import json
import uuid

import pytest
from django.core.serializers.json import DjangoJSONEncoder

from django_rakaia.verification import canonical_value, diff_effects_against_rows
from rakaia.effects import Effect

from .models import Measure

pytestmark = pytest.mark.django_db

MODEL = "test_django_rakaia.Measure"
REF = uuid.UUID("33333333-3333-3333-3333-333333333333")

# Non-zero microseconds: the case the encoder truncates, i.e. almost every value.
AWARE = dt.datetime(2026, 1, 2, 3, 4, 5, 123456, tzinfo=dt.timezone.utc)
ON = dt.date(2026, 1, 2)
AT_TIME = dt.time(3, 4, 5, 123456)


def _encoded(value):
    """Exactly what the payload carries: the value through the event encoder."""
    return json.loads(json.dumps(value, cls=DjangoJSONEncoder))


class TestTheEncoderLosesPrecision:
    """Characterization — the premise everything else rests on."""

    def test_a_datetime_is_truncated_to_milliseconds(self):
        assert _encoded(AWARE) == "2026-01-02T03:04:05.123Z"

    def test_a_whole_second_carries_no_fractional_part(self):
        assert _encoded(AWARE.replace(microsecond=0)) == "2026-01-02T03:04:05Z"

    def test_truncation_not_rounding(self):
        """`…999999` becomes `…999`, not `…06.000` — so a normalizer that
        rounded would disagree with the encoder on half of all values."""
        assert _encoded(AWARE.replace(microsecond=999999)).endswith(".999Z")


class TestCanonicalValueMakesThemAgree:
    """The RED core: every assertion here fails today."""

    def test_an_encoded_datetime_equals_the_column_value(self):
        payload = _encoded(AWARE)
        assert canonical_value(Measure, "observed_at", payload) == canonical_value(
            Measure, "observed_at", AWARE
        )

    def test_a_whole_second_datetime_equals_the_column_value(self):
        value = AWARE.replace(microsecond=0)
        assert canonical_value(
            Measure, "observed_at", _encoded(value)
        ) == canonical_value(Measure, "observed_at", value)

    def test_an_encoded_date_equals_the_column_value(self):
        assert canonical_value(Measure, "observed_on", _encoded(ON)) == canonical_value(
            Measure, "observed_on", ON
        )

    def test_an_encoded_time_equals_the_column_value(self):
        assert canonical_value(
            Measure, "observed_time", _encoded(AT_TIME)
        ) == canonical_value(Measure, "observed_time", AT_TIME)

    def test_a_genuinely_different_datetime_still_differs(self):
        """Normalizing must not collapse real differences into equality."""
        other = AWARE + dt.timedelta(seconds=1)
        assert canonical_value(
            Measure, "observed_at", _encoded(AWARE)
        ) != canonical_value(Measure, "observed_at", other)

    def test_a_difference_above_the_millisecond_floor_is_still_seen(self):
        other = AWARE.replace(microsecond=124456)  # 123ms -> 124ms
        assert canonical_value(
            Measure, "observed_at", _encoded(AWARE)
        ) != canonical_value(Measure, "observed_at", other)


class TestTheAcceptedCost:
    """Truncating both sides means sub-millisecond changes are invisible. Pinned
    so the trade-off is a decision on record rather than a surprise."""

    def test_a_sub_millisecond_change_reads_as_unchanged(self):
        a = AWARE.replace(microsecond=123456)
        b = AWARE.replace(microsecond=123999)
        assert canonical_value(Measure, "observed_at", a) == canonical_value(
            Measure, "observed_at", b
        )


class TestItOnlyTouchesTemporalColumns:
    def test_a_non_temporal_field_is_untouched(self):
        value = "2026-01-02T03:04:05.123Z"
        assert canonical_value(Measure, "ref", value) == value

    def test_none_is_untouched(self):
        assert canonical_value(Measure, "observed_at", None) is None

    def test_an_unparseable_string_is_left_alone(self):
        """Better to report a difference than to silently swallow a value the
        normalizer cannot interpret."""
        assert canonical_value(Measure, "observed_at", "not a timestamp") == (
            "not a timestamp"
        )

    def test_a_datetime_field_is_not_treated_as_a_date_field(self):
        """`DateTimeField` subclasses `DateField` in Django, so a normalizer
        that checks `DateField` first would truncate every datetime to its date.
        """
        normalized = canonical_value(Measure, "observed_at", _encoded(AWARE))
        assert isinstance(normalized, dt.datetime)


class TestTheEndToEndSymptom:
    """What #83 actually reports: a replay that re-UPDATEs forever, and a diff
    that shows a permanent phantom difference."""

    def test_a_replayed_timestamp_is_not_reported_as_a_difference(self):
        Measure.objects.create(
            ref=REF, amount=1, observed_at=AWARE, observed_on=ON, observed_time=AT_TIME
        )
        effect = Effect(
            op="update_or_create",
            model_label=MODEL,
            lookup={"ref": REF},
            defaults={
                "observed_at": _encoded(AWARE),
                "observed_on": _encoded(ON),
                "observed_time": _encoded(AT_TIME),
            },
        )
        report = diff_effects_against_rows([effect])
        assert report.certified, str(report)

    def test_skip_unchanged_does_not_rewrite_an_unchanged_row(self):
        """The executor shares `canonical_value`, so the fix has to land on the
        write path too — that is the half that costs real IO."""
        from django_rakaia.effect_executor import DjangoExecutor

        Measure.objects.create(ref=REF, amount=1, observed_at=AWARE)
        row = Measure.objects.get(ref=REF)
        before = row.pk

        DjangoExecutor(skip_unchanged=True).apply(
            [
                Effect(
                    op="update_or_create",
                    model_label=MODEL,
                    lookup={"ref": REF},
                    defaults={"observed_at": _encoded(AWARE)},
                )
            ]
        )

        row.refresh_from_db()
        assert row.pk == before
        # The stored value keeps its full microsecond precision — the normalizer
        # is a *comparison* device, and must not write a truncated value back.
        assert row.observed_at == AWARE
