"""A verification that compared nothing must not report itself clean.

`diff_effects_against_rows` answers "does replaying the log reproduce the
projection we already have?" — the go/no-go for a migration cutover. It answers
by diffing each write effect against its row, so with **no effects** it finds no
problems and reports success. "Nothing was wrong" and "nothing was looked at"
come back as the same verdict.

That is not hypothetical. The ways a sweep silently compares zero rows are all
ordinary: a store pointed at the wrong backend, a `replay()` over a stream path
that no longer exists, an `event_match` filter that stops matching after a
rename, a handler registry that failed to autodiscover. Each yields an empty
effect list, and the proof prints a clean bill of health with nothing behind it.

The first production consumer hit exactly this and named it **the vacuous
green**, adding a population guard to its own gates. These tests are that guard,
upstream. The rule it settled on, preserved here: *failures are checked before
vacuity* — if anything was compared and disagreed, report the disagreement. RED
is strictly safer than hiding a real failure behind "empty".
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from django_rakaia.verification import (
    GREEN,
    RED,
    VACUOUS,
    DiffReport,
    VacuousVerification,
    VerificationError,
    diff_effects_against_rows,
)
from rakaia.effects import Effect

from .models import Measure

REF = uuid.UUID("11111111-1111-1111-1111-111111111111")
OTHER_REF = uuid.UUID("22222222-2222-2222-2222-222222222222")


def _effect(ref: uuid.UUID, amount: str) -> Effect:
    return Effect(
        op="update_or_create",
        model_label="test_django_rakaia.Measure",
        lookup={"ref": ref},
        defaults={"amount": Decimal(amount)},
    )


class TestAnEmptyReportIsNotClean:
    """The RED core: today every assertion below fails."""

    def test_an_empty_report_is_vacuous_not_green(self):
        assert DiffReport(rows=[]).verdict == VACUOUS

    def test_raise_if_diff_refuses_to_certify_an_empty_population(self):
        with pytest.raises(VacuousVerification):
            DiffReport(rows=[]).raise_if_diff()

    def test_an_empty_report_says_it_certifies_nothing(self):
        text = str(DiffReport(rows=[]))
        assert "NOTHING COMPARED" in text
        assert "certifies nothing" in text

    def test_certified_is_false_for_an_empty_report(self):
        assert DiffReport(rows=[]).certified is False


class TestVerdictOrdering:
    """Failures are checked *before* vacuity — a real failure must never be
    reported as "empty"."""

    @pytest.mark.django_db
    def test_a_compared_and_matching_population_is_green(self):
        Measure.objects.create(ref=REF, amount=Decimal("10.00"))
        report = diff_effects_against_rows([_effect(REF, "10.00")])
        assert report.verdict == GREEN
        assert report.certified is True
        report.raise_if_diff()  # must not raise

    @pytest.mark.django_db
    def test_a_mismatch_is_red(self):
        Measure.objects.create(ref=REF, amount=Decimal("10.00"))
        report = diff_effects_against_rows([_effect(REF, "99.00")])
        assert report.verdict == RED
        assert report.certified is False
        with pytest.raises(VerificationError):
            report.raise_if_diff()

    @pytest.mark.django_db
    def test_a_missing_row_is_red_not_vacuous(self):
        """A lookup that finds nothing *was* compared — it is a failure, not an
        empty population."""
        report = diff_effects_against_rows([_effect(OTHER_REF, "1.00")])
        assert report.verdict == RED
        with pytest.raises(VerificationError):
            report.raise_if_diff()


class TestTheWaysASweepSilentlyComparesNothing:
    """Each of these produces an empty effect list in the wild."""

    @pytest.mark.django_db
    def test_no_effects_at_all(self):
        with pytest.raises(VacuousVerification):
            diff_effects_against_rows([]).raise_if_diff()

    @pytest.mark.django_db
    def test_only_non_write_effects_were_produced(self):
        """delete/retire/external effects are skipped, so a batch of only those
        compares nothing — the report must say so rather than report clean."""
        effects = [
            Effect(
                op="delete",
                model_label="test_django_rakaia.Measure",
                lookup={"ref": OTHER_REF},
            ),
            Effect(op="external", payload={"kind": "notify"}),
        ]
        report = diff_effects_against_rows(effects)
        assert report.verdict == VACUOUS
        with pytest.raises(VacuousVerification):
            report.raise_if_diff()


class TestOptingOut:
    """A caller who genuinely expects an empty population must be able to say
    so — explicitly, at the call site, never by default."""

    def test_allow_empty_downgrades_vacuous_to_a_pass(self):
        DiffReport(rows=[]).raise_if_diff(allow_empty=True)

    @pytest.mark.django_db
    def test_allow_empty_does_not_suppress_a_real_failure(self):
        report = diff_effects_against_rows([_effect(OTHER_REF, "1.00")])
        with pytest.raises(VerificationError):
            report.raise_if_diff(allow_empty=True)


class TestBackwardCompatibility:
    """`ok` keeps its old meaning — "no problems" — so existing callers and the
    `VerificationError` message are unaffected. `certified` is the new, stricter
    property a migration proof should assert."""

    def test_ok_is_still_true_for_an_empty_report(self):
        assert DiffReport(rows=[]).ok is True

    def test_vacuous_verification_is_an_assertion_error(self):
        """So `except AssertionError` / a bare pytest.raises in existing proofs
        still catches it."""
        assert issubclass(VacuousVerification, AssertionError)

    @pytest.mark.django_db
    def test_compared_counts_only_what_was_checked(self):
        Measure.objects.create(ref=REF, amount=Decimal("10.00"))
        report = diff_effects_against_rows(
            [
                _effect(REF, "10.00"),
                Effect(
                    op="delete",
                    model_label="test_django_rakaia.Measure",
                    lookup={"ref": OTHER_REF},
                ),
            ]
        )
        assert report.compared == 1
