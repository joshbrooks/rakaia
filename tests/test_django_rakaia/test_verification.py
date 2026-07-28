"""Tests for ``diff_effects_against_rows`` — the projection-verification helper.

The migration pattern these back is: run ``replay()`` with a ``CollectingExecutor``
and diff each write effect's ``defaults`` against the live row, so a port can prove
"replaying the log reproduces the current projection" without touching the DB. See
issue #68 item #1.
"""

from __future__ import annotations

import json
import uuid
from decimal import Decimal

import pytest

from django_rakaia.store import get_store
from django_rakaia.verification import (
    VerificationError,
    diff_effects_against_rows,
)
from rakaia.effects import Effect
from rakaia.executors import CollectingExecutor
from rakaia.registry import HandlerRegistry, UpcasterRegistry
from rakaia.replay import replay

from .models import FinanceLine, Measure


@pytest.mark.django_db
class TestDiffEffectsAgainstRows:
    def test_all_effects_match_reports_ok(self):
        FinanceLine.objects.create(submission_id="s1", suku="A", delta=100)
        FinanceLine.objects.create(submission_id="s2", suku="B", delta=50)
        effects = [
            Effect(
                op="update_or_create",
                model_label="test_django_rakaia.FinanceLine",
                lookup={"submission_id": "s1"},
                defaults={"suku": "A", "delta": 100},
            ),
            Effect(
                op="update_or_create",
                model_label="test_django_rakaia.FinanceLine",
                lookup={"submission_id": "s2"},
                defaults={"suku": "B", "delta": 50},
            ),
        ]
        report = diff_effects_against_rows(effects)
        assert report.ok
        assert not report.problems
        assert len(report.rows) == 2

    def test_field_mismatch_is_reported_with_expected_and_actual(self):
        FinanceLine.objects.create(submission_id="s1", suku="A", delta=100)
        effects = [
            Effect(
                op="update_or_create",
                model_label="test_django_rakaia.FinanceLine",
                lookup={"submission_id": "s1"},
                defaults={"suku": "A", "delta": 999},
            ),
        ]
        report = diff_effects_against_rows(effects)
        assert not report.ok
        (row,) = report.problems
        assert not row.missing
        (diff,) = row.field_diffs
        assert diff.field == "delta"
        assert diff.expected == 999
        assert diff.actual == 100

    def test_missing_row_is_reported(self):
        effects = [
            Effect(
                op="update_or_create",
                model_label="test_django_rakaia.FinanceLine",
                lookup={"submission_id": "ghost"},
                defaults={"suku": "A", "delta": 1},
            ),
        ]
        report = diff_effects_against_rows(effects)
        assert not report.ok
        (row,) = report.problems
        assert row.missing
        assert row.field_diffs == []

    def test_empty_defaults_only_checks_existence(self):
        FinanceLine.objects.create(submission_id="s1", suku="A", delta=1)
        present = Effect(
            op="update_or_create",
            model_label="test_django_rakaia.FinanceLine",
            lookup={"submission_id": "s1"},
            defaults={},
        )
        absent = Effect(
            op="update_or_create",
            model_label="test_django_rakaia.FinanceLine",
            lookup={"submission_id": "ghost"},
            defaults={},
        )
        assert diff_effects_against_rows([present]).ok
        assert not diff_effects_against_rows([absent]).ok

    def test_non_write_effects_are_ignored(self):
        # A delete effect carries no defaults to verify; it must not be treated
        # as a row to diff (and must not crash the helper).
        effects = [
            Effect(
                op="delete",
                model_label="test_django_rakaia.FinanceLine",
                lookup={"submission_id": "whatever"},
            ),
        ]
        report = diff_effects_against_rows(effects)
        assert report.ok
        assert report.rows == []


@pytest.mark.django_db
class TestDefaultNormalizers:
    def test_uuid_object_matches_stored_uuid(self):
        ref = uuid.uuid4()
        Measure.objects.create(ref=ref, amount=Decimal("1.00"))
        # Effect carries the UUID as a string (as it would after JSON round-trip);
        # the stored column is a uuid.UUID. Without normalization these differ.
        effect = Effect(
            op="update_or_create",
            model_label="test_django_rakaia.Measure",
            lookup={"ref": str(ref)},
            defaults={"amount": Decimal("1.00")},
        )
        assert diff_effects_against_rows([effect]).ok

    def test_over_precise_float_matches_column_decimal(self):
        ref = uuid.uuid4()
        Measure.objects.create(ref=ref, amount=Decimal("2.10"))
        # A JSON payload decodes 2.10 as the float 2.1, which does NOT compare
        # equal to Decimal("2.10"). The decimal normalizer quantizes it to the
        # column's decimal_places so the diff is clean.
        effect = Effect(
            op="update_or_create",
            model_label="test_django_rakaia.Measure",
            lookup={"ref": str(ref)},
            defaults={"amount": 2.1},
        )
        report = diff_effects_against_rows([effect])
        assert report.ok, str(report)

    def test_normalization_can_be_disabled(self):
        ref = uuid.uuid4()
        Measure.objects.create(ref=ref, amount=Decimal("2.10"))
        effect = Effect(
            op="update_or_create",
            model_label="test_django_rakaia.Measure",
            lookup={"ref": str(ref)},
            defaults={"amount": 2.1},
        )
        # With no normalizers, the float-vs-Decimal mismatch surfaces.
        report = diff_effects_against_rows([effect], normalizers=[])
        assert not report.ok

    def test_custom_normalizer_is_applied(self):
        FinanceLine.objects.create(submission_id="s1", suku="a", delta=1)
        effect = Effect(
            op="update_or_create",
            model_label="test_django_rakaia.FinanceLine",
            lookup={"submission_id": "s1"},
            defaults={"suku": "A", "delta": 1},  # note upper-case
        )
        assert not diff_effects_against_rows([effect]).ok

        def casefold_norm(_model, _field_name, value):
            return value.casefold() if isinstance(value, str) else value

        report = diff_effects_against_rows([effect], normalizers=[casefold_norm])
        assert report.ok


def _finance_line_handler(event):
    return Effect(
        op="update_or_create",
        model_label="test_django_rakaia.FinanceLine",
        lookup={"submission_id": event["key"]},
        defaults={"suku": event["suku"], "delta": event["delta"]},
    )


@pytest.mark.django_db
class TestEndToEndCollectingExecutorProof:
    """The headline pattern: prove replay reproduces the projection, read-only."""

    def test_replay_effects_match_current_rows(self):
        FinanceLine.objects.create(submission_id="f1", suku="A", delta=100)
        FinanceLine.objects.create(submission_id="f2", suku="B", delta=50)

        store = get_store()
        store.delete("s")
        store.create("s")
        for event in (
            {
                "schema_version": 1,
                "kind": "FINANCE",
                "key": "f1",
                "suku": "A",
                "delta": 100,
            },
            {
                "schema_version": 1,
                "kind": "FINANCE",
                "key": "f2",
                "suku": "B",
                "delta": 50,
            },
        ):
            store.append("s", json.dumps(event).encode("utf-8"))

        reg = HandlerRegistry()
        reg.register(
            "finance",
            "FINANCE",
            _finance_line_handler,
            0,
            None,
            match_field="kind",
            stage=0,
        )
        ex = CollectingExecutor()
        replay(
            store,
            "s",
            ex,
            handler_registry=reg,
            upcaster_registry=UpcasterRegistry(),
        )

        report = diff_effects_against_rows(ex.effects)
        assert report.ok, str(report)

    def test_raise_if_diff_raises_on_mismatch_and_is_quiet_when_clean(self):
        FinanceLine.objects.create(submission_id="f1", suku="A", delta=1)
        good = Effect(
            op="update_or_create",
            model_label="test_django_rakaia.FinanceLine",
            lookup={"submission_id": "f1"},
            defaults={"suku": "A", "delta": 1},
        )
        diff_effects_against_rows([good]).raise_if_diff()  # no raise

        bad = Effect(
            op="update_or_create",
            model_label="test_django_rakaia.FinanceLine",
            lookup={"submission_id": "f1"},
            defaults={"suku": "A", "delta": 2},
        )
        with pytest.raises(VerificationError):
            diff_effects_against_rows([bad]).raise_if_diff()
