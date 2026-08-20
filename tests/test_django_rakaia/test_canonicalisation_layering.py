"""One definition of "did this value change", reachable from both sides.

Deciding whether a stored value differs from the one the log carries is a domain
question — what the column's representation of a value *means* — and two paths
depend on the answer:

* the **verify** path (`diff_effects_against_rows`) asks "does replaying the log
  reproduce the rows we already have?";
* the **write** path (`DjangoExecutor(skip_unchanged=True)`) asks "is this write
  a no-op I can skip?".

They must agree. If the diff says a row is unchanged and the executor rewrites it
anyway, a replay churns every row on a coercion-only difference; if they disagree
the other way, a rebuild reports clean while the executor is skipping real
changes.

The rule lived in `verification.py`, so the write path imported the verify path to
reach it, and only the verify path could be handed a different one:
`diff_effects_against_rows` takes `normalizers=`, `DjangoExecutor` took
`skip_unchanged` and `using`. There was no way to construct an executor that
agreed with a diff run under a custom set — both docstrings said so, and neither
could fix it from where it sat.

These cases pin the fix from the outside: the rule is reachable without importing
the verify path, and both sides can be given the same set.
"""

from __future__ import annotations

import datetime as _dt
import uuid
from decimal import Decimal

import pytest
from django.utils import timezone

from django_rakaia.canonicalisation import DEFAULT_NORMALIZERS, canonical_value
from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.verification import diff_effects_against_rows
from rakaia.effects import Upsert

from .models import Measure

REF = uuid.UUID("11111111-2222-3333-4444-555555555555")

pytestmark = pytest.mark.django_db


def _loud(_model: type, _field_name: str, value: object) -> object:
    """A normalizer nothing else would apply: every number collapses to zero.

    Deliberately absurd, so that "1.00 equals 2.50" can only be true if this
    function actually reached the compare. A plausible normalizer risks agreeing
    with the default set by accident, which would let a test pass without the
    custom set ever being consulted.
    """
    return 0 if isinstance(value, (int, float, Decimal)) else value


class TestTheRuleIsReachableWithoutTheVerifyPath:
    def test_the_write_path_does_not_import_the_verify_path(self):
        # The layering claim, stated as an import fact. `effect_executor` needs
        # the equality rule, not the diff report, the reader, or the vacuity
        # checks that live alongside it.
        import ast
        import pathlib

        import django_rakaia.effect_executor as mod

        source = pathlib.Path(mod.__file__).read_text()
        imported: set[str] = set()
        for node in ast.walk(ast.parse(source)):
            if isinstance(node, ast.ImportFrom) and node.module:
                imported.add(node.module)
            elif isinstance(node, ast.Import):
                imported.update(a.name for a in node.names)

        offenders = {m for m in imported if "verification" in m}
        assert not offenders, (
            f"the write path imports the verify path ({sorted(offenders)}); the "
            f"value-equality rule both need belongs in a module neither owns"
        )

    def test_the_rule_has_its_own_module(self):
        from django_rakaia import canonicalisation

        assert canonicalisation.canonical_value is canonical_value
        assert canonicalisation.DEFAULT_NORMALIZERS == DEFAULT_NORMALIZERS

    def test_the_verify_path_uses_the_same_definition(self):
        from django_rakaia import canonicalisation, verification

        assert verification.canonical_value is canonicalisation.canonical_value
        assert verification.DEFAULT_NORMALIZERS is canonicalisation.DEFAULT_NORMALIZERS

    def test_both_documented_import_paths_still_work(self):
        # `django_rakaia.verification` is the path this module's own docstring
        # taught and the one the package root used to resolve. The names are Tier
        # 1; which module defines them is not — so moving the definition must not
        # move the import.
        import django_rakaia
        from django_rakaia.canonicalisation import canonical_value as from_new
        from django_rakaia.verification import canonical_value as from_old

        assert from_old is from_new is django_rakaia.canonical_value

    def test_reaching_the_rule_does_not_drag_in_the_verify_path(self):
        # The measurable payoff, and the reason the package root resolves this
        # name to `canonicalisation` rather than leaving it on `verification`'s
        # re-export: both spellings return the same object, so nothing else can
        # tell them apart, but resolving through `verification` also imports
        # `projection_reader` and the effect types for a caller that wanted one
        # pure function. `__init__` is lazy (PEP 562) precisely so importing the
        # package does not pull in the ORM; this keeps that true one level down.
        import subprocess
        import sys

        script = (
            "import os, sys\n"
            "os.environ.setdefault('DJANGO_SETTINGS_MODULE',"
            "'tests.test_django_rakaia.settings')\n"
            "import django; django.setup()\n"
            "import django_rakaia\n"
            "django_rakaia.canonical_value\n"
            "assert 'django_rakaia.canonicalisation' in sys.modules\n"
            "print('verification' if 'django_rakaia.verification' in sys.modules"
            " else 'clean')\n"
        )
        out = subprocess.run(
            [sys.executable, "-c", script], capture_output=True, text=True, check=True
        )
        assert out.stdout.strip() == "clean", out.stdout


class TestBothSidesCanBeGivenTheSameNormalizers:
    """The divergence #160 names, closed.

    This is the one assertion in the epic that described an interface that did
    not exist — the executor had no way to accept a normalizer set, so the
    disagreement could not be demonstrated, only read about in two docstrings.
    """

    def test_the_executor_accepts_a_normalizer_set(self):
        executor = DjangoExecutor(skip_unchanged=True, normalizers=(_loud,))
        assert executor is not None

    def test_a_custom_set_changes_what_the_executor_calls_unchanged(self):
        # Under `_loud` every number is equal to every other number, so a real
        # change from 1.00 to 2.50 reads as no change and the write is skipped.
        # Absurd on purpose: it can only pass if the custom set reached the
        # compare.
        Measure.objects.create(ref=REF, amount=Decimal("1.00"))
        executor = DjangoExecutor(skip_unchanged=True, normalizers=(_loud,))

        report = executor.apply(
            [
                Upsert(
                    model_label="test_django_rakaia.Measure",
                    lookup={"ref": str(REF)},
                    defaults={"amount": 2.5},
                )
            ]
        )

        assert report is not None
        assert report.upserts_skipped == 1
        assert report.upserts_written == 0
        assert Measure.objects.get(ref=REF).amount == Decimal("1.00")

    def test_the_default_set_still_sees_that_change(self):
        # The contrast that gives the previous case its meaning.
        Measure.objects.create(ref=REF, amount=Decimal("1.00"))
        executor = DjangoExecutor(skip_unchanged=True)

        report = executor.apply(
            [
                Upsert(
                    model_label="test_django_rakaia.Measure",
                    lookup={"ref": str(REF)},
                    defaults={"amount": 2.5},
                )
            ]
        )

        assert report is not None
        assert report.upserts_written == 1
        assert report.upserts_skipped == 0
        assert Measure.objects.get(ref=REF).amount == Decimal("2.50")

    def test_the_two_paths_agree_under_one_custom_set(self):
        # The property the divergence broke: hand the same set to both, and
        # "unchanged" means the same thing on each. Under `_loud` the diff finds
        # no difference and the executor skips the write — one answer, two paths.
        Measure.objects.create(ref=REF, amount=Decimal("1.00"))
        effects = [
            Upsert(
                model_label="test_django_rakaia.Measure",
                lookup={"ref": str(REF)},
                defaults={"amount": 2.5},
            )
        ]

        diff = diff_effects_against_rows(effects, normalizers=[_loud])
        report = DjangoExecutor(skip_unchanged=True, normalizers=(_loud,)).apply(
            effects
        )

        # `certified`, not `ok`: `ok` is vacuously true for an empty population,
        # and a diff that compared nothing would otherwise read as agreement.
        assert diff.certified
        assert diff.compared == 1
        assert report is not None
        assert report.upserts_skipped == 1

    def test_they_diverged_before_and_the_test_can_tell(self):
        # With the executor on the default set and the diff on a custom one, the
        # two disagree: the diff reports no difference while the executor writes.
        # This is the state that used to be the *only* one available.
        Measure.objects.create(ref=REF, amount=Decimal("1.00"))
        effects = [
            Upsert(
                model_label="test_django_rakaia.Measure",
                lookup={"ref": str(REF)},
                defaults={"amount": 2.5},
            )
        ]

        diff = diff_effects_against_rows(effects, normalizers=[_loud])
        report = DjangoExecutor(skip_unchanged=True).apply(effects)

        assert diff.certified
        assert diff.compared == 1
        assert report is not None
        assert report.upserts_written == 1


class TestTheDefaultsAreUnchanged:
    """The rule itself did not move semantically — only where it lives.

    Spot-checks of the three normalizers through the new module, so a refactor
    that dropped one from `DEFAULT_NORMALIZERS` fails here rather than in a
    consumer's rebuild months later.
    """

    def test_the_default_set_is_the_three_normalizers(self):
        from django_rakaia.canonicalisation import (
            normalize_decimal,
            normalize_temporal,
            normalize_uuid,
        )

        assert (
            normalize_uuid,
            normalize_decimal,
            normalize_temporal,
        ) == DEFAULT_NORMALIZERS

    def test_a_json_float_equals_the_columns_decimal(self):
        assert canonical_value(Measure, "amount", 2.1) == Decimal("2.10")

    def test_a_uuid_column_compares_as_its_string(self):
        u = uuid.uuid4()
        assert canonical_value(Measure, "ref", u) == str(u)

    def test_a_timestamp_is_compared_at_millisecond_precision(self):
        stamped = timezone.now().replace(microsecond=123456)
        assert canonical_value(Measure, "observed_at", stamped).microsecond == 123000

    def test_a_foreign_key_column_resolves_to_its_field(self):
        # Effects address the *column* (`area_id`), not the relation (`area`), and
        # both must resolve or an FK column would never be normalized.
        #
        # Note what this does **not** cover. `_resolve_field` has a fallback that
        # scans `attname` when `get_field` raises, and on Django 6.0
        # `get_field("area_id")` does not raise — it returns the ForeignKey
        # directly — so that loop has no trigger here and deleting it leaves the
        # suite green. It is kept because the declared floor is Django 4.2 and
        # this suite runs one version; it is not claimed as covered. See the
        # docstring in `canonicalisation._resolve_field`.
        from .models import Project

        assert canonical_value(Project, "area_id", 7) == 7

        from django_rakaia.canonicalisation import _resolve_field

        assert _resolve_field(Project, "area_id") is not None
        assert _resolve_field(Project, "area") is not None
        assert _resolve_field(Project, "nope") is None

    def test_an_unknown_field_is_returned_unchanged(self):
        sentinel = object()
        assert canonical_value(Measure, "not_a_field", sentinel) is sentinel

    def test_a_date_string_becomes_a_date(self):
        assert canonical_value(Measure, "observed_on", "2026-01-02") == _dt.date(
            2026, 1, 2
        )
