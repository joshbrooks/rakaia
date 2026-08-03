"""Tests for ``PreloadedProjectionReader`` — the batch-fetching reader that backs
a large ``diff_effects_against_rows`` sweep.

The friction it removes: the diff helper does one ``reader.get`` per effect, so a
20k-effect reconcile is ~20k round-trips. Given the same batch up front, this
reader bulk-fetches every lookup in one query per (model, lookup-shape) group and
serves each ``get`` from an in-memory snapshot. These tests pin the query count
(the whole point) and the snapshot/fallback semantics.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest
from django.db import connection
from django.test.utils import CaptureQueriesContext

from django_rakaia.projection_reader import DjangoProjectionReader
from django_rakaia.verification import (
    PreloadedProjectionReader,
    diff_effects_against_rows,
)
from rakaia.effects import Effect

from .models import Alert, FinanceLine, Measure


def _finance_effect(submission_id: str, suku: str, delta: int) -> Effect:
    return Effect(
        op="update_or_create",
        model_label="test_django_rakaia.FinanceLine",
        lookup={"submission_id": submission_id},
        defaults={"suku": suku, "delta": delta},
    )


@pytest.mark.django_db
class TestPreloadedProjectionReader:
    def test_single_field_batch_uses_one_query_for_the_whole_sweep(self):
        for i in range(10):
            FinanceLine.objects.create(submission_id=f"s{i}", suku="A", delta=i)
        effects = [_finance_effect(f"s{i}", "A", i) for i in range(10)]

        with CaptureQueriesContext(connection) as ctx:
            reader = PreloadedProjectionReader(effects)
            report = diff_effects_against_rows(effects, reader=reader)

        assert report.ok
        # One `submission_id__in=[...]` fetch for the whole batch — not one per row.
        assert len(ctx) == 1

    def test_plain_reader_issues_one_query_per_effect(self):
        # Contrast fixture: the un-preloaded path this reader replaces.
        for i in range(10):
            FinanceLine.objects.create(submission_id=f"s{i}", suku="A", delta=i)
        effects = [_finance_effect(f"s{i}", "A", i) for i in range(10)]

        with CaptureQueriesContext(connection) as ctx:
            diff_effects_against_rows(effects, reader=DjangoProjectionReader())

        assert len(ctx) == 10

    def test_composite_key_batch_uses_one_query(self):
        for i in range(5):
            Alert.objects.create(
                stream_key=f"sub{i}", alert_type="ff4", field_key="", message="m"
            )
        effects = [
            Effect(
                op="update_or_create",
                model_label="test_django_rakaia.Alert",
                lookup={"stream_key": f"sub{i}", "alert_type": "ff4", "field_key": ""},
                defaults={"message": "m"},
            )
            for i in range(5)
        ]

        with CaptureQueriesContext(connection) as ctx:
            reader = PreloadedProjectionReader(effects)
            report = diff_effects_against_rows(effects, reader=reader)

        assert report.ok
        # A single OR-of-tuples query covers the composite-key shape.
        assert len(ctx) == 1

    def test_uuid_string_lookup_keys_to_stored_uuid_row(self):
        # The lookup carries a UUID *string* (post-JSON), the column stores a
        # ``uuid.UUID``; the canonicalised key must land them in the same slot.
        ref = uuid4()
        Measure.objects.create(ref=ref, amount=Decimal("2.10"))
        effect = Effect(
            op="update_or_create",
            model_label="test_django_rakaia.Measure",
            lookup={"ref": str(ref)},
            defaults={"amount": Decimal("2.10")},
        )
        reader = PreloadedProjectionReader([effect])
        with CaptureQueriesContext(connection) as ctx:
            row = reader.get("test_django_rakaia.Measure", ref=str(ref))
        assert row is not None and row.ref == ref
        assert len(ctx) == 0  # served from the snapshot, no live query

    def test_absent_row_is_a_cached_miss_not_a_repeat_query(self):
        effect = _finance_effect("ghost", "A", 1)  # no such row
        with CaptureQueriesContext(connection) as ctx:
            reader = PreloadedProjectionReader([effect])
        assert len(ctx) == 1  # the preload fetch (returns nothing)

        with CaptureQueriesContext(connection) as ctx:
            assert (
                reader.get("test_django_rakaia.FinanceLine", submission_id="ghost")
                is None
            )
            assert (
                reader.get("test_django_rakaia.FinanceLine", submission_id="ghost")
                is None
            )
        assert len(ctx) == 0  # the miss is cached; neither get re-queries

    def test_lookup_outside_the_batch_falls_back_live_then_memoises(self):
        FinanceLine.objects.create(submission_id="in", suku="A", delta=1)
        FinanceLine.objects.create(submission_id="out", suku="B", delta=2)
        reader = PreloadedProjectionReader([_finance_effect("in", "A", 1)])

        with CaptureQueriesContext(connection) as ctx:
            first = reader.get("test_django_rakaia.FinanceLine", submission_id="out")
            second = reader.get("test_django_rakaia.FinanceLine", submission_id="out")
        assert first is not None and first.suku == "B"
        assert second is not None
        assert len(ctx) == 1  # one live fetch, then memoised

    def test_snapshot_is_point_in_time(self):
        # A row created *after* construction is not visible to a lookup that was
        # in the batch — the reader is a snapshot, for read-only verification.
        effect = _finance_effect("late", "A", 1)
        reader = PreloadedProjectionReader([effect])
        FinanceLine.objects.create(submission_id="late", suku="A", delta=1)
        assert (
            reader.get("test_django_rakaia.FinanceLine", submission_id="late") is None
        )

    def test_spanning_lookup_is_not_preloaded_and_reads_live(self):
        FinanceLine.objects.create(submission_id="s1", suku="A", delta=5)
        # A relation/transform lookup (`__`) can't be indexed by exact match, so
        # it's excluded from the preload and served live.
        reader = PreloadedProjectionReader([_finance_effect("s1", "A", 5)])
        with CaptureQueriesContext(connection) as ctx:
            row = reader.get("test_django_rakaia.FinanceLine", delta__gte=1)
        assert row is not None
        assert len(ctx) == 1

    def test_filter_and_query_remain_live(self):
        FinanceLine.objects.create(submission_id="s1", suku="A", delta=1)
        FinanceLine.objects.create(submission_id="s2", suku="A", delta=2)
        reader = PreloadedProjectionReader([_finance_effect("s1", "A", 1)])
        assert reader.filter("test_django_rakaia.FinanceLine", suku="A").count() == 2
        assert reader.query("test_django_rakaia.FinanceLine").count() == 2

    def test_duplicate_lookups_across_effects_fetch_once(self):
        FinanceLine.objects.create(submission_id="s1", suku="A", delta=1)
        # Same lookup repeated (e.g. two effects touching one row) de-dups.
        effects = [_finance_effect("s1", "A", 1), _finance_effect("s1", "A", 1)]
        with CaptureQueriesContext(connection) as ctx:
            reader = PreloadedProjectionReader(effects)
        assert len(ctx) == 1
        assert (
            reader.get("test_django_rakaia.FinanceLine", submission_id="s1") is not None
        )
