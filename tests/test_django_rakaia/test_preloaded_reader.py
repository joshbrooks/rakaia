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
from django.db import connection, connections
from django.test.utils import CaptureQueriesContext

from django_rakaia.projection_reader import DjangoProjectionReader
from django_rakaia.verification import (
    PreloadedProjectionReader,
    PreloadMismatch,
    diff_effects_against_rows,
)
from rakaia.effects import Delete, Effect, Update, Upsert

from .models import Alert, FinanceLine, Measure


def _finance_effect(submission_id: str, suku: str, delta: int) -> Effect:
    return Upsert(
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
            Upsert(
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
        effect = Upsert(
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

    def test_empty_lookup_is_not_preloaded_and_reads_live(self):
        # The other shape a bulk fetch cannot index: an empty lookup scopes the
        # whole model, so preloading it would cache one arbitrary row as *the*
        # answer (and scan the table to get it). It stays live, which is visible
        # as a row created after construction being the answer.
        reader = PreloadedProjectionReader(
            [
                Update(
                    model_label="test_django_rakaia.FinanceLine",
                    lookup={},
                    defaults={"suku": "A"},
                )
            ]
        )
        FinanceLine.objects.create(submission_id="s1", suku="A", delta=1)
        row = reader.get("test_django_rakaia.FinanceLine")
        assert row is not None and row.submission_id == "s1"

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


@pytest.mark.django_db
class TestPreloadOption:
    """``diff_effects_against_rows(effects, preload=True)`` — the fast path as one
    option on the diff (#190).

    The pattern this replaces was: build a ``PreloadedProjectionReader`` with your
    effects, then pass *the same* effects to the diff, with nothing enforcing the
    "same". Getting it wrong returned a report rather than an error — a report
    resting on a snapshot of a different batch. With the flag there is one list,
    so there is nothing to keep in step.
    """

    def test_preload_agrees_with_the_plain_path(self):
        for i in range(6):
            FinanceLine.objects.create(submission_id=f"s{i}", suku="A", delta=i)
        # One row deliberately wrong, so agreement covers a RED verdict too.
        FinanceLine.objects.filter(submission_id="s3").update(delta=99)
        effects = [_finance_effect(f"s{i}", "A", i) for i in range(6)]

        slow = diff_effects_against_rows(effects)
        fast = diff_effects_against_rows(effects, preload=True)

        assert fast.verdict == slow.verdict == "red"
        assert fast.compared == slow.compared == 6
        assert [str(p) for p in fast.problems] == [str(p) for p in slow.problems]

    def test_preload_uses_one_query_for_the_whole_sweep(self):
        for i in range(10):
            FinanceLine.objects.create(submission_id=f"s{i}", suku="A", delta=i)
        effects = [_finance_effect(f"s{i}", "A", i) for i in range(10)]

        with CaptureQueriesContext(connection) as fast:
            assert diff_effects_against_rows(effects, preload=True).ok
        with CaptureQueriesContext(connection) as slow:
            assert diff_effects_against_rows(effects).ok

        assert len(fast) == 1  # one `submission_id__in=[...]` for the batch
        assert len(slow) == 10  # the cost the flag exists to remove

    def test_preload_consumes_a_one_shot_iterable_once(self):
        """The drift the two-argument pattern allowed, at its sharpest.

        Handing a generator to the reader *and* to the diff left the diff an
        exhausted iterator: nothing compared, and only ``raise_if_diff``'s
        vacuity guard between that and a report that reads as a pass. One list
        means the flag cannot lose the batch.
        """
        for i in range(4):
            FinanceLine.objects.create(submission_id=f"s{i}", suku="A", delta=i)
        effects = (_finance_effect(f"s{i}", "A", i) for i in range(4))

        report = diff_effects_against_rows(effects, preload=True)

        assert report.compared == 4 and report.certified

    def test_reader_together_with_preload_is_refused(self):
        effects = [_finance_effect("s1", "A", 1)]
        with pytest.raises(TypeError, match="preload=/using="):
            diff_effects_against_rows(
                effects, reader=DjangoProjectionReader(), preload=True
            )

    def test_reader_together_with_using_is_refused(self):
        effects = [_finance_effect("s1", "A", 1)]
        with pytest.raises(TypeError, match="preload=/using="):
            diff_effects_against_rows(
                effects, reader=DjangoProjectionReader(), using="overlay"
            )

    def test_a_reader_built_from_another_batch_is_refused(self):
        """The misuse that used to answer instead of raising.

        The reader below was preloaded with one effect and the diff is given two,
        so half the report would come from the snapshot and half from live
        queries taken afterwards.
        """
        FinanceLine.objects.create(submission_id="s1", suku="A", delta=1)
        FinanceLine.objects.create(submission_id="s2", suku="A", delta=2)
        preloaded = [_finance_effect("s1", "A", 1)]
        diffed = [*preloaded, _finance_effect("s2", "A", 2)]

        reader = PreloadedProjectionReader(preloaded)
        with pytest.raises(PreloadMismatch, match="did not preload 1 of the 2"):
            diff_effects_against_rows(diffed, reader=reader)

        # The covering batch is still accepted, so the standalone reader remains
        # usable with the diff for a caller that genuinely needs it.
        assert diff_effects_against_rows(preloaded, reader=reader).certified

    def test_a_live_fallback_does_not_count_as_covered(self):
        """A memoised live answer is a reading taken at another moment, so it must
        not satisfy the coverage check — otherwise touching the reader first would
        launder a mismatched batch."""
        FinanceLine.objects.create(submission_id="s1", suku="A", delta=1)
        FinanceLine.objects.create(submission_id="s2", suku="A", delta=2)
        reader = PreloadedProjectionReader([_finance_effect("s1", "A", 1)])
        assert reader.get("test_django_rakaia.FinanceLine", submission_id="s2")

        with pytest.raises(PreloadMismatch):
            diff_effects_against_rows(
                [_finance_effect("s1", "A", 1), _finance_effect("s2", "A", 2)],
                reader=reader,
            )

    def test_a_spanning_lookup_is_not_a_mismatch(self):
        """A spanning lookup is documented as always live, so its absence from the
        snapshot says nothing about which batch built the reader."""
        FinanceLine.objects.create(submission_id="s1", suku="A", delta=5)
        spanning = Upsert(
            model_label="test_django_rakaia.FinanceLine",
            lookup={"delta__gte": 1},
            defaults={"suku": "A"},
        )
        effects = [_finance_effect("s1", "A", 5), spanning]
        reader = PreloadedProjectionReader(effects)
        assert diff_effects_against_rows(effects, reader=reader).certified

    def test_an_effect_the_diff_never_reads_is_not_required_to_be_covered(self):
        """Coverage is about the lookups this diff will make, not the batch.

        A delete carries no values to diff, so the diff never asks the reader for
        its row — and a reader built from only what *will* be read is correct.
        Blaming it for the delete would refuse a legitimate reader (the same holds
        for a narrower ``kinds=``).
        """
        FinanceLine.objects.create(submission_id="s1", suku="A", delta=1)
        read = _finance_effect("s1", "A", 1)
        never_read = Delete(
            model_label="test_django_rakaia.FinanceLine",
            lookup={"submission_id": "gone"},
        )
        reader = PreloadedProjectionReader([read])

        assert diff_effects_against_rows([read, never_read], reader=reader).certified

    def test_an_empty_lookup_is_not_a_mismatch(self):
        """The other always-live shape: an empty lookup scopes the whole model
        (`Update({})` — update every row), so the snapshot cannot index it and
        must not be blamed for not holding it."""
        FinanceLine.objects.create(submission_id="s1", suku="A", delta=5)
        whole_model = Update(
            model_label="test_django_rakaia.FinanceLine",
            lookup={},
            defaults={"suku": "A"},
        )
        effects = [_finance_effect("s1", "A", 5), whole_model]
        reader = PreloadedProjectionReader(effects)
        assert diff_effects_against_rows(effects, reader=reader).certified


@pytest.mark.django_db(databases=["default", "overlay"])
def test_using_routes_the_reader_the_diff_builds() -> None:
    """``using=`` is the other half of building the reader internally: without it
    the fast path on a non-default alias would still need a hand-built reader,
    which is the pattern #190 removes.

    A plain marker on both aliases, not `transaction=True`: the diff only reads,
    so there is no `atomic(using=)` for a lent transaction to supply and nothing
    here whose failure mode is a lock (#148)."""
    FinanceLine.objects.using("overlay").create(submission_id="o1", suku="A", delta=1)
    effects = [_finance_effect("o1", "A", 1)]

    # The row exists only on `overlay`, so the alias is load-bearing on both of
    # the readers the diff can build.
    assert diff_effects_against_rows(effects, using="overlay").certified
    assert diff_effects_against_rows(effects, preload=True, using="overlay").certified
    assert not diff_effects_against_rows(effects).certified

    with CaptureQueriesContext(connections["overlay"]) as ctx:
        assert diff_effects_against_rows(
            effects, preload=True, using="overlay"
        ).certified
    assert len(ctx) == 1
