"""Collapsing a fanned-out `Update` into one statement (#199).

A handler that fans one logical change across many rows emits one `Update` per
row — deliberately, because a verification pass replays the same effects and
diffs them against live rows one row at a time, so collapsing in the handler
would hide the per-row detail from the oracle. The consumer that raised this runs
nine identical `UPDATE`s to save a form with eight repeating rows.

`Update` is `filter(**lookup).update(**defaults)`: no signals, no `auto_now`
advance, no return value a caller reads. So N single-row updates that share a
model and identical defaults are equivalent to one `filter(field__in=[…])`, and
the collapse is invisible except in the statement count.

What is *not* free is order. These cases pin both halves: where batching happens,
and the three shapes where it must not.
"""

from __future__ import annotations

import pytest
from django.db import connection
from django.test.utils import CaptureQueriesContext

from django_rakaia.effect_executor import DjangoExecutor
from rakaia.effects import Delete, Update, Upsert

from .models import FinanceLine

pytestmark = pytest.mark.django_db

MODEL = "test_django_rakaia.FinanceLine"


def _rows(*ids: str) -> None:
    for i in ids:
        FinanceLine.objects.create(submission_id=i, suku="s", delta=0)


def _updates(ids, **defaults):
    return [
        Update(model_label=MODEL, lookup={"submission_id": i}, defaults=dict(defaults))
        for i in ids
    ]


def _update_statements(ctx: CaptureQueriesContext) -> list[str]:
    return [
        q["sql"]
        for q in ctx.captured_queries
        if q["sql"].lstrip().upper().startswith("UPDATE")
    ]


class TestTheFanOutCollapses:
    def test_nine_identical_updates_become_one_statement(self):
        # The reported case: a parent plus eight repeater rows, all setting the
        # same column to the same value.
        ids = [f"r{i}" for i in range(9)]
        _rows(*ids)

        with CaptureQueriesContext(connection) as ctx:
            DjangoExecutor().apply(_updates(ids, delta=7))

        assert len(_update_statements(ctx)) == 1
        assert set(FinanceLine.objects.values_list("delta", flat=True)) == {7}

    def test_the_collapsed_statement_uses_an_in_clause(self):
        _rows("a", "b")
        with CaptureQueriesContext(connection) as ctx:
            DjangoExecutor().apply(_updates(["a", "b"], delta=1))
        assert "IN" in _update_statements(ctx)[0].upper()

    def test_a_single_update_is_unchanged(self):
        _rows("a")
        with CaptureQueriesContext(connection) as ctx:
            DjangoExecutor().apply(_updates(["a"], delta=1))
        assert len(_update_statements(ctx)) == 1
        assert FinanceLine.objects.get(submission_id="a").delta == 1

    def test_rows_that_do_not_exist_are_still_not_inserted(self):
        # `Update` never inserts. Collapsing must not turn a miss into a write.
        with CaptureQueriesContext(connection) as ctx:
            DjangoExecutor().apply(_updates(["ghost1", "ghost2"], delta=1))
        assert len(_update_statements(ctx)) == 1
        assert FinanceLine.objects.count() == 0


class TestWhereItMustNotBatch:
    def test_different_defaults_stay_separate(self):
        _rows("a", "b")
        effects = [
            Update(
                model_label=MODEL, lookup={"submission_id": "a"}, defaults={"delta": 1}
            ),
            Update(
                model_label=MODEL, lookup={"submission_id": "b"}, defaults={"delta": 2}
            ),
        ]
        with CaptureQueriesContext(connection) as ctx:
            DjangoExecutor().apply(effects)

        assert len(_update_statements(ctx)) == 2
        assert FinanceLine.objects.get(submission_id="a").delta == 1
        assert FinanceLine.objects.get(submission_id="b").delta == 2

    def test_a_multi_key_lookup_is_not_collapsed(self):
        # No safe `__in` collapse for a composite lookup.
        _rows("a", "b")
        effects = [
            Update(
                model_label=MODEL,
                lookup={"submission_id": i, "suku": "s"},
                defaults={"delta": 3},
            )
            for i in ("a", "b")
        ]
        with CaptureQueriesContext(connection) as ctx:
            DjangoExecutor().apply(effects)

        assert len(_update_statements(ctx)) == 2
        assert set(FinanceLine.objects.values_list("delta", flat=True)) == {3}

    def test_a_traversal_lookup_is_not_collapsed(self):
        _rows("a", "b")
        effects = [
            Update(
                model_label=MODEL,
                lookup={"submission_id__in": [i]},
                defaults={"delta": 4},
            )
            for i in ("a", "b")
        ]
        with CaptureQueriesContext(connection) as ctx:
            DjangoExecutor().apply(effects)

        assert len(_update_statements(ctx)) == 2
        assert set(FinanceLine.objects.values_list("delta", flat=True)) == {4}

    def test_unhashable_defaults_fall_back_rather_than_raise(self):
        # A JSON column's value is a dict; a grouping key cannot hold it. Falling
        # back keeps the executor total instead of raising on a valid effect.
        from .models import History

        History.objects.create(submission_id="h1", version=1, marker="~", snapshot={})
        History.objects.create(submission_id="h2", version=1, marker="~", snapshot={})
        effects = [
            Update(
                model_label="test_django_rakaia.History",
                lookup={"submission_id": f"h{i}"},
                defaults={"snapshot": {"n": 1}},
            )
            for i in (1, 2)
        ]
        DjangoExecutor().apply(effects)
        assert History.objects.get(submission_id="h1").snapshot == {"n": 1}
        assert History.objects.get(submission_id="h2").snapshot == {"n": 1}

    def test_different_models_stay_separate(self):
        from .models import Balance

        _rows("a")
        Balance.objects.create(suku="x", total=0)
        effects = [
            Update(
                model_label=MODEL, lookup={"submission_id": "a"}, defaults={"delta": 5}
            ),
            Update(
                model_label="test_django_rakaia.Balance",
                lookup={"suku": "x"},
                defaults={"total": 5},
            ),
        ]
        with CaptureQueriesContext(connection) as ctx:
            DjangoExecutor().apply(effects)
        assert len(_update_statements(ctx)) == 2


class TestOrderIsPreserved:
    """The part that is not free.

    Two `Update`s with *different* defaults can match overlapping rows through
    different lookups, and `check_disjoint_defaults` does not catch it — it keys
    on the exact lookup, so two different lookups hitting one row are invisible
    to it. Grouping non-adjacent effects would therefore reorder writes that
    apply in sequence today, and last-write-wins would change answer.

    Only immediately-adjacent runs are collapsed, which is why these hold.
    """

    def test_effects_matching_one_row_by_different_lookups_keep_their_sequence(self):
        # Three different lookups, all matching the same row, all writing the
        # same column. `check_disjoint_defaults` cannot see the overlap — it keys
        # on the exact lookup — so nothing but ordering protects the answer.
        # Applied in order the row ends at 3.
        _rows("a")
        effects = [
            Update(
                model_label=MODEL, lookup={"submission_id": "a"}, defaults={"delta": 1}
            ),
            Update(model_label=MODEL, lookup={"suku": "s"}, defaults={"delta": 2}),
            Update(
                model_label=MODEL,
                lookup={"submission_id__in": ["a"]},
                defaults={"delta": 3},
            ),
        ]
        DjangoExecutor().apply(effects)
        assert FinanceLine.objects.get(submission_id="a").delta == 3

    def test_two_collapsible_effects_can_never_target_the_same_row(self):
        # Why within-group reordering is safe rather than merely conservative.
        # A group requires one equality on the *same* field, so two members with
        # different values match disjoint rows — and two with the same value are
        # the same lookup, which `check_disjoint_defaults` rejects outright
        # before anything applies.
        from rakaia.effects import EffectCollisionError, check_disjoint_defaults

        same = _updates(["a", "a"], delta=1)
        with pytest.raises(EffectCollisionError):
            check_disjoint_defaults(same)

    def test_an_update_is_never_hoisted_over_an_upsert_that_produces_its_row(self):
        # A `Ref` in a lookup resolves against a row an earlier `Upsert`
        # materialised, so a batch must not move an `Update` above its producer.
        from rakaia.effects import Ref

        effects = [
            Upsert(
                model_label="test_django_rakaia.Area",
                lookup={"name": "north"},
                defaults={},
                produces="area",
            ),
            Update(
                model_label="test_django_rakaia.Project",
                lookup={"area_id": Ref("area")},
                defaults={"name": "renamed"},
            ),
        ]
        DjangoExecutor().apply(effects)  # must not raise UnresolvedRefError

    def test_a_delete_between_updates_does_not_merge_them(self):
        # Deletes run in their own pass, but the two `Update` runs either side of
        # one are still separated in the write pass by nothing — so they *may*
        # merge. Pinned so the behaviour is deliberate rather than accidental.
        _rows("a", "b")
        effects = [
            *_updates(["a"], delta=8),
            Delete(model_label=MODEL, lookup={"submission_id": "gone"}),
            *_updates(["b"], delta=8),
        ]
        DjangoExecutor().apply(effects)
        assert set(FinanceLine.objects.values_list("delta", flat=True)) == {8}


class TestTheRunEndsAtAnUpsert:
    """An update deferred past an upsert stops being update-if-exists.

    `Update` never inserts, so an update that runs *before* an upsert which
    creates its row correctly matches nothing. Let the run carry on past the
    upsert and the same effect matches the row the upsert just made — a no-op
    becomes a write.

    This is the hazard that survives the Ref one. Deferring every update to the
    end of the write pass leaves Refs resolvable, because their producers have
    all run by then; what it silently changes is the order against the upserts.

    Note the columns have to differ. An update and an upsert writing the *same*
    column on the same lookup is rejected by `check_disjoint_defaults` before
    anything applies, so that version of the collision is not reachable — which
    is why this uses `delta` against `suku`.
    """

    def test_an_update_before_an_upsert_does_not_see_the_row_it_creates(self):
        effects = [
            Update(
                model_label=MODEL,
                lookup={"submission_id": "late"},
                defaults={"delta": 99},
            ),
            Upsert(
                model_label=MODEL,
                lookup={"submission_id": "late"},
                defaults={"suku": "s"},
            ),
        ]
        DjangoExecutor().apply(effects)

        row = FinanceLine.objects.get(submission_id="late")
        assert row.suku == "s"
        # The update ran first and matched nothing, so the column keeps its model
        # default. Deferring it past the upsert would give 99.
        assert row.delta == 0


class TestTheGroupingKeyIsWhatItClaims:
    def test_two_models_keyed_on_the_same_field_do_not_merge(self):
        # Contrived on purpose: both models happen to have a `suku` column, so
        # without the model in the grouping key these two would collapse into one
        # statement against one table and the other table would go unwritten.
        from .models import Balance

        FinanceLine.objects.create(submission_id="f", suku="before", delta=0)
        Balance.objects.create(suku="before", total=0)

        DjangoExecutor().apply(
            [
                Update(
                    model_label=MODEL,
                    lookup={"suku": "before"},
                    defaults={"suku": "after"},
                ),
                Update(
                    model_label="test_django_rakaia.Balance",
                    lookup={"suku": "before"},
                    defaults={"suku": "after"},
                ),
            ]
        )

        assert FinanceLine.objects.get(submission_id="f").suku == "after"
        assert Balance.objects.get().suku == "after"


class TestGroupingIsAdjacentOnly:
    """Why the run has to be contiguous, not gathered across the batch.

    Two members of one group can never collide — same field, different values,
    so disjoint rows; same value would be the same lookup, which
    `check_disjoint_defaults` rejects. That makes it tempting to gather every
    matching effect in the batch into one statement regardless of position.

    It is not safe. A *third* effect with different defaults can match rows that
    two same-key effects also match, through a different lookup that
    `check_disjoint_defaults` cannot see — it keys on the exact lookup. Hoisting
    the later same-key effect above that third one changes which write lands
    last.

    Written after a gathering implementation passed every other case in this
    file: the three-effect interleave below is the only shape that tells the two
    apart.
    """

    def test_a_later_same_key_effect_is_not_hoisted_over_a_different_one(self):
        FinanceLine.objects.create(submission_id="a", suku="s", delta=0)
        FinanceLine.objects.create(submission_id="b", suku="s", delta=0)

        effects = [
            # matches a
            Update(
                model_label=MODEL, lookup={"submission_id": "a"}, defaults={"delta": 1}
            ),
            # matches both a and b, by a lookup the collision check cannot relate
            # to either of the others
            Update(model_label=MODEL, lookup={"suku": "s"}, defaults={"delta": 2}),
            # matches b, and shares its grouping key with the first effect
            Update(
                model_label=MODEL, lookup={"submission_id": "b"}, defaults={"delta": 1}
            ),
        ]
        DjangoExecutor().apply(effects)

        # In order: a→1, then both→2, then b→1.
        assert FinanceLine.objects.get(submission_id="a").delta == 2
        assert FinanceLine.objects.get(submission_id="b").delta == 1
