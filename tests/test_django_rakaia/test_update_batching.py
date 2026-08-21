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

That last claim — invisible except in the statement count — is *asserted* here and
*measured* in `test_update_batching_equivalence.py`, which runs the same effects
down both paths and compares every column. Read that file for which of these
exclusions turn out to be load-bearing; a case here proving the collapse declined
does not, on its own, say it needed to.
"""

from __future__ import annotations

import pytest
from django.db import connection
from django.db.models import F
from django.test.utils import CaptureQueriesContext

from django_rakaia.effect_executor import DjangoExecutor
from rakaia.effects import Delete, Update, Upsert

from .models import FinanceLine

pytestmark = pytest.mark.django_db

MODEL = "test_django_rakaia.FinanceLine"


def _batching() -> DjangoExecutor:
    """An executor with the collapse enabled.

    Off by default (#199, and four review rounds' worth of reasons), so every case
    here has to ask for it. Which is itself the point: a consumer that has not
    asked keeps the one-statement-per-effect path unchanged.
    """
    return DjangoExecutor(batch_updates=True)


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
            _batching().apply(_updates(ids, delta=7))

        assert len(_update_statements(ctx)) == 1
        assert set(FinanceLine.objects.values_list("delta", flat=True)) == {7}

    def test_the_collapsed_statement_uses_an_in_clause(self):
        _rows("a", "b")
        with CaptureQueriesContext(connection) as ctx:
            _batching().apply(_updates(["a", "b"], delta=1))
        assert " IN (" in _update_statements(ctx)[0].upper()

    def test_a_single_update_is_unchanged(self):
        _rows("a")
        with CaptureQueriesContext(connection) as ctx:
            _batching().apply(_updates(["a"], delta=1))
        assert len(_update_statements(ctx)) == 1
        assert FinanceLine.objects.get(submission_id="a").delta == 1

    def test_rows_that_do_not_exist_are_still_not_inserted(self):
        # `Update` never inserts. Collapsing must not turn a miss into a write.
        with CaptureQueriesContext(connection) as ctx:
            _batching().apply(_updates(["ghost1", "ghost2"], delta=1))
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
            _batching().apply(effects)

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
            _batching().apply(effects)

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
            _batching().apply(effects)

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
        _batching().apply(effects)
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
            _batching().apply(effects)
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
        _batching().apply(effects)
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
        _batching().apply(effects)  # must not raise UnresolvedRefError

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
        _batching().apply(effects)
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
        _batching().apply(effects)

        row = FinanceLine.objects.get(submission_id="late")
        assert row.suku == "s"
        # The update ran first and matched nothing, so the column keeps its model
        # default. Deferring it past the upsert would give 99.
        assert row.delta == 0


class TestTheGroupingKeyIsWhatItClaims:
    def test_two_models_keyed_on_the_same_field_do_not_merge(self):
        # Two models that genuinely share both the lookup column and the written
        # column, so the pair is collapsible except for the model. Without the
        # model in the key they merge into one statement against one table and
        # the other goes unwritten.
        #
        # An earlier version used `suku` for both the lookup and the default,
        # which the self-rewrite guard added later excludes — so the test stopped
        # exercising the model at all and a mutation dropping it from the key
        # passed. Columns the guard does not reject, this time.
        from .models import ArchivedDoc, SoftDeleteDoc

        SoftDeleteDoc.objects.create(name="before", is_active=True)
        ArchivedDoc.objects.create(name="before", is_active=True)

        _batching().apply(
            [
                Update(
                    model_label="test_django_rakaia.SoftDeleteDoc",
                    lookup={"is_active": True},
                    defaults={"name": "after"},
                ),
                Update(
                    model_label="test_django_rakaia.ArchivedDoc",
                    lookup={"is_active": True},
                    defaults={"name": "after"},
                ),
            ]
        )

        assert SoftDeleteDoc.objects.get().name == "after"
        assert ArchivedDoc.objects.get().name == "after"


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
        _batching().apply(effects)

        # In order: a→1, then both→2, then b→1.
        assert FinanceLine.objects.get(submission_id="a").delta == 2
        assert FinanceLine.objects.get(submission_id="b").delta == 1


class TestNullIsNotAValueYouCanGather:
    """`filter(f=None)` is `f IS NULL`; `filter(f__in=[None, x])` is not.

    Django strips `None` from an `__in` right-hand side, so gathering a
    null-matching effect with a value-matching one emits ``WHERE f IN ('x')`` —
    the null effect vanishes with no error and no rows written. Nothing upstream
    catches it: the two lookups differ, so `check_disjoint_defaults` has nothing
    to say.

    Not an exotic shape. ``resolved_at IS NULL`` is this codebase's own
    open-versus-closed predicate — `reconcile_by_key` builds its soft-delete
    guard out of exactly that — so a reconcile touching the open rows and one
    closed row is the fan-out this batching exists for.
    """

    def test_a_null_lookup_is_not_gathered_with_a_value(self):
        from .models import Alert

        alert_model = "test_django_rakaia.Alert"
        Alert.objects.create(stream_key="open", alert_type="t", resolved_at=None)
        Alert.objects.create(stream_key="closed", alert_type="t", resolved_at="2020")

        _batching().apply(
            [
                Update(
                    model_label=alert_model,
                    lookup={"resolved_at": None},
                    defaults={"severity": "hit"},
                ),
                Update(
                    model_label=alert_model,
                    lookup={"resolved_at": "2020"},
                    defaults={"severity": "hit"},
                ),
            ]
        )

        assert Alert.objects.get(stream_key="open").severity == "hit"
        assert Alert.objects.get(stream_key="closed").severity == "hit"

    def test_two_null_lookups_cannot_arise(self):
        # The other null shape is unreachable: two effects with the *same* lookup
        # writing the same field are rejected by `check_disjoint_defaults` before
        # the executor sees them, so there is no two-null group to guard against.
        from rakaia.effects import EffectCollisionError, check_disjoint_defaults

        both_null = [
            Update(
                model_label="test_django_rakaia.Alert",
                lookup={"resolved_at": None},
                defaults={"severity": "one"},
            )
        ] * 2
        with pytest.raises(EffectCollisionError):
            check_disjoint_defaults(both_null)


class TestOnlyIdempotentDefaultsAreGathered:
    """The property the collapse needs is that re-applying a write is a no-op.

    Two grouped effects match one field with different values, so their rows look
    disjoint — but a member can move a row into a later member's value, and the
    later statement then matches it too. With literal defaults that second write
    sets the same columns to the same values and nothing changes. With an
    expression it does: `F("n") + 1` twice is not `F("n") + 1` once.

    Guarding the *lookup key* against appearing in `defaults` closed one route
    into this and left three open — a foreign key reachable as both `area` and
    `area_id`; a `Ref` and a literal resolving to one value, where
    `check_disjoint_defaults` runs pre-resolution and sees two lookups while the
    grouper sees one; and values distinct in Python but equal in the database
    (`1` versus `"1"` on an integer column). Every one of them needs a
    non-idempotent write to become visible, so requiring idempotent defaults
    closes all of them together.
    """

    def test_an_expression_default_is_not_gathered(self):
        from django.db.models import F

        FinanceLine.objects.create(submission_id="x", suku="a", delta=0)
        FinanceLine.objects.create(submission_id="y", suku="b", delta=0)

        _batching().apply(
            [
                Update(
                    model_label=MODEL,
                    lookup={"suku": "a"},
                    defaults={"suku": "b", "delta": F("delta") + 1},
                ),
                Update(
                    model_label=MODEL,
                    lookup={"suku": "b"},
                    defaults={"suku": "b", "delta": F("delta") + 1},
                ),
            ]
        )

        # x is moved into suku=b by the first statement, then matched again by the
        # second. Gathered into one `IN ('a', 'b')` it is incremented only once.
        assert FinanceLine.objects.get(submission_id="x").delta == 2
        assert FinanceLine.objects.get(submission_id="y").delta == 1

    def test_a_literal_default_that_rewrites_the_lookup_field_is_safe(self):
        # The narrower guard's other half: a literal *is* collapsed even when it
        # rewrites the matched field, because re-applying it changes nothing.
        # Both rows end identical either way, which is what makes the collapse
        # invisible rather than merely rare.
        FinanceLine.objects.create(submission_id="x", suku="a", delta=0)
        FinanceLine.objects.create(submission_id="y", suku="b", delta=0)

        with CaptureQueriesContext(connection) as ctx:
            _batching().apply(
                [
                    Update(
                        model_label=MODEL, lookup={"suku": "a"}, defaults={"suku": "c"}
                    ),
                    Update(
                        model_label=MODEL, lookup={"suku": "b"}, defaults={"suku": "c"}
                    ),
                ]
            )

        assert len(_update_statements(ctx)) == 1
        assert set(FinanceLine.objects.values_list("suku", flat=True)) == {"c"}

    def test_a_foreign_key_under_its_other_name_is_not_gathered(self):
        # `area` and `area_id` are one column, so a lookup key check could not see
        # this route. An expression default makes the divergence observable.
        from django.contrib.auth import get_user_model
        from django.db.models import Value
        from django.db.models.functions import Concat

        from .models import Area, Project

        user = get_user_model().objects.create_user(username="pj")
        a1 = Area.objects.create(name="a1")
        a2 = Area.objects.create(name="a2")
        Project.objects.create(name="p1", area=a1, created_by=user)
        Project.objects.create(name="p2", area=a2, created_by=user)

        _batching().apply(
            [
                Update(
                    model_label="test_django_rakaia.Project",
                    lookup={"area_id": a1.pk},
                    defaults={"area": a2, "name": Concat(F("name"), Value("!"))},
                ),
                Update(
                    model_label="test_django_rakaia.Project",
                    lookup={"area_id": a2.pk},
                    defaults={"area": a2, "name": Concat(F("name"), Value("!"))},
                ),
            ]
        )

        # p1 is moved onto a2 by the first statement and matched again by the
        # second. Gathered, it would be suffixed once.
        assert sorted(Project.objects.values_list("name", flat=True)) == ["p1!!", "p2!"]

    def test_values_equal_in_the_database_but_not_in_python_are_not_gathered(self):
        # `1` and `"1"` are two lookups to `check_disjoint_defaults` and one row
        # to the database, so nothing upstream relates them.
        from django.db.models import Value
        from django.db.models.functions import Concat

        from .models import Alert

        alert_model = "test_django_rakaia.Alert"
        Alert.objects.create(
            stream_key="m", alert_type="t", dismissed_version=1, message="m"
        )

        _batching().apply(
            [
                Update(
                    model_label=alert_model,
                    lookup={"dismissed_version": 1},
                    defaults={"message": Concat(F("message"), Value("!"))},
                ),
                Update(
                    model_label=alert_model,
                    lookup={"dismissed_version": "1"},
                    defaults={"message": Concat(F("message"), Value("!"))},
                ),
            ]
        )
        assert Alert.objects.get(stream_key="m").message == "m!!"


class TestASingleUpdateUsesPlainEquality:
    def test_one_effect_does_not_become_an_in_clause(self):
        # Equivalent for a non-null value, but the `IN` form is what breaks on
        # `None`, so the one-effect path stays on plain equality rather than
        # relying on the null guard alone.
        #
        # Matched as ` IN (`, not `"IN"`: the table is named `financeline`, which
        # contains the substring, and the first version of this assertion caught
        # the table name rather than the clause.
        _rows("solo")
        with CaptureQueriesContext(connection) as ctx:
            _batching().apply(_updates(["solo"], delta=1))

        statement = _update_statements(ctx)[0]
        assert " IN (" not in statement.upper(), statement


class TestTheLookupFieldIsPartOfTheKey:
    def test_two_lookup_fields_on_one_model_do_not_group(self):
        # Dropping `field` from the grouping key makes these group, and then
        # gathering reads `eff.lookup[field]` off an effect that has no such key.
        # A `KeyError` is loud rather than silent, but nothing pinned it.
        _rows("a")
        _batching().apply(
            [
                Update(
                    model_label=MODEL,
                    lookup={"submission_id": "a"},
                    defaults={"delta": 6},
                ),
                Update(model_label=MODEL, lookup={"suku": "s"}, defaults={"delta": 6}),
            ]
        )
        assert FinanceLine.objects.get(submission_id="a").delta == 6


class TestEqualInPythonIsNotEqualInTheDatabase:
    """Grouping compares `defaults` by value; the database writes them by type.

    Python calls `1`, `1.0`, `True` and `Decimal("1")` equal and hashes them
    alike. Written to a text or JSON column they are different rows. So keying on
    value alone let two effects group and then applied one member's `defaults` to
    the other's rows — wrong data, with *literal* defaults and no row overlap at
    all, so none of the idempotence reasoning applies.

    The mirror image of the route the guard's docstring already described:
    distinct in Python, equal in the database. Both directions matter, and only
    one of them had been considered.
    """

    def test_an_int_and_a_float_are_not_the_same_write(self):
        FinanceLine.objects.create(submission_id="a", suku="", delta=0)
        FinanceLine.objects.create(submission_id="b", suku="", delta=0)

        _batching().apply(
            [
                Update(
                    model_label=MODEL,
                    lookup={"submission_id": "a"},
                    defaults={"suku": 1},
                ),
                Update(
                    model_label=MODEL,
                    lookup={"submission_id": "b"},
                    defaults={"suku": 1.0},
                ),
            ]
        )

        assert sorted(FinanceLine.objects.values_list("submission_id", "suku")) == [
            ("a", "1"),
            ("b", "1.0"),
        ]

    def test_a_bool_and_an_int_are_not_the_same_write(self):
        FinanceLine.objects.create(submission_id="a", suku="", delta=0)
        FinanceLine.objects.create(submission_id="b", suku="", delta=0)

        _batching().apply(
            [
                Update(
                    model_label=MODEL,
                    lookup={"submission_id": "a"},
                    defaults={"suku": True},
                ),
                Update(
                    model_label=MODEL,
                    lookup={"submission_id": "b"},
                    defaults={"suku": 1},
                ),
            ]
        )

        stored = dict(FinanceLine.objects.values_list("submission_id", "suku"))
        assert stored["a"] != stored["b"], stored

    def test_identical_literals_still_collapse(self):
        # The guard must not cost the case #199 asked for.
        _rows("a", "b", "c")
        with CaptureQueriesContext(connection) as ctx:
            _batching().apply(_updates(["a", "b", "c"], delta=7))

        assert len(_update_statements(ctx)) == 1
        assert set(FinanceLine.objects.values_list("delta", flat=True)) == {7}


class TestQIsNotCombinableButIsStillUnsafe:
    """`Q` is hashable, accepted by `.update()`, non-idempotent — and not
    `Combinable`, so a predicate written against that base class let it through.

    Which is the argument for the allowlist. Enumerating unsafe values needs the
    enumeration to be complete; enumerating safe ones needs it only to be
    correct, and a mistake costs a missed collapse rather than a wrong write.
    """

    def test_a_q_default_is_not_gathered(self):
        from django.db.models import Q

        from .models import SoftDeleteDoc

        SoftDeleteDoc.objects.create(name="a", is_active=False)
        SoftDeleteDoc.objects.create(name="b", is_active=False)

        _batching().apply(
            [
                Update(
                    model_label="test_django_rakaia.SoftDeleteDoc",
                    lookup={"name": name},
                    defaults={"name": "b", "is_active": Q(name="a")},
                )
                for name in ("a", "b")
            ]
        )

        # Applied one at a time, the first moves row "a" to name="b" and the
        # second then matches both. Collapsed, the `Q` is evaluated once against
        # the pre-move state and the two rows disagree.
        assert set(SoftDeleteDoc.objects.values_list("is_active", flat=True)) == {False}


class TestRefsAreResolvedBeforeGrouping:
    def test_a_ref_and_a_literal_for_one_row_still_collapse(self):
        # The docstring claims refs are resolved before grouping; computing the
        # key first passed every other test, because nothing exercised a `Ref` in
        # a grouped lookup. Getting it wrong costs a missed collapse rather than a
        # wrong write, which is why it is worth a test rather than a guard.
        from django.contrib.auth import get_user_model

        from rakaia.effects import Ref

        from .models import Area, Project

        user = get_user_model().objects.create_user(username="rf")
        area = Area.objects.create(name="north")
        Project.objects.create(name="p", area=area, created_by=user)

        with CaptureQueriesContext(connection) as ctx:
            _batching().apply(
                [
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
                    Update(
                        model_label="test_django_rakaia.Project",
                        lookup={"area_id": area.pk},
                        defaults={"name": "renamed"},
                    ),
                ]
            )

        # One statement: the Ref resolved to the same pk as the literal, so the
        # two updates are alike by the time grouping sees them.
        assert len(_update_statements(ctx)) == 1
        assert Project.objects.get().name == "renamed"


class TestASubclassIsNotItsBaseType:
    """Membership is tested with `type(v) in`, not `isinstance`.

    `bool` being a subclass of `int` is handled by putting `type(v)` in the
    grouping key, so that is not what the stricter check is for. It is for a
    subclass of an allowlisted type that overrides equality: `isinstance` would
    admit it, and grouping would then trust an `__eq__` written by someone else
    to decide that two different writes are the same write.

    Remote, and cheap to exclude. The allowlist exists so that being wrong costs
    a missed collapse, and admitting arbitrary subclasses gives that away.
    """

    def test_a_str_subclass_that_lies_about_equality_is_not_gathered(self):
        class Sneaky(str):
            def __eq__(self, other):  # noqa: D105
                return True  # "I am every string"

            def __hash__(self):  # noqa: D105
                return 0

        FinanceLine.objects.create(submission_id="a", suku="", delta=0)
        FinanceLine.objects.create(submission_id="b", suku="", delta=0)

        _batching().apply(
            [
                Update(
                    model_label=MODEL,
                    lookup={"submission_id": "a"},
                    defaults={"suku": Sneaky("first")},
                ),
                Update(
                    model_label=MODEL,
                    lookup={"submission_id": "b"},
                    defaults={"suku": Sneaky("second")},
                ),
            ]
        )

        assert sorted(FinanceLine.objects.values_list("submission_id", "suku")) == [
            ("a", "first"),
            ("b", "second"),
        ]


class TestEqualityThatIgnoresRepresentation:
    """Four types whose `__eq__` hides exactly what the database stores.

    The grouping key compares `defaults` with `==` and `hash`, then writes one
    member's values to every member's rows. For `float`, `Decimal` and aware
    `datetime`/`time`, Python equality is *semantic*: it deliberately treats
    values as the same when their representations differ, and the representation
    is what gets written.

    No row overlap is involved, so none of the idempotence reasoning applies —
    these are two different writes that the key called one write. All four are
    excluded from `_COLLAPSIBLE_DEFAULTS` for exactly this reason, and each case
    below is the counterexample that put its type on the excluded list.
    """

    def _pair(self, va, vb) -> dict[str, str]:
        FinanceLine.objects.create(submission_id="a", suku="", delta=0)
        FinanceLine.objects.create(submission_id="b", suku="", delta=0)
        _batching().apply(
            [
                Update(
                    model_label=MODEL,
                    lookup={"submission_id": "a"},
                    defaults={"suku": va},
                ),
                Update(
                    model_label=MODEL,
                    lookup={"submission_id": "b"},
                    defaults={"suku": vb},
                ),
            ]
        )
        return dict(FinanceLine.objects.values_list("submission_id", "suku"))

    def test_negative_zero_is_not_zero_once_stored(self):
        assert -0.0 == 0.0 and hash(-0.0) == hash(0.0)
        stored = self._pair(-0.0, 0.0)
        assert stored == {"a": "-0.0", "b": "0.0"}

    def test_decimal_trailing_zeros_survive(self):
        from decimal import Decimal

        assert Decimal("1.0") == Decimal("1.00")
        stored = self._pair(Decimal("1.0"), Decimal("1.00"))
        assert stored == {"a": "1.0", "b": "1.00"}

    def test_one_instant_in_two_time_zones_stores_two_ways(self):
        import datetime as dt

        a = dt.datetime(2020, 1, 1, 12, tzinfo=dt.timezone.utc)
        b = dt.datetime(2020, 1, 1, 13, tzinfo=dt.timezone(dt.timedelta(hours=1)))
        assert a == b and hash(a) == hash(b)
        stored = self._pair(a, b)
        assert stored["a"] != stored["b"], stored

    def test_the_same_for_a_bare_time(self):
        import datetime as dt

        a = dt.time(12, 0, tzinfo=dt.timezone.utc)
        b = dt.time(13, 0, tzinfo=dt.timezone(dt.timedelta(hours=1)))
        assert a == b and hash(a) == hash(b)
        stored = self._pair(a, b)
        assert stored["a"] != stored["b"], stored

    @pytest.mark.parametrize(
        ("column", "value"),
        [
            ("resolved_by", "s"),
            ("resolved_by", b"s"),
            ("resolved_by", None),
            ("dismissed_version", 3),
            ("dismissed_version", True),
        ],
    )
    def test_the_admitted_types_still_collapse(self, column, value):
        # The narrowing must not cost the case #199 asked for. `str`, `bytes`,
        # `int`, `bool` and `None` are admitted because equality implies identical
        # storage for each — the property the grouping key actually relies on.
        # Written against nullable columns so `None` is a legal value rather than
        # an integrity error.
        from .models import Alert

        alert_model = "test_django_rakaia.Alert"
        for key in ("a", "b"):
            Alert.objects.create(stream_key=key, alert_type="t")

        with CaptureQueriesContext(connection) as ctx:
            _batching().apply(
                [
                    Update(
                        model_label=alert_model,
                        lookup={"stream_key": key},
                        defaults={column: value},
                    )
                    for key in ("a", "b")
                ]
            )

        assert len(_update_statements(ctx)) == 1, (column, value)


class TestTheCollapseIsOptIn:
    """Default off. The flag is the deliverable as much as the batching is.

    The collapse is semantics-preserving under the conditions `_batch_key`
    checks, and those conditions took four attempts — each earlier version
    looking closed at the time and each failure writing wrong data rather than
    raising. Applying it to every `apply()` in every consumer, with no opt-out and
    no way to bisect a write anomaly back to it, is not a trade worth one
    consumer's nine statements becoming one. So they ask.
    """

    def test_the_default_executor_still_issues_one_statement_per_effect(self):
        ids = [f"r{i}" for i in range(9)]
        _rows(*ids)

        with CaptureQueriesContext(connection) as ctx:
            DjangoExecutor().apply(_updates(ids, delta=7))

        assert len(_update_statements(ctx)) == 9
        assert set(FinanceLine.objects.values_list("delta", flat=True)) == {7}

    def test_the_two_paths_agree(self):
        # The property that matters more than either count: opting in must not
        # change the answer.
        ids = [f"r{i}" for i in range(5)]

        _rows(*ids)
        DjangoExecutor().apply(_updates(ids, delta=4))
        per_effect = sorted(FinanceLine.objects.values_list("submission_id", "delta"))

        FinanceLine.objects.all().delete()
        _rows(*ids)
        _batching().apply(_updates(ids, delta=4))
        collapsed = sorted(FinanceLine.objects.values_list("submission_id", "delta"))

        assert per_effect == collapsed


class TestALargeFanOutIsChunked:
    """An `IN` list is one bind parameter per value, and SQLite caps those.

    So the fan-out big enough to be worth collapsing was big enough to raise
    `too many SQL variables` where the per-effect loop had worked — the collapse
    failing exactly where it was meant to help. Chunked, the statement count stays
    proportional to the batch rather than to the rows.
    """

    def test_a_fan_out_past_the_bind_limit_still_applies(self):
        # Above SQLite's 32766 cap. Slow-ish but this is the boundary that broke.
        ids = [f"r{i}" for i in range(33_000)]
        FinanceLine.objects.bulk_create(
            FinanceLine(submission_id=i, suku="s", delta=0) for i in ids
        )

        with CaptureQueriesContext(connection) as ctx:
            _batching().apply(_updates(ids, delta=5))

        statements = _update_statements(ctx)
        # Correct on every backend, and never one statement per row.
        assert FinanceLine.objects.filter(delta=5).count() == 33_000
        assert len(statements) < 10, len(statements)
        # Chunked only where the backend declares a cap. Postgres binds
        # client-side and declares none, so 33,000 values are one statement there
        # and that is right — asserting a chunk unconditionally made this fail on
        # the Postgres leg for being correct.
        declared = getattr(connection.features, "max_query_params", None)
        if declared is not None and declared < 33_000:
            assert len(statements) > 1, declared


class TestAnOutOfRangeIntegerIsNotBound:
    """`field__in=[v]` is not `field=v`, one last time.

    Django compiles an out-of-range integer `exact` lookup away — the effect is a
    correct no-op. Inside an `IN` the same value goes to the driver, where SQLite
    raises and takes the rest of the batch with it, including siblings that would
    have written. Such a run is applied per effect instead.
    """

    def test_an_out_of_range_value_does_not_abort_its_siblings(self):
        _rows("real")
        effects = [
            Update(model_label=MODEL, lookup={"delta": 2**70}, defaults={"suku": "z"}),
            Update(model_label=MODEL, lookup={"delta": 0}, defaults={"suku": "z"}),
        ]

        _batching().apply(effects)

        # The out-of-range lookup matches nothing, as it did before; the sibling
        # writes.
        assert FinanceLine.objects.get(submission_id="real").suku == "z"
