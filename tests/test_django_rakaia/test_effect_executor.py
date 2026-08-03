"""Tests for django_rakaia.effect_executor.DjangoExecutor."""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest
from django.contrib.auth.models import User
from django.db import connection
from django.test.utils import CaptureQueriesContext

from django_rakaia.effect_executor import DjangoExecutor
from rakaia.effects import Effect, EffectCollisionError

from .models import Area, History, Measure, Project


def _updates(ctx: CaptureQueriesContext) -> list[str]:
    # Count only UPDATEs to the projection tables under test. Writes to rakaia's
    # own `rakaia_*` tables (the stream append log and the #34 offset high-water)
    # are append-path infrastructure — emitted as a side effect of saving a
    # @stream_model projection — not the executor's projection write these
    # skip-unchanged tests measure. Excluding by the `rakaia_` db_table prefix
    # (the app models set it explicitly; the projection tables under test are
    # `test_django_rakaia_*`) keeps the count stable as the append path evolves,
    # rather than naming one infrastructure table.
    return [
        sql
        for q in ctx.captured_queries
        if (sql := q["sql"].lstrip()).upper().startswith("UPDATE")
        and not sql.lower().startswith('update "rakaia_')
    ]


@pytest.mark.django_db
class TestDjangoExecutor:
    def test_apply_creates_row(self):
        executor = DjangoExecutor()
        executor.apply(
            [
                Effect(
                    op="update_or_create",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "Region A"},
                    defaults={},
                )
            ]
        )
        assert Area.objects.filter(name="Region A").exists()

    def test_apply_is_idempotent(self):
        executor = DjangoExecutor()
        effect = Effect(
            op="update_or_create",
            model_label="test_django_rakaia.Area",
            lookup={"name": "Once"},
            defaults={},
        )
        executor.apply([effect])
        executor.apply([effect])
        assert Area.objects.filter(name="Once").count() == 1

    def test_apply_updates_existing_row(self):
        Area.objects.create(name="Old")
        existing_id = Area.objects.get(name="Old").id

        executor = DjangoExecutor()
        executor.apply(
            [
                Effect(
                    op="update_or_create",
                    model_label="test_django_rakaia.Area",
                    lookup={"id": existing_id},
                    defaults={"name": "New"},
                )
            ]
        )
        assert Area.objects.get(id=existing_id).name == "New"

    def test_apply_batch_of_effects(self):
        executor = DjangoExecutor()
        executor.apply(
            [
                Effect(
                    op="update_or_create",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "A1"},
                    defaults={},
                ),
                Effect(
                    op="update_or_create",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "A2"},
                    defaults={},
                ),
            ]
        )
        assert Area.objects.filter(name__in=["A1", "A2"]).count() == 2

    def test_apply_skips_external_effects(self):
        executor = DjangoExecutor()
        # External effects should pass through without raising or writing
        executor.apply(
            [
                Effect(op="external", kind="email", payload={"to": "x@y.z"}),
            ]
        )
        # No areas created (and no error)
        assert Area.objects.count() == 0

    def test_apply_raises_on_unknown_model(self):
        executor = DjangoExecutor()
        with pytest.raises(LookupError):
            executor.apply(
                [
                    Effect(
                        op="update_or_create",
                        model_label="nonexistent_app.Nothing",
                        lookup={"id": 1},
                        defaults={},
                    )
                ]
            )

    def test_apply_raises_collision(self):
        executor = DjangoExecutor()
        with pytest.raises(EffectCollisionError):
            executor.apply(
                [
                    Effect(
                        op="update_or_create",
                        model_label="test_django_rakaia.Area",
                        lookup={"name": "X"},
                        defaults={"name": "a"},
                    ),
                    Effect(
                        op="update_or_create",
                        model_label="test_django_rakaia.Area",
                        lookup={"name": "X"},
                        defaults={"name": "b"},
                    ),
                ]
            )

    def test_apply_empty_is_noop(self):
        executor = DjangoExecutor()
        executor.apply([])
        assert Area.objects.count() == 0


@pytest.mark.django_db
class TestDjangoExecutorDelete:
    def test_delete_removes_matching(self):
        Area.objects.create(name="gone")
        executor = DjangoExecutor()
        executor.apply(
            [
                Effect(
                    op="delete",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "gone"},
                )
            ]
        )
        assert not Area.objects.filter(name="gone").exists()

    def test_delete_with_exclude_keeps_set(self):
        # Reconcile-children shape: delete everything under a scope EXCEPT the
        # rows in the keep-set. Here the whole Area table is the "scope".
        for name in ("keep0", "keep1", "orphan2", "orphan3"):
            Area.objects.create(name=name)
        executor = DjangoExecutor()
        executor.apply(
            [
                Effect(
                    op="delete",
                    model_label="test_django_rakaia.Area",
                    lookup={},
                    exclude={"name__in": ["keep0", "keep1"]},
                )
            ]
        )
        assert set(Area.objects.values_list("name", flat=True)) == {"keep0", "keep1"}

    def test_delete_noop_when_no_match(self):
        Area.objects.create(name="stays")
        executor = DjangoExecutor()
        executor.apply(
            [
                Effect(
                    op="delete",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "absent"},
                )
            ]
        )
        assert Area.objects.filter(name="stays").exists()

    def test_upserts_applied_before_deletes(self):
        # A batch that both upserts "temp" and deletes it. If upserts run first
        # and deletes second, the row is created then removed -> 0 rows. This
        # pins the deterministic ordering (upserts before deletes).
        executor = DjangoExecutor()
        executor.apply(
            [
                Effect(
                    op="delete",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "temp"},
                ),
                Effect(
                    op="update_or_create",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "temp"},
                    defaults={},
                ),
            ]
        )
        assert not Area.objects.filter(name="temp").exists()

    def test_delete_and_upsert_share_transaction(self):
        # A valid delete followed by an effect that raises must roll back the
        # delete too — the whole batch is one atomic transaction.
        Area.objects.create(name="keep")
        executor = DjangoExecutor()
        with pytest.raises(LookupError):
            executor.apply(
                [
                    Effect(
                        op="delete",
                        model_label="test_django_rakaia.Area",
                        lookup={"name": "keep"},
                    ),
                    Effect(
                        op="update_or_create",
                        model_label="nonexistent_app.Nothing",
                        lookup={"id": 1},
                        defaults={},
                    ),
                ]
            )
        assert Area.objects.filter(name="keep").exists()


@pytest.mark.django_db
class TestSkipUnchanged:
    """The opt-in `skip_unchanged` executor avoids no-op writes on replay."""

    def _area_upsert(self, area_id: int, name: str) -> Effect:
        return Effect(
            op="update_or_create",
            model_label="test_django_rakaia.Area",
            lookup={"id": area_id},
            defaults={"name": name},
        )

    def test_unchanged_upsert_issues_no_update(self):
        area = Area.objects.create(name="X")
        executor = DjangoExecutor(skip_unchanged=True)
        with CaptureQueriesContext(connection) as ctx:
            executor.apply([self._area_upsert(area.id, "X")])
        assert _updates(ctx) == []
        assert Area.objects.get(id=area.id).name == "X"

    def test_changed_upsert_issues_one_update(self):
        area = Area.objects.create(name="X")
        executor = DjangoExecutor(skip_unchanged=True)
        with CaptureQueriesContext(connection) as ctx:
            executor.apply([self._area_upsert(area.id, "Y")])
        assert len(_updates(ctx)) == 1
        assert Area.objects.get(id=area.id).name == "Y"

    def test_creates_missing_row(self):
        executor = DjangoExecutor(skip_unchanged=True)
        executor.apply(
            [
                Effect(
                    op="update_or_create",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "New"},
                    defaults={},
                )
            ]
        )
        assert Area.objects.filter(name="New").count() == 1

    def test_second_apply_is_a_noop(self):
        area = Area.objects.create(name="X")
        executor = DjangoExecutor(skip_unchanged=True)
        executor.apply([self._area_upsert(area.id, "Y")])  # first: writes
        with CaptureQueriesContext(connection) as ctx:
            executor.apply([self._area_upsert(area.id, "Y")])  # now unchanged
        assert _updates(ctx) == []

    def test_only_changed_column_is_written(self):
        user = User.objects.create(username="u")
        area = Area.objects.create(name="A")
        project = Project.objects.create(name="old", area=area, created_by=user)
        executor = DjangoExecutor(skip_unchanged=True)
        with CaptureQueriesContext(connection) as ctx:
            executor.apply(
                [
                    Effect(
                        op="update_or_create",
                        model_label="test_django_rakaia.Project",
                        lookup={"id": project.id},
                        defaults={"name": "new"},  # only the name changes
                    )
                ]
            )
        updates = _updates(ctx)
        assert len(updates) == 1
        sql = updates[0].lower()
        # update_fields limits the UPDATE to the changed column
        assert "name" in sql
        assert "area_id" not in sql and "created_by_id" not in sql

    def test_default_executor_writes_even_when_unchanged(self):
        # Backward compatibility: the default path uses update_or_create, which
        # always issues an UPDATE — the skip is strictly opt-in.
        area = Area.objects.create(name="X")
        executor = DjangoExecutor()
        with CaptureQueriesContext(connection) as ctx:
            executor.apply([self._area_upsert(area.id, "X")])
        assert len(_updates(ctx)) == 1


@pytest.mark.django_db
class TestSkipUnchangedCoercion:
    """P4 — `skip_unchanged` compares through the field's canonical form, not raw
    `!=`, so a value the column would round or re-type is not a spurious change.
    Without this, replaying a log that carries a JSON float for a DecimalField (or
    a string for a UUIDField) rewrites the row on *every* pass, defeating the
    optimisation. Uses `canonical_value` with the default normalizers — the set
    `diff_effects_against_rows` uses by default — so under those, "unchanged" is
    defined identically in the migration diff and on the write path. (A diff run
    with a custom `normalizers=` set has no executor counterpart.)
    """

    def _measure_upsert(self, ref, amount) -> Effect:
        return Effect(
            op="update_or_create",
            model_label="test_django_rakaia.Measure",
            lookup={"ref": ref},
            defaults={"amount": amount},
        )

    def test_json_float_equal_to_stored_decimal_issues_no_update(self):
        ref = uuid4()
        Measure.objects.create(ref=ref, amount=Decimal("2.10"))
        executor = DjangoExecutor(skip_unchanged=True)
        with CaptureQueriesContext(connection) as ctx:
            # 2.1 is how the JSON log decodes it — equal to the column's 2.10.
            executor.apply([self._measure_upsert(str(ref), 2.1)])
        assert _updates(ctx) == []

    def test_over_precise_decimal_equal_after_rounding_issues_no_update(self):
        ref = uuid4()
        Measure.objects.create(ref=ref, amount=Decimal("2.10"))
        executor = DjangoExecutor(skip_unchanged=True)
        with CaptureQueriesContext(connection) as ctx:
            # More scale than the column stores; rounds to the same 2.10.
            executor.apply([self._measure_upsert(str(ref), Decimal("2.104"))])
        assert _updates(ctx) == []

    def test_uuid_string_equal_to_stored_uuid_issues_no_update(self):
        ref = uuid4()
        Measure.objects.create(ref=ref, amount=Decimal("2.10"))
        executor = DjangoExecutor(skip_unchanged=True)
        with CaptureQueriesContext(connection) as ctx:
            executor.apply(
                [
                    Effect(
                        op="update_or_create",
                        model_label="test_django_rakaia.Measure",
                        lookup={"ref": str(ref)},
                        defaults={"ref": str(ref), "amount": Decimal("2.10")},
                    )
                ]
            )
        assert _updates(ctx) == []

    def test_genuinely_different_decimal_still_writes(self):
        ref = uuid4()
        Measure.objects.create(ref=ref, amount=Decimal("2.10"))
        executor = DjangoExecutor(skip_unchanged=True)
        with CaptureQueriesContext(connection) as ctx:
            executor.apply([self._measure_upsert(str(ref), 3.5)])
        assert len(_updates(ctx)) == 1
        assert Measure.objects.get(ref=ref).amount == Decimal("3.50")

    def test_raw_comparison_would_have_churned(self):
        # Guardrail: the raw `!=` the fix replaced treats float 2.1 and
        # Decimal("2.10") as different — this documents the drift we closed.
        assert Decimal("2.10") != 2.1


@pytest.mark.django_db
class TestDjangoExecutorUpdate:
    """op="update" is update-if-exists: it modifies matching rows in place and
    never inserts, so a secondary owner of a multi-owned projection row can emit
    it unconditionally without minting an empty row."""

    def test_update_modifies_existing_row(self):
        area = Area.objects.create(name="Old")
        executor = DjangoExecutor()
        executor.apply(
            [
                Effect(
                    op="update",
                    model_label="test_django_rakaia.Area",
                    lookup={"id": area.id},
                    defaults={"name": "New"},
                )
            ]
        )
        assert Area.objects.get(id=area.id).name == "New"

    def test_update_noop_when_absent(self):
        # The core update-if-exists guarantee: a lookup that matches nothing
        # writes nothing and — crucially — inserts nothing.
        executor = DjangoExecutor()
        executor.apply(
            [
                Effect(
                    op="update",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "ghost"},
                    defaults={"name": "ghost"},
                )
            ]
        )
        assert Area.objects.count() == 0

    def test_update_empty_defaults_is_noop(self):
        # No defaults (or an empty dict) matches a row but writes nothing and
        # must not error.
        area = Area.objects.create(name="stays")
        executor = DjangoExecutor()
        executor.apply(
            [
                Effect(
                    op="update",
                    model_label="test_django_rakaia.Area",
                    lookup={"id": area.id},
                )
            ]
        )
        assert Area.objects.get(id=area.id).name == "stays"

    def test_update_clears_column_to_none(self):
        # The verified->draft "clear an existing row" path: contributors vanished
        # but the row (owned by others) stays; the owner nulls its columns.
        History.objects.create(
            submission_id="s", version=1, marker="c", actor=7, ts=1.0
        )
        executor = DjangoExecutor()
        executor.apply(
            [
                Effect(
                    op="update",
                    model_label="test_django_rakaia.History",
                    lookup={"submission_id": "s", "version": 1},
                    defaults={"actor": None},
                )
            ]
        )
        assert History.objects.get(submission_id="s", version=1).actor is None

    def test_multi_owner_disjoint_updates_land(self):
        # Two independent owners each write a disjoint column to the same row in
        # one batch; both writes must land.
        History.objects.create(submission_id="s", version=1, marker="c")
        executor = DjangoExecutor()
        executor.apply(
            [
                Effect(
                    op="update",
                    model_label="test_django_rakaia.History",
                    lookup={"submission_id": "s", "version": 1},
                    defaults={"actor": 42},
                ),
                Effect(
                    op="update",
                    model_label="test_django_rakaia.History",
                    lookup={"submission_id": "s", "version": 1},
                    defaults={"ts": 9.5},
                ),
            ]
        )
        row = History.objects.get(submission_id="s", version=1)
        assert row.actor == 42
        assert row.ts == 9.5

    def test_updates_applied_before_deletes(self):
        # Parity with the upsert-before-delete ordering: a batch that updates a
        # pre-existing "temp" row and deletes it converges to 0 rows regardless
        # of the effects' order (writes run before deletes).
        Area.objects.create(name="temp")
        executor = DjangoExecutor()
        executor.apply(
            [
                Effect(
                    op="delete",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "temp"},
                ),
                Effect(
                    op="update",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "temp"},
                    defaults={"name": "temp"},
                ),
            ]
        )
        assert not Area.objects.filter(name="temp").exists()
