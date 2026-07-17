"""Tests for django_rakaia.effect_executor.DjangoExecutor."""

from __future__ import annotations

import pytest

from django_rakaia.effect_executor import DjangoExecutor
from rakaia.effects import Effect, EffectCollisionError

from .models import Area


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
