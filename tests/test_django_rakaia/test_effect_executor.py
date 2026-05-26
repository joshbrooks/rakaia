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
