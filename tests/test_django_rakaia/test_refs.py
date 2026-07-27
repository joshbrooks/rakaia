"""Symbolic refs through the live DjangoExecutor: a batch that creates a row and
binds a sibling to its DB-assigned pk, in one apply() (#68 item 3)."""

from __future__ import annotations

import pytest
from django.contrib.auth.models import User

from django_rakaia.effect_executor import DjangoExecutor
from rakaia.effects import Effect, Ref, UnresolvedRefError

from .models import Area, Project


@pytest.mark.django_db
class TestRefsThroughDjangoExecutor:
    def test_ref_binds_sibling_pk_as_fk(self):
        user = User.objects.create(username="u")
        effects = [
            Effect(
                op="update_or_create",
                model_label="test_django_rakaia.Area",
                lookup={"name": "Zone"},
                defaults={},
                produces="area",
            ),
            Effect(
                op="update_or_create",
                model_label="test_django_rakaia.Project",
                lookup={"name": "P1"},
                defaults={"area_id": Ref("area"), "created_by_id": user.id},
            ),
        ]
        DjangoExecutor().apply(effects)

        area = Area.objects.get(name="Zone")
        proj = Project.objects.get(name="P1")
        # The FK was wired to the Area's real, DB-assigned pk — no staging, no
        # natural-key reader lookup.
        assert proj.area_id == area.pk

    def test_ref_resolves_a_named_field(self):
        user = User.objects.create(username="u2")
        effects = [
            Effect(
                op="update_or_create",
                model_label="test_django_rakaia.Area",
                lookup={"name": "SourceName"},
                defaults={},
                produces="area",
            ),
            # Bind the *name* of the produced Area, not its pk.
            Effect(
                op="update_or_create",
                model_label="test_django_rakaia.Project",
                lookup={"name": Ref("area", "name")},
                defaults={"area_id": Ref("area"), "created_by_id": user.id},
            ),
        ]
        DjangoExecutor().apply(effects)
        assert Project.objects.filter(name="SourceName").exists()

    def test_forward_reference_raises(self):
        user = User.objects.create(username="u3")
        # Consumer appears BEFORE its producer — a forward ref is a hard error,
        # never a silent None.
        effects = [
            Effect(
                op="update_or_create",
                model_label="test_django_rakaia.Project",
                lookup={"name": "Early"},
                defaults={"area_id": Ref("area"), "created_by_id": user.id},
            ),
            Effect(
                op="update_or_create",
                model_label="test_django_rakaia.Area",
                lookup={"name": "Late"},
                defaults={},
                produces="area",
            ),
        ]
        with pytest.raises(UnresolvedRefError, match="area"):
            DjangoExecutor().apply(effects)

    def test_skip_unchanged_executor_also_records_produces(self):
        user = User.objects.create(username="u4")
        effects = [
            Effect(
                op="update_or_create",
                model_label="test_django_rakaia.Area",
                lookup={"name": "Zone2"},
                defaults={},
                produces="area",
            ),
            Effect(
                op="update_or_create",
                model_label="test_django_rakaia.Project",
                lookup={"name": "P2"},
                defaults={"area_id": Ref("area"), "created_by_id": user.id},
            ),
        ]
        DjangoExecutor(skip_unchanged=True).apply(effects)
        assert (
            Project.objects.get(name="P2").area_id == Area.objects.get(name="Zone2").pk
        )
