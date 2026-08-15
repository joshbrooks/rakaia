"""Tests for rakaia.effects: the effect variants and collision detection."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from rakaia.effects import (
    Delete,
    EffectCollisionError,
    Exclude,
    ExternalEffect,
    Retire,
    RowEffect,
    SpareKeys,
    Transition,
    Update,
    Upsert,
    check_disjoint_defaults,
)


class TestVariantFields:
    """Each variant carries **only** its own fields.

    This is what replaced `Effect.__post_init__`: `produces` on a delete or
    `patch` on an upsert is a type error, not a runtime ValueError, because the
    attribute does not exist. These assertions pin that, so a field creeping
    back onto the wrong variant fails here.
    """

    def test_upsert_fields(self):
        eff = Upsert(
            model_label="myapp.Room", lookup={"id": 5}, defaults={"name": "general"}
        )
        assert eff.model_label == "myapp.Room"
        assert eff.lookup == {"id": 5}
        assert eff.defaults == {"name": "general"}
        assert eff.produces is None
        for absent in ("spare", "patch", "transition", "kind", "payload"):
            assert not hasattr(eff, absent)

    def test_update_fields(self):
        # update-if-exists: the same row identity plus `defaults`, and nothing
        # else. It never mints a row, so it has no `produces`; it deletes no
        # row, so it has no `spare`.
        eff = Update(
            model_label="myapp.ProjectProjection",
            lookup={"project_id": 5},
            defaults={"ksp_operational": 300},
        )
        assert eff.model_label == "myapp.ProjectProjection"
        assert eff.lookup == {"project_id": 5}
        assert eff.defaults == {"ksp_operational": 300}
        for absent in ("produces", "spare", "patch", "transition"):
            assert not hasattr(eff, absent)

    def test_delete_fields(self):
        eff = Delete(
            model_label="myapp.Child",
            lookup={"parent_id": 7},
            spare=Exclude({"idx__in": [0, 1]}),
        )
        assert eff.model_label == "myapp.Child"
        assert eff.lookup == {"parent_id": 7}
        assert eff.spare == Exclude({"idx__in": [0, 1]})
        for absent in ("defaults", "produces", "patch", "transition"):
            assert not hasattr(eff, absent)

    def test_delete_spare_defaults_to_none(self):
        assert Delete(model_label="myapp.Child", lookup={"parent_id": 7}).spare is None

    def test_retire_fields(self):
        eff = Retire(
            model_label="myapp.Alert",
            lookup={"stream_key": "s", "resolved_at__isnull": True},
            patch={"resolved_at": "t1", "resolved_by": "system"},
            spare=SpareKeys([{"alert_type": "ff4", "field_key": "a"}]),
        )
        assert eff.spare == SpareKeys([{"alert_type": "ff4", "field_key": "a"}])
        assert eff.patch == {"resolved_at": "t1", "resolved_by": "system"}
        assert eff.transition is None
        for absent in ("defaults", "produces", "kind", "payload"):
            assert not hasattr(eff, absent)

    def test_retire_takes_a_transition(self):
        eff = Retire(
            model_label="m.Alert",
            lookup={"s": 1},
            patch={"resolved_at": "t"},
            transition=Transition(
                kind="alert_transition", key_fields=("alert_type", "field_key")
            ),
        )
        assert eff.transition is not None
        assert eff.transition.kind == "alert_transition"
        assert eff.transition.key_fields == ("alert_type", "field_key")

    def test_delete_spares_by_composite_key_too(self):
        # `spare` is one field with two shapes, so "both mechanisms at once" is
        # not a state the type can hold.
        eff = Delete(model_label="m.M", lookup={"p": 1}, spare=SpareKeys([{"k": "v"}]))
        assert eff.spare == SpareKeys([{"k": "v"}])

    def test_external_effect_is_not_a_row_effect(self):
        eff = ExternalEffect(kind="email", payload={"to": "x@y.z", "subject": "hi"})
        assert eff.kind == "email"
        assert eff.payload == {"to": "x@y.z", "subject": "hi"}
        assert not isinstance(eff, RowEffect)
        for absent in ("model_label", "lookup", "defaults", "spare", "patch"):
            assert not hasattr(eff, absent)

    def test_equality(self):
        a = Upsert(model_label="m.M", lookup={"id": 1}, defaults={"x": 1})
        b = Upsert(model_label="m.M", lookup={"id": 1}, defaults={"x": 1})
        assert a == b

    def test_variants_of_different_types_are_not_equal(self):
        assert Upsert(model_label="m.M", lookup={"id": 1}) != Update(
            model_label="m.M", lookup={"id": 1}
        )

    def test_frozen(self):
        eff = ExternalEffect(kind="email", payload={})
        with pytest.raises(FrozenInstanceError):
            eff.kind = "stripe"  # type: ignore[misc]


class TestTransition:
    """The one cross-field rule that survives — inside a two-field object."""

    def test_empty_key_fields_rejected(self):
        with pytest.raises(ValueError, match="at least one column"):
            Transition(kind="alert_transition", key_fields=())

    def test_key_fields_are_kept_in_order(self):
        t = Transition(kind="k", key_fields=("scope", "alert_type", "field_key"))
        assert t.key_fields == ("scope", "alert_type", "field_key")

    def test_frozen(self):
        t = Transition(kind="k", key_fields=("a",))
        with pytest.raises(FrozenInstanceError):
            t.kind = "other"  # type: ignore[misc]


class TestCheckDisjointDefaults:
    def test_no_effects(self):
        check_disjoint_defaults([])

    def test_single_effect(self):
        check_disjoint_defaults(
            [Upsert(model_label="m.M", lookup={"id": 1}, defaults={"x": 1})]
        )

    def test_disjoint_defaults_on_same_row_ok(self):
        check_disjoint_defaults(
            [
                Upsert(model_label="m.M", lookup={"id": 1}, defaults={"x": 1}),
                Upsert(model_label="m.M", lookup={"id": 1}, defaults={"y": 2}),
            ]
        )

    def test_overlapping_defaults_on_same_row_raises(self):
        with pytest.raises(EffectCollisionError, match="'x'"):
            check_disjoint_defaults(
                [
                    Upsert(model_label="m.M", lookup={"id": 1}, defaults={"x": 1}),
                    Upsert(model_label="m.M", lookup={"id": 1}, defaults={"x": 2}),
                ]
            )

    def test_disjoint_update_defaults_on_same_row_ok(self):
        # The multi-owner case: two owners each write a disjoint column to the
        # same row via an Update. This must NOT collide.
        check_disjoint_defaults(
            [
                Update(
                    model_label="m.M", lookup={"id": 1}, defaults={"ksp_operational": 1}
                ),
                Update(
                    model_label="m.M",
                    lookup={"id": 1},
                    defaults={"effective_status": "verified"},
                ),
            ]
        )

    def test_overlapping_update_defaults_on_same_row_raises(self):
        # Two owners must never write the SAME column on the same row, even via
        # update — the collision check covers Update too.
        with pytest.raises(EffectCollisionError, match="'x'"):
            check_disjoint_defaults(
                [
                    Update(model_label="m.M", lookup={"id": 1}, defaults={"x": 1}),
                    Update(model_label="m.M", lookup={"id": 1}, defaults={"x": 2}),
                ]
            )

    def test_update_and_upsert_same_field_same_row_raises(self):
        # Cross-variant collision: an Update and an Upsert writing the same
        # field on the same row still collide.
        with pytest.raises(EffectCollisionError, match="'x'"):
            check_disjoint_defaults(
                [
                    Upsert(model_label="m.M", lookup={"id": 1}, defaults={"x": 1}),
                    Update(model_label="m.M", lookup={"id": 1}, defaults={"x": 2}),
                ]
            )

    def test_same_field_different_row_is_ok(self):
        check_disjoint_defaults(
            [
                Upsert(model_label="m.M", lookup={"id": 1}, defaults={"x": 1}),
                Upsert(model_label="m.M", lookup={"id": 2}, defaults={"x": 2}),
            ]
        )

    def test_same_field_different_model_is_ok(self):
        check_disjoint_defaults(
            [
                Upsert(model_label="m.A", lookup={"id": 1}, defaults={"x": 1}),
                Upsert(model_label="m.B", lookup={"id": 1}, defaults={"x": 2}),
            ]
        )

    def test_lookup_ordering_does_not_matter(self):
        # Two effects targeting the same row, but the lookup dicts are written
        # in different key orders. Canonical JSON serialisation should treat
        # them as the same row.
        with pytest.raises(EffectCollisionError):
            check_disjoint_defaults(
                [
                    Upsert(
                        model_label="m.M", lookup={"a": 1, "b": 2}, defaults={"x": 1}
                    ),
                    Upsert(
                        model_label="m.M", lookup={"b": 2, "a": 1}, defaults={"x": 2}
                    ),
                ]
            )

    def test_external_effects_ignored(self):
        check_disjoint_defaults(
            [
                ExternalEffect(kind="email", payload={"to": "x"}),
                ExternalEffect(kind="email", payload={"to": "x"}),
            ]
        )

    def test_delete_effects_ignored(self):
        # Delete effects carry no `defaults`; they must never trip collision
        # detection, even when a sibling upsert targets the same row.
        check_disjoint_defaults(
            [
                Delete(model_label="m.M", lookup={"parent": 1}),
                Delete(model_label="m.M", lookup={"parent": 1}),
                Upsert(model_label="m.M", lookup={"parent": 1}, defaults={"x": 1}),
            ]
        )

    def test_no_defaults_ignored(self):
        check_disjoint_defaults(
            [
                Upsert(model_label="m.M", lookup={"id": 1}),
                Upsert(model_label="m.M", lookup={"id": 1}),
            ]
        )

    def test_error_message_includes_field_and_row(self):
        with pytest.raises(EffectCollisionError) as exc:
            check_disjoint_defaults(
                [
                    Upsert(
                        model_label="myapp.Room",
                        lookup={"id": 42},
                        defaults={"name": "a"},
                    ),
                    Upsert(
                        model_label="myapp.Room",
                        lookup={"id": 42},
                        defaults={"name": "b"},
                    ),
                ]
            )
        msg = str(exc.value)
        assert "name" in msg
        assert "myapp.Room" in msg
        assert "42" in msg
