"""Tests for rakaia.effects: Effect dataclass and collision detection."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from rakaia.effects import (
    Effect,
    EffectCollisionError,
    check_disjoint_defaults,
)


class TestEffect:
    def test_update_or_create_effect_fields(self):
        eff = Effect(
            op="update_or_create",
            model_label="myapp.Room",
            lookup={"id": 5},
            defaults={"name": "general"},
        )
        assert eff.op == "update_or_create"
        assert eff.model_label == "myapp.Room"
        assert eff.lookup == {"id": 5}
        assert eff.defaults == {"name": "general"}

    def test_update_effect_fields(self):
        # update-if-exists: same shape as update_or_create (model_label + lookup
        # + defaults) but never inserts. The row-sparing/retire fields stay None.
        eff = Effect(
            op="update",
            model_label="myapp.ProjectProjection",
            lookup={"project_id": 5},
            defaults={"ksp_operational": 300},
        )
        assert eff.op == "update"
        assert eff.model_label == "myapp.ProjectProjection"
        assert eff.lookup == {"project_id": 5}
        assert eff.defaults == {"ksp_operational": 300}
        assert eff.exclude is None
        assert eff.spare_keys is None
        assert eff.patch is None
        assert eff.transition_kind is None

    def test_external_effect_fields(self):
        eff = Effect(
            op="external",
            kind="email",
            payload={"to": "x@y.z", "subject": "hi"},
        )
        assert eff.op == "external"
        assert eff.kind == "email"
        assert eff.payload == {"to": "x@y.z", "subject": "hi"}

    def test_delete_effect_fields(self):
        eff = Effect(
            op="delete",
            model_label="myapp.Child",
            lookup={"parent_id": 7},
            exclude={"idx__in": [0, 1]},
        )
        assert eff.op == "delete"
        assert eff.model_label == "myapp.Child"
        assert eff.lookup == {"parent_id": 7}
        assert eff.exclude == {"idx__in": [0, 1]}

    def test_delete_effect_exclude_defaults_to_none(self):
        eff = Effect(op="delete", model_label="myapp.Child", lookup={"parent_id": 7})
        assert eff.exclude is None

    def test_retire_effect_fields(self):
        eff = Effect(
            op="retire",
            model_label="myapp.Alert",
            lookup={"stream_key": "s", "resolved_at__isnull": True},
            spare_keys=[{"alert_type": "ff4", "field_key": "a"}],
            patch={"resolved_at": "t1", "resolved_by": "system"},
        )
        assert eff.op == "retire"
        assert eff.spare_keys == [{"alert_type": "ff4", "field_key": "a"}]
        assert eff.patch == {"resolved_at": "t1", "resolved_by": "system"}

    def test_patch_and_spare_keys_default_to_none(self):
        eff = Effect(op="update_or_create", model_label="m.M", lookup={"id": 1})
        assert eff.patch is None
        assert eff.spare_keys is None

    def test_equality(self):
        a = Effect(
            op="update_or_create",
            model_label="m.M",
            lookup={"id": 1},
            defaults={"x": 1},
        )
        b = Effect(
            op="update_or_create",
            model_label="m.M",
            lookup={"id": 1},
            defaults={"x": 1},
        )
        assert a == b

    def test_frozen(self):
        eff = Effect(op="external", kind="email")
        with pytest.raises(FrozenInstanceError):
            eff.kind = "stripe"  # type: ignore[misc]


class TestEffectValidation:
    def test_exclude_and_spare_keys_together_rejected(self):
        # they are alternative row-sparing mechanisms; both would silently AND.
        with pytest.raises(ValueError, match="both `exclude` and `spare_keys`"):
            Effect(
                op="delete",
                model_label="m.M",
                lookup={"parent": 1},
                exclude={"idx__in": [0]},
                spare_keys=[{"k": "v"}],
            )

    def test_exclude_on_retire_rejected(self):
        # retire ignores exclude in the executor — fail loudly instead.
        with pytest.raises(ValueError, match="only applies to op='delete'"):
            Effect(
                op="retire",
                model_label="m.Alert",
                lookup={"s": 1},
                exclude={"idx__in": [0]},
                patch={"resolved_at": "t"},
            )

    def test_exclude_on_delete_ok(self):
        Effect(
            op="delete", model_label="m.M", lookup={"p": 1}, exclude={"idx__in": [0]}
        )

    def test_spare_keys_on_retire_ok(self):
        Effect(
            op="retire",
            model_label="m.Alert",
            lookup={"s": 1},
            spare_keys=[{"k": "v"}],
            patch={"resolved_at": "t"},
        )

    def test_transition_kind_on_retire_ok(self):
        eff = Effect(
            op="retire",
            model_label="m.Alert",
            lookup={"s": 1},
            patch={"resolved_at": "t"},
            transition_kind="alert_transition",
            transition_key_fields=("alert_type", "field_key"),
        )
        assert eff.transition_kind == "alert_transition"
        assert eff.transition_key_fields == ("alert_type", "field_key")

    def test_transition_kind_without_key_fields_rejected(self):
        # The pair identifies each flipped row; a half-set retire would degrade
        # the executor's deterministic identity SELECT — reject it.
        with pytest.raises(ValueError, match="set together"):
            Effect(
                op="retire",
                model_label="m.Alert",
                lookup={"s": 1},
                patch={"resolved_at": "t"},
                transition_kind="alert_transition",
            )

    def test_key_fields_without_transition_kind_rejected(self):
        with pytest.raises(ValueError, match="set together"):
            Effect(
                op="retire",
                model_label="m.Alert",
                lookup={"s": 1},
                patch={"resolved_at": "t"},
                transition_key_fields=("alert_type",),
            )

    def test_empty_transition_key_fields_rejected(self):
        with pytest.raises(ValueError, match="at least one column"):
            Effect(
                op="retire",
                model_label="m.Alert",
                lookup={"s": 1},
                patch={"resolved_at": "t"},
                transition_kind="alert_transition",
                transition_key_fields=(),
            )

    def test_transition_kind_on_non_retire_rejected(self):
        # transition_kind means "emit one external per row this retire flipped".
        # Only a retire flips rows silently; on any other op the flag is
        # meaningless and would be dropped — fail loudly instead.
        with pytest.raises(ValueError, match="only applies to op='retire'"):
            Effect(
                op="update_or_create",
                model_label="m.Alert",
                lookup={"s": 1},
                transition_kind="alert_transition",
            )

    def test_transition_kind_defaults_to_none(self):
        eff = Effect(
            op="retire", model_label="m.Alert", lookup={"s": 1}, patch={"r": 1}
        )
        assert eff.transition_kind is None

    def test_exclude_on_update_rejected(self):
        # `exclude` only applies to op='delete'; an update ignores it in the
        # executor, so the existing guard must reject it for the new op too.
        with pytest.raises(ValueError, match="only applies to op='delete'"):
            Effect(
                op="update",
                model_label="m.M",
                lookup={"id": 1},
                exclude={"idx__in": [0]},
            )

    def test_transition_kind_on_update_rejected(self):
        # Only a retire flips rows silently; the notification flags are
        # meaningless on an update and must be rejected by the existing guard.
        with pytest.raises(ValueError, match="only applies to op='retire'"):
            Effect(
                op="update",
                model_label="m.Alert",
                lookup={"s": 1},
                transition_kind="alert_transition",
                transition_key_fields=("alert_type",),
            )


class TestCheckDisjointDefaults:
    def test_no_effects(self):
        check_disjoint_defaults([])

    def test_single_effect(self):
        check_disjoint_defaults(
            [
                Effect(
                    op="update_or_create",
                    model_label="m.M",
                    lookup={"id": 1},
                    defaults={"x": 1},
                )
            ]
        )

    def test_disjoint_defaults_on_same_row_ok(self):
        check_disjoint_defaults(
            [
                Effect(
                    op="update_or_create",
                    model_label="m.M",
                    lookup={"id": 1},
                    defaults={"x": 1},
                ),
                Effect(
                    op="update_or_create",
                    model_label="m.M",
                    lookup={"id": 1},
                    defaults={"y": 2},
                ),
            ]
        )

    def test_overlapping_defaults_on_same_row_raises(self):
        with pytest.raises(EffectCollisionError, match="'x'"):
            check_disjoint_defaults(
                [
                    Effect(
                        op="update_or_create",
                        model_label="m.M",
                        lookup={"id": 1},
                        defaults={"x": 1},
                    ),
                    Effect(
                        op="update_or_create",
                        model_label="m.M",
                        lookup={"id": 1},
                        defaults={"x": 2},
                    ),
                ]
            )

    def test_disjoint_update_defaults_on_same_row_ok(self):
        # The multi-owner case: two owners each write a disjoint column to the
        # same row via op="update". This must NOT collide.
        check_disjoint_defaults(
            [
                Effect(
                    op="update",
                    model_label="m.M",
                    lookup={"id": 1},
                    defaults={"ksp_operational": 1},
                ),
                Effect(
                    op="update",
                    model_label="m.M",
                    lookup={"id": 1},
                    defaults={"effective_status": "verified"},
                ),
            ]
        )

    def test_overlapping_update_defaults_on_same_row_raises(self):
        # Two owners must never write the SAME column on the same row, even via
        # update — the collision check covers op="update" too.
        with pytest.raises(EffectCollisionError, match="'x'"):
            check_disjoint_defaults(
                [
                    Effect(
                        op="update",
                        model_label="m.M",
                        lookup={"id": 1},
                        defaults={"x": 1},
                    ),
                    Effect(
                        op="update",
                        model_label="m.M",
                        lookup={"id": 1},
                        defaults={"x": 2},
                    ),
                ]
            )

    def test_update_and_upsert_same_field_same_row_raises(self):
        # Cross-op collision: an update and an update_or_create writing the same
        # field on the same row still collide.
        with pytest.raises(EffectCollisionError, match="'x'"):
            check_disjoint_defaults(
                [
                    Effect(
                        op="update_or_create",
                        model_label="m.M",
                        lookup={"id": 1},
                        defaults={"x": 1},
                    ),
                    Effect(
                        op="update",
                        model_label="m.M",
                        lookup={"id": 1},
                        defaults={"x": 2},
                    ),
                ]
            )

    def test_same_field_different_row_is_ok(self):
        check_disjoint_defaults(
            [
                Effect(
                    op="update_or_create",
                    model_label="m.M",
                    lookup={"id": 1},
                    defaults={"x": 1},
                ),
                Effect(
                    op="update_or_create",
                    model_label="m.M",
                    lookup={"id": 2},
                    defaults={"x": 2},
                ),
            ]
        )

    def test_same_field_different_model_is_ok(self):
        check_disjoint_defaults(
            [
                Effect(
                    op="update_or_create",
                    model_label="m.A",
                    lookup={"id": 1},
                    defaults={"x": 1},
                ),
                Effect(
                    op="update_or_create",
                    model_label="m.B",
                    lookup={"id": 1},
                    defaults={"x": 2},
                ),
            ]
        )

    def test_lookup_ordering_does_not_matter(self):
        # Two effects targeting the same row, but the lookup dicts are written
        # in different key orders. Canonical JSON serialisation should treat
        # them as the same row.
        with pytest.raises(EffectCollisionError):
            check_disjoint_defaults(
                [
                    Effect(
                        op="update_or_create",
                        model_label="m.M",
                        lookup={"a": 1, "b": 2},
                        defaults={"x": 1},
                    ),
                    Effect(
                        op="update_or_create",
                        model_label="m.M",
                        lookup={"b": 2, "a": 1},
                        defaults={"x": 2},
                    ),
                ]
            )

    def test_external_effects_ignored(self):
        check_disjoint_defaults(
            [
                Effect(op="external", kind="email", payload={"to": "x"}),
                Effect(op="external", kind="email", payload={"to": "x"}),
            ]
        )

    def test_delete_effects_ignored(self):
        # Delete effects carry no `defaults`; they must never trip collision
        # detection, even when a sibling upsert targets the same row.
        check_disjoint_defaults(
            [
                Effect(op="delete", model_label="m.M", lookup={"parent": 1}),
                Effect(op="delete", model_label="m.M", lookup={"parent": 1}),
                Effect(
                    op="update_or_create",
                    model_label="m.M",
                    lookup={"parent": 1},
                    defaults={"x": 1},
                ),
            ]
        )

    def test_no_defaults_ignored(self):
        check_disjoint_defaults(
            [
                Effect(
                    op="update_or_create",
                    model_label="m.M",
                    lookup={"id": 1},
                ),
                Effect(
                    op="update_or_create",
                    model_label="m.M",
                    lookup={"id": 1},
                ),
            ]
        )

    def test_error_message_includes_field_and_row(self):
        with pytest.raises(EffectCollisionError) as exc:
            check_disjoint_defaults(
                [
                    Effect(
                        op="update_or_create",
                        model_label="myapp.Room",
                        lookup={"id": 42},
                        defaults={"name": "a"},
                    ),
                    Effect(
                        op="update_or_create",
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
