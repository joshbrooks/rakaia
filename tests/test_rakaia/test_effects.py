"""Tests for rakaia.effects: Effect dataclass and collision detection."""

from __future__ import annotations

from dataclasses import FrozenInstanceError

import pytest

from rakaia.effects import (
    Effect,
    EffectCollisionError,
    check_disjoint_defaults,
    dispatch_external,
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


class TestDispatchExternal:
    def test_routes_by_kind(self):
        seen = []
        effects = [
            Effect(op="update_or_create", model_label="m.M", lookup={"id": 1}),
            Effect(op="external", kind="email", payload={"to": "a"}),
            Effect(op="external", kind="webhook", payload={"url": "u"}),
        ]
        n = dispatch_external(
            effects,
            {
                "email": lambda e: seen.append(("email", e.payload)),
                "webhook": lambda e: seen.append(("webhook", e.payload)),
            },
        )
        assert n == 2
        assert seen == [("email", {"to": "a"}), ("webhook", {"url": "u"})]

    def test_non_external_effects_skipped(self):
        n = dispatch_external(
            [Effect(op="delete", model_label="m.M", lookup={"p": 1})],
            {"email": lambda _e: None},
        )
        assert n == 0

    def test_unknown_kind_raises_by_default(self):
        with pytest.raises(KeyError, match="stripe"):
            dispatch_external(
                [Effect(op="external", kind="stripe", payload={})],
                {"email": lambda _e: None},
            )

    def test_unknown_kind_ignored_when_configured(self):
        n = dispatch_external(
            [Effect(op="external", kind="stripe", payload={})],
            {"email": lambda _e: None},
            on_unknown="ignore",
        )
        assert n == 0

    def test_kind_none_treated_as_unknown(self):
        # A kind=None external effect has no handler; pins the behavior so a
        # future refactor (e.g. handlers.get(eff.kind, default)) can't silently
        # change it. Raises by default, skipped under on_unknown="ignore".
        with pytest.raises(KeyError, match="kind=None"):
            dispatch_external(
                [Effect(op="external", kind=None, payload={})],
                {"email": lambda _e: None},
            )
        assert (
            dispatch_external(
                [Effect(op="external", kind=None, payload={})],
                {"email": lambda _e: None},
                on_unknown="ignore",
            )
            == 0
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
