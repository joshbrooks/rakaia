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
