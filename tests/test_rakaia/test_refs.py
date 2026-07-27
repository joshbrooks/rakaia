"""Tests for symbolic effect refs — `Ref` / `Effect.produces` / `RefResolver`.

A handler can emit two effects where the second binds to the pk of the row the
first materialises, without splitting across replay stages or a natural-key
reader lookup (#68 item 3). Resolution happens in the executor at apply time;
these cover the framework-agnostic core pieces.
"""

from __future__ import annotations

import pytest

from rakaia.effects import Effect, Ref, RefResolver, UnresolvedRefError


class TestRefAndProduces:
    def test_ref_defaults_to_pk(self):
        r = Ref("proj")
        assert r.produces == "proj"
        assert r.field == "pk"

    def test_ref_can_name_a_field(self):
        assert Ref("proj", "suku").field == "suku"

    def test_produces_valid_on_update_or_create(self):
        eff = Effect(
            op="update_or_create",
            model_label="app.Project",
            lookup={"suku": "A"},
            produces="proj",
        )
        assert eff.produces == "proj"

    def test_produces_rejected_on_other_ops(self):
        for op in ("update", "delete", "retire"):
            with pytest.raises(ValueError, match="produces"):
                Effect(
                    op=op,
                    model_label="app.Project",
                    lookup={"suku": "A"},
                    produces="proj",
                )


class TestRefResolver:
    def _accessor(self, row: dict):
        return lambda field: row["pk"] if field in ("pk", "id") else row[field]

    def test_resolves_ref_to_recorded_pk(self):
        resolver = RefResolver()
        resolver.record("proj", self._accessor({"pk": 42, "suku": "A"}))
        assert resolver.resolve_value(Ref("proj")) == 42
        assert resolver.resolve_value(Ref("proj", "suku")) == "A"

    def test_non_ref_value_passes_through(self):
        resolver = RefResolver()
        assert resolver.resolve_value("literal") == "literal"
        assert resolver.resolve_value(7) == 7

    def test_unknown_ref_raises(self):
        resolver = RefResolver()
        with pytest.raises(UnresolvedRefError, match="proj"):
            resolver.resolve_value(Ref("proj"))

    def test_resolve_effect_substitutes_in_defaults_and_lookup(self):
        resolver = RefResolver()
        resolver.record("proj", self._accessor({"pk": 7}))
        eff = Effect(
            op="update_or_create",
            model_label="app.Tf",
            lookup={"submission_id": "s1"},
            defaults={"project_id": Ref("proj"), "name": "keep"},
        )
        resolved = resolver.resolve_effect(eff)
        assert resolved.defaults == {"project_id": 7, "name": "keep"}
        assert resolved.lookup == {"submission_id": "s1"}

    def test_resolve_effect_is_identity_when_no_refs(self):
        # Fast path: an effect with no Ref values is returned unchanged (same
        # object), so the common no-ref batch pays nothing.
        resolver = RefResolver()
        eff = Effect(
            op="update_or_create",
            model_label="app.Tf",
            lookup={"submission_id": "s1"},
            defaults={"name": "x"},
        )
        assert resolver.resolve_effect(eff) is eff

    def test_resolve_effect_substitutes_in_spare_keys(self):
        resolver = RefResolver()
        resolver.record("proj", self._accessor({"pk": 3}))
        eff = Effect(
            op="retire",
            model_label="app.Alert",
            lookup={"stream_key": "k"},
            patch={"resolved_at": "t"},
            spare_keys=[{"project_id": Ref("proj")}],
        )
        resolved = resolver.resolve_effect(eff)
        assert resolved.spare_keys == [{"project_id": 3}]
