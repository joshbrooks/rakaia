"""Tests for the upcaster registry and chain application."""

from __future__ import annotations

import json

import pytest

from rakaia.registry import (
    UPCASTERS_META_STREAM,
    UpcasterChainError,
    UpcasterConflictError,
    UpcasterRegistry,
    register_upcaster,
)
from rakaia.store import StreamStore


def _up_v1_to_v2(event: dict) -> dict:
    return {**event, "currency": "USD"}


def _up_v2_to_v3(event: dict) -> dict:
    # Rename currency -> currency_code
    out = {k: v for k, v in event.items() if k != "currency"}
    out["currency_code"] = event.get("currency", "USD")
    return out


def _up_other_v1_to_v2(event: dict) -> dict:
    return {**event, "added_by_chat_upcaster": True}


@pytest.fixture
def reg() -> UpcasterRegistry:
    return UpcasterRegistry()


class TestRegistration:
    def test_register_single(self, reg: UpcasterRegistry):
        v = reg.register("room:*", 1, _up_v1_to_v2)
        assert v.event_match == "room:*"
        assert v.from_version == 1
        assert v.source_hash
        assert v.dotted_path

    def test_register_chain(self, reg: UpcasterRegistry):
        reg.register("room:*", 1, _up_v1_to_v2)
        reg.register("room:*", 2, _up_v2_to_v3)
        assert len(reg.all_upcasters()) == 2

    def test_zero_from_version_raises(self, reg: UpcasterRegistry):
        with pytest.raises(ValueError, match=">= 1"):
            reg.register("e", 0, _up_v1_to_v2)

    def test_negative_from_version_raises(self, reg: UpcasterRegistry):
        with pytest.raises(ValueError, match=">= 1"):
            reg.register("e", -1, _up_v1_to_v2)

    def test_duplicate_different_fn_rejected(self, reg: UpcasterRegistry):
        reg.register("room:*", 1, _up_v1_to_v2)
        with pytest.raises(UpcasterConflictError):
            reg.register("room:*", 1, _up_other_v1_to_v2)

    def test_identical_re_registration_is_noop(self, reg: UpcasterRegistry):
        v1 = reg.register("room:*", 1, _up_v1_to_v2)
        v2 = reg.register("room:*", 1, _up_v1_to_v2)
        assert v1 is v2

    def test_different_patterns_share_from_version(self, reg: UpcasterRegistry):
        reg.register("room:*", 1, _up_v1_to_v2)
        reg.register("chat:*", 1, _up_other_v1_to_v2)
        assert len(reg.all_upcasters()) == 2


class TestCurrentVersion:
    def test_no_upcasters_returns_1(self, reg: UpcasterRegistry):
        assert reg.current_version("anything") == 1

    def test_one_upcaster_returns_2(self, reg: UpcasterRegistry):
        reg.register("room:*", 1, _up_v1_to_v2)
        assert reg.current_version("room:5") == 2

    def test_chain_returns_top(self, reg: UpcasterRegistry):
        reg.register("room:*", 1, _up_v1_to_v2)
        reg.register("room:*", 2, _up_v2_to_v3)
        assert reg.current_version("room:5") == 3

    def test_unrelated_pattern_returns_1(self, reg: UpcasterRegistry):
        reg.register("room:*", 1, _up_v1_to_v2)
        assert reg.current_version("chat:5") == 1


class TestApplyChain:
    def test_no_upcast_needed_returns_event_unchanged(self, reg: UpcasterRegistry):
        out = reg.apply_chain({"schema_version": 1, "x": 1}, "e", target_version=1)
        assert out == {"schema_version": 1, "x": 1}

    def test_single_step(self, reg: UpcasterRegistry):
        reg.register("room:*", 1, _up_v1_to_v2)
        out = reg.apply_chain(
            {"schema_version": 1, "name": "x"}, "room:5", target_version=2
        )
        assert out["schema_version"] == 2
        assert out["currency"] == "USD"
        assert out["name"] == "x"

    def test_multi_step_chain(self, reg: UpcasterRegistry):
        reg.register("room:*", 1, _up_v1_to_v2)
        reg.register("room:*", 2, _up_v2_to_v3)
        out = reg.apply_chain(
            {"schema_version": 1, "name": "x"}, "room:5", target_version=3
        )
        assert out["schema_version"] == 3
        assert out["currency_code"] == "USD"
        assert "currency" not in out
        assert out["name"] == "x"

    def test_default_schema_version_is_1(self, reg: UpcasterRegistry):
        reg.register("room:*", 1, _up_v1_to_v2)
        # Event has no schema_version field - treated as v1
        out = reg.apply_chain({"name": "x"}, "room:5", target_version=2)
        assert out["schema_version"] == 2
        assert out["currency"] == "USD"

    def test_missing_link_raises(self, reg: UpcasterRegistry):
        # Have v2->v3 but not v1->v2
        reg.register("room:*", 2, _up_v2_to_v3)
        with pytest.raises(UpcasterChainError, match="v1 -> v2"):
            reg.apply_chain({"schema_version": 1, "x": 1}, "room:5", target_version=3)

    def test_target_lower_than_current_raises(self, reg: UpcasterRegistry):
        # Producer wrote v5; registry only knows up to v2 → target=2 → can't
        # downcast, raise clearly.
        with pytest.raises(UpcasterChainError, match="newer schema"):
            reg.apply_chain({"schema_version": 5, "x": 1}, "room:5", target_version=2)

    def test_pattern_doesnt_match_no_progress_raises(self, reg: UpcasterRegistry):
        reg.register("room:*", 1, _up_v1_to_v2)
        # Asking for chain on chat:5 from v1->v2; no matching pattern → raise
        with pytest.raises(UpcasterChainError, match="v1 -> v2"):
            reg.apply_chain({"schema_version": 1}, "chat:5", target_version=2)


class TestPersistence:
    def test_creates_meta_stream(self):
        store = StreamStore()
        UpcasterRegistry(store=store)
        assert store.has(UPCASTERS_META_STREAM)

    def test_register_appends_event(self):
        store = StreamStore()
        reg = UpcasterRegistry(store=store)
        reg.register("room:*", 1, _up_v1_to_v2)
        messages, _ = store.read(UPCASTERS_META_STREAM)
        assert len(messages) == 1
        payload = json.loads(messages[0].data)
        assert payload["event_match"] == "room:*"
        assert payload["from_version"] == 1

    def test_idempotent_across_instances(self):
        store = StreamStore()
        reg1 = UpcasterRegistry(store=store)
        reg1.register("room:*", 1, _up_v1_to_v2)
        # Restart
        reg2 = UpcasterRegistry(store=store)
        reg2.register("room:*", 1, _up_v1_to_v2)
        messages, _ = store.read(UPCASTERS_META_STREAM)
        assert len(messages) == 1


class TestDecorator:
    def test_decorator_with_explicit_registry(self):
        reg = UpcasterRegistry()

        @register_upcaster("room:*", 1, registry=reg)
        def my_upcaster(event):
            return event

        assert len(reg.all_upcasters()) == 1

    def test_decorator_returns_original_function(self):
        reg = UpcasterRegistry()

        @register_upcaster("e", 1, registry=reg)
        def my_upcaster(event):
            return {**event, "tag": "x"}

        assert my_upcaster({"a": 1}) == {"a": 1, "tag": "x"}


class TestUpcastToCurrent:
    def test_applies_full_chain(self, reg: UpcasterRegistry):
        reg.register("orders", 1, _up_v1_to_v2)
        reg.register("orders", 2, _up_v2_to_v3)
        event = {"id": 1, "schema_version": 1}

        out = reg.upcast_to_current(event, "orders")

        assert out["schema_version"] == 3
        assert out["currency_code"] == "USD"
        assert "currency" not in out

    def test_noop_when_no_upcasters(self, reg: UpcasterRegistry):
        event = {"id": 1}
        out = reg.upcast_to_current(event, "orders")
        assert out == {"id": 1}

    def test_when_already_current(self, reg: UpcasterRegistry):
        reg.register("orders", 1, _up_v1_to_v2)
        event = {"id": 1, "schema_version": 2}  # already at current
        out = reg.upcast_to_current(event, "orders")
        assert out == {"id": 1, "schema_version": 2}


class TestModuleLevelUpcast:
    def test_uses_default_registry(self):
        from rakaia import upcast
        from rakaia.registry import get_default_upcaster_registry

        reg = get_default_upcaster_registry()

        def _u(event: dict) -> dict:
            return {**event, "normalised": True}

        reg.register("module_upcast_stream", 1, _u)
        try:
            out = upcast({"id": 1, "schema_version": 1}, "module_upcast_stream")
            assert out["normalised"] is True
            assert out["schema_version"] == 2
        finally:
            reg._upcasters.pop(("module_upcast_stream", 1), None)

    def test_explicit_registry(self, reg: UpcasterRegistry):
        from rakaia import upcast

        reg.register("orders", 1, _up_v1_to_v2)
        out = upcast({"id": 1, "schema_version": 1}, "orders", registry=reg)
        assert out["currency"] == "USD"
        assert out["schema_version"] == 2
