"""Tests for registry reset / test-isolation (#38).

Covers `HandlerRegistry.reset()`, `UpcasterRegistry.reset()`, and the
`reset_default_registries()` teardown helper — the documented way to stop
registrations leaking between tests.
"""

from __future__ import annotations

from rakaia.registry import (
    HANDLERS_META_STREAM,
    HandlerRegistry,
    UpcasterRegistry,
    get_default_registry,
    get_default_upcaster_registry,
    register_handler,
    register_upcaster,
    reset_default_registries,
)
from rakaia.store import StreamStore


def _fn(tag: str):
    def handler(event):  # noqa: ARG001
        return tag

    handler.__qualname__ = f"_fn.<{tag}>"
    return handler


def _up(tag: str):
    def upcaster(event):
        return {**event, "tag": tag}

    upcaster.__qualname__ = f"_up.<{tag}>"
    return upcaster


class TestHandlerRegistryReset:
    def test_reset_clears_handlers_and_reducers(self):
        reg = HandlerRegistry()
        reg.register("mogrify", "room:*", _fn("v1"), 0, 10)
        reg.register_reducer("balance", 1, _fn("b"))
        assert reg.all_versions()
        assert reg.all_reducers()

        reg.reset()

        assert reg.all_versions() == []
        assert reg.all_reducers() == []
        assert reg.stages() == []
        assert reg.has_reducers() is False

    def test_reset_allows_reregistering_a_conflicting_range(self):
        # A range that would overlap the first registration is accepted again
        # after reset, proving the in-memory series was cleared.
        reg = HandlerRegistry()
        reg.register("m", "e", _fn("v1"), 0, None)
        reg.reset()
        reg.register("m", "e", _fn("v2"), 0, None)  # would overlap pre-reset
        assert [v.effective_from for v in reg.all_versions()] == [0]

    def test_reset_reappends_audit_event_on_store_backed(self):
        # Clearing the dedup cache means an identical re-registration appends a
        # fresh audit event to the meta-stream (reset() is in-memory only; it
        # does not delete the durable stream).
        store = StreamStore()
        reg = HandlerRegistry(store=store)
        reg.register("mogrify", "room:*", _fn("v1"), 0, 10)
        assert len(store.read(HANDLERS_META_STREAM)[0]) == 1

        reg.reset()
        assert store.has(HANDLERS_META_STREAM)  # durable stream survives reset

        reg.register("mogrify", "room:*", _fn("v1"), 0, 10)
        assert len(store.read(HANDLERS_META_STREAM)[0]) == 2


class TestUpcasterRegistryReset:
    def test_reset_clears_upcasters(self):
        reg = UpcasterRegistry()
        reg.register("room:*", 1, _up("v1"))
        assert reg.all_upcasters()

        reg.reset()

        assert reg.all_upcasters() == []
        # A different fn is now accepted at the same key (no conflict) — proof
        # the in-memory map was cleared.
        reg.register("room:*", 1, _up("v2"))
        assert len(reg.all_upcasters()) == 1


class TestResetDefaultRegistries:
    def test_resets_both_default_registries(self):
        register_handler("m", "room:*", 0, 10)(_fn("v1"))
        register_upcaster("room:*", 1)(_up("v1"))
        assert get_default_registry().all_versions()
        assert get_default_upcaster_registry().all_upcasters()

        reset_default_registries()

        assert get_default_registry().all_versions() == []
        assert get_default_upcaster_registry().all_upcasters() == []


class TestAutouseIsolation:
    """The autouse conftest fixture must leave each test a clean default
    registry — these two tests register the same handler and neither sees the
    other's registration (order-independent)."""

    def test_first_registers_on_clean_default(self):
        assert get_default_registry().all_versions() == []
        register_handler("leaky", "room:*", 0, None)(_fn("v1"))
        assert len(get_default_registry().all_versions()) == 1

    def test_second_also_sees_clean_default(self):
        assert get_default_registry().all_versions() == []
        register_handler("leaky", "room:*", 0, None)(_fn("v1"))
        assert len(get_default_registry().all_versions()) == 1
