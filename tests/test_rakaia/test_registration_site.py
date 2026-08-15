"""Restoring a registration means re-importing the module that *registered* it.

`rehydrate()` works by import side effect: it imports modules and lets the
`@register_handler` decorators in them run again. So the module it has to import
is the one holding the **decorator call**, which is not necessarily the one
holding the function.

The meta-stream used to record only `dotted_path` — the function's
`__module__.__qualname__`, taken from the *unwrapped* callable — and derive the
module to import by chopping the last segment off it. That silently assumed two
things:

* the decoration site and the definition site are the same module. Since
  `functools.partial(fn, **deps)` became the documented way to bind a handler
  dependency, they routinely are not: `fn` lives in a shared module and the
  binding happens in an app's `handlers.py`. Importing `fn`'s module runs no
  decorator, so the handler came back from a restart missing, with no error.
* the qualname has exactly one segment past the module. A handler defined as a
  method has `pkg.mod.Class.method`, so the chop yields `pkg.mod.Class` — an
  `ImportError` on rehydrate, or worse a stale hit.

A registration now records `registered_in` — the module of the frame that called
the decorator — and that is what `rehydrate()` imports. `dotted_path` keeps
meaning "where the logic is", which is what drift detection and the
`<locals>` warning want from it.
"""

from __future__ import annotations

import sys

import pytest

from rakaia.registry import HandlerRegistry, UpcasterRegistry
from rakaia.store import StreamStore

from . import registration_sites

WIRING = "tests.test_rakaia.registration_sites.wiring"
METHOD_WIRING = "tests.test_rakaia.registration_sites.method_wiring"
LOGIC = "tests.test_rakaia.registration_sites.logic"


@pytest.fixture
def store() -> StreamStore:
    return StreamStore()


@pytest.fixture(autouse=True)
def _unimported():
    """Each test starts with the wiring modules unimported and restores that
    afterwards, so `rehydrate()` genuinely re-runs their decorators — the fresh
    process it exists to stand in for."""
    for name in (WIRING, METHOD_WIRING):
        sys.modules.pop(name, None)
    yield
    for name in (WIRING, METHOD_WIRING):
        sys.modules.pop(name, None)
    registration_sites.handler_registry = None
    registration_sites.upcaster_registry = None


def _wire(module: str, handlers: HandlerRegistry, upcasters: UpcasterRegistry) -> None:
    """Import a wiring module fresh, registering against these registries."""
    registration_sites.handler_registry = handlers
    registration_sites.upcaster_registry = upcasters
    sys.modules.pop(module, None)
    __import__(module)


class TestAHandlerRegisteredAwayFromWhereItIsDefined:
    """The #117 case: `logic.project_room` is bound with `functools.partial` and
    registered from `wiring`. Only `wiring` running restores the registration."""

    def test_the_handler_registers_in_the_first_place(self, store):
        handlers = HandlerRegistry(store=store)
        _wire(WIRING, handlers, UpcasterRegistry(store=store))
        assert [v.name for v in handlers.all_versions()] == ["room"]

    def test_the_dotted_path_still_points_at_the_function(self, store):
        """Unchanged, deliberately: drift detection and the `<locals>` warning
        both want the function, not the wiring."""
        handlers = HandlerRegistry(store=store)
        _wire(WIRING, handlers, UpcasterRegistry(store=store))
        (version,) = handlers.all_versions()
        assert version.dotted_path == f"{LOGIC}.project_room"

    def test_the_registration_site_is_recorded_separately(self, store):
        handlers = HandlerRegistry(store=store)
        _wire(WIRING, handlers, UpcasterRegistry(store=store))
        (version,) = handlers.all_versions()
        assert version.registered_in == WIRING

    def test_a_fresh_registry_restores_the_handler(self, store):
        """The bug, stated as the behaviour that was missing: a new process
        reads the meta-stream, imports what it names, and has the handler back."""
        _wire(WIRING, HandlerRegistry(store=store), UpcasterRegistry(store=store))

        sys.modules.pop(WIRING, None)
        restored = HandlerRegistry(store=store)
        registration_sites.handler_registry = restored
        registration_sites.upcaster_registry = UpcasterRegistry()
        restored.rehydrate()

        assert [v.name for v in restored.all_versions()] == ["room"]

    def test_a_fresh_registry_restores_the_reducer(self, store):
        _wire(WIRING, HandlerRegistry(store=store), UpcasterRegistry(store=store))

        sys.modules.pop(WIRING, None)
        restored = HandlerRegistry(store=store)
        registration_sites.handler_registry = restored
        registration_sites.upcaster_registry = UpcasterRegistry()
        restored.rehydrate()

        assert [r.name for r in restored.all_reducers()] == ["rooms"]

    def test_a_fresh_upcaster_registry_restores_the_upcaster(self, store):
        _wire(WIRING, HandlerRegistry(store=store), UpcasterRegistry(store=store))

        sys.modules.pop(WIRING, None)
        restored = UpcasterRegistry(store=store)
        registration_sites.handler_registry = HandlerRegistry()
        registration_sites.upcaster_registry = restored
        restored.rehydrate()

        assert [u.event_match for u in restored.all_upcasters()] == ["room:*"]

    def test_importing_only_the_definition_module_restores_nothing(self, store):
        """Why the old derivation could not work, made explicit: importing
        `logic` runs no decorator, so a `modules()` that named it would restore
        an empty registry however faithfully it was imported."""
        _wire(WIRING, HandlerRegistry(store=store), UpcasterRegistry(store=store))

        sys.modules.pop(WIRING, None)
        restored = HandlerRegistry(store=store)
        registration_sites.handler_registry = restored
        __import__(LOGIC)

        assert restored.all_versions() == []


class TestAHandlerDefinedAsAMethod:
    """`pkg.mod.Class.method` — chopping one segment yields the class."""

    def test_the_recorded_site_is_a_module_not_the_class(self, store):
        handlers = HandlerRegistry(store=store)
        _wire(METHOD_WIRING, handlers, UpcasterRegistry(store=store))
        (version,) = handlers.all_versions()
        assert version.dotted_path == f"{LOGIC}.Projector.project"
        assert version.registered_in == METHOD_WIRING

    def test_rehydrate_does_not_try_to_import_the_class(self, store):
        _wire(
            METHOD_WIRING, HandlerRegistry(store=store), UpcasterRegistry(store=store)
        )

        sys.modules.pop(METHOD_WIRING, None)
        restored = HandlerRegistry(store=store)
        registration_sites.handler_registry = restored
        restored.rehydrate()

        assert [v.name for v in restored.all_versions()] == ["methods"]


class TestTheRecordedSiteIsPartOfWhatIsPersisted:
    def test_it_survives_a_reload_of_the_meta_stream(self, store):
        _wire(WIRING, HandlerRegistry(store=store), UpcasterRegistry(store=store))
        assert WIRING in HandlerRegistry(store=store)._handler_log.modules()

    def test_the_same_registration_from_the_same_site_still_dedups(self, store):
        """Recording the site must not make every restart re-append."""
        from rakaia.registry import HANDLERS_META_STREAM

        _wire(WIRING, HandlerRegistry(store=store), UpcasterRegistry(store=store))
        before = len(store.read(HANDLERS_META_STREAM)[0])
        _wire(WIRING, HandlerRegistry(store=store), UpcasterRegistry(store=store))
        assert len(store.read(HANDLERS_META_STREAM)[0]) == before


class TestOlderMetaStreamsStillLoad:
    """Payloads written before `registered_in` existed have to keep working —
    they fall back to the old derivation, which is right whenever the decoration
    site and the definition site were the same module (the case that worked)."""

    def test_a_payload_without_the_field_falls_back_to_the_dotted_path(self, store):
        import json

        from rakaia.registry import HANDLERS_META_STREAM

        store.create(HANDLERS_META_STREAM)
        store.append(
            HANDLERS_META_STREAM,
            json.dumps(
                {
                    "name": "legacy",
                    "event_match": "x",
                    "effective_from": 0,
                    "effective_to": None,
                    "dotted_path": "pkg.legacy_mod.handler",
                    "source_hash": "deadbeef",
                }
            ).encode("utf-8"),
        )
        assert HandlerRegistry(store=store)._handler_log.modules() == {"pkg.legacy_mod"}

    def test_an_old_payload_dedups_against_the_same_registration_today(self):
        """A same-module registration reconstructs the same `registered_in` the
        fallback produces, so upgrading does not re-append the whole log."""
        import json

        from rakaia.registry import HandlerVersion

        live = HandlerVersion(
            name="legacy",
            event_match="x",
            effective_from=0,
            effective_to=None,
            fn=lambda _e: [],
            dotted_path="pkg.legacy_mod.handler",
            source_hash="deadbeef",
            registered_in="pkg.legacy_mod",
        )
        old_payload = json.loads(json.dumps(live.to_payload()))
        del old_payload["registered_in"]
        assert HandlerVersion.identity_from_payload(old_payload) == live.identity
