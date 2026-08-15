"""A handler that needs a dependency must not lose drift detection to get one.

A stage-0 handler is called `fn(event)`. So a consumer that needs to inject
something — the canonical case is a hermeticity probe, "does this FK target
exist *on the alias I am replaying into*", which cannot be an ambient read
(ADR 0003) — has nowhere to put it. Both obvious routes were broken:

* **`functools.partial`** — rejected outright by `hash_function_source`, with a
  message telling the caller their handler is not in an importable source file.
  It is; the partial simply is not a function.
* **A closure factory** — accepted, and silently harmful. Two consequences, both
  verified below:

  1. `dotted_path` becomes `…build_registry.<locals>._projection`, which
     `rehydrate()` cannot import, so a registry restored from its meta-stream
     never re-registers that handler.
  2. `source_hash` hashes the **wrapper**, not the wrapped function. Rewriting
     the wrapped logic entirely leaves the hash identical — drift detection,
     the feature that exists to catch exactly that edit, goes blind.

The second is the serious one: it is a silent loss of a correctness guarantee,
and the library reports success throughout. The first consumer hit both, wrote a
comment explaining why it chose the closure over the partial, and had four
handlers with drift detection disabled without knowing.

The fix is to make `partial` the supported route: unwrap it, so identity and
source hash describe the function that holds the logic.
"""

from __future__ import annotations

import functools

import pytest

from rakaia.registry import HandlerRegistry
from rakaia.source_hash import hash_function_source
from rakaia.store import StreamStore


def probe_a(_model, _pk):
    return True


def probe_b(_model, _pk):
    return False


def projection(_event, *, fk_exists=None):  # noqa: ARG001
    """The handler that holds the real logic."""
    return []


def _closure_factory(fk_exists):
    def _projection(event):
        return projection(event, fk_exists=fk_exists)

    return _projection


class TestPartialIsSupported:
    """The route the first consumer tried first, and was refused."""

    def test_a_partial_can_be_source_hashed(self):
        hashed = hash_function_source(functools.partial(projection, fk_exists=probe_a))
        assert hashed

    def test_a_partial_hashes_as_the_function_it_wraps(self):
        """Identity follows the logic, not the binding — two different injected
        dependencies are the same handler code."""
        assert hash_function_source(
            functools.partial(projection, fk_exists=probe_a)
        ) == hash_function_source(projection)

    def test_a_partial_registers_with_an_importable_dotted_path(self):
        registry = HandlerRegistry()
        version = registry.register(
            name="p",
            event_match="s",
            fn=functools.partial(projection, fk_exists=probe_a),
            effective_from=0,
        )
        assert version.dotted_path.endswith(".projection")
        assert "<locals>" not in version.dotted_path

    def test_a_registered_partial_is_still_callable_with_one_argument(self):
        """Stage 0 dispatches `fn(event)`; the bound keyword must not change
        that."""
        registry = HandlerRegistry()
        version = registry.register(
            name="p",
            event_match="s",
            fn=functools.partial(projection, fk_exists=probe_a),
            effective_from=0,
        )
        assert version.fn({"id": 1}) == []

    def test_a_nested_partial_is_unwrapped_all_the_way(self):
        doubled = functools.partial(
            functools.partial(projection, fk_exists=probe_a), fk_exists=probe_b
        )
        assert hash_function_source(doubled) == hash_function_source(projection)


class TestDriftIsDetectedThroughTheWrapper:
    """The defect that motivates all of this."""

    def test_a_partials_hash_tracks_the_wrapped_function(self):
        """Not a tautology: it is the property a closure fails. Rebinding must
        not change the hash, but rewriting the wrapped body must."""
        one = hash_function_source(functools.partial(projection, fk_exists=probe_a))
        two = hash_function_source(functools.partial(projection, fk_exists=probe_b))
        assert one == two, "rebinding a dependency is not a logic change"
        assert one == hash_function_source(projection), (
            "the hash must describe the wrapped function, so an edit to it drifts"
        )

    def test_a_closure_hashes_only_its_own_body(self):
        """Characterization of the hazard, so the fix's value is visible: a
        closure's hash covers four lines of wrapper and nothing it calls."""
        closure = _closure_factory(probe_a)
        assert hash_function_source(closure) != hash_function_source(projection)


class TestAnUnimportableHandlerIsReported:
    """`rehydrate()` imports each recorded `dotted_path`'s module. A `<locals>`
    path cannot be imported, so a **persisted** registry silently loses that
    handler. Refusing is wrong — a closure is legitimate in a test, and rakaia's
    own suite registers lambdas — but it must not be silent.

    Scoped to registries with a store: one without persists nothing and restores
    nothing, so there is nothing to lose. Warning there would fire on every
    throwaway registry in every test suite, which is how a real warning gets
    filtered out and stops being read.
    """

    def test_registering_a_closure_warns(self):
        registry = HandlerRegistry(store=StreamStore())
        with pytest.warns(UserWarning, match="rehydrate"):
            registry.register(
                name="c",
                event_match="s",
                fn=_closure_factory(probe_a),
                effective_from=0,
            )

    def test_the_warning_names_the_offending_path(self):
        registry = HandlerRegistry(store=StreamStore())
        with pytest.warns(UserWarning) as caught:
            registry.register(
                name="c",
                event_match="s",
                fn=_closure_factory(probe_a),
                effective_from=0,
            )
        assert "<locals>" in str(caught[0].message)

    def test_a_closure_in_a_registry_without_a_store_is_quiet(self):
        """Nothing is recorded, so nothing can be lost."""
        import warnings

        with warnings.catch_warnings():
            warnings.simplefilter("error")
            HandlerRegistry().register(
                name="c",
                event_match="s",
                fn=_closure_factory(probe_a),
                effective_from=0,
            )

    def test_a_module_level_function_does_not_warn(self):
        import warnings

        registry = HandlerRegistry(store=StreamStore())
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            registry.register(
                name="ok", event_match="s", fn=projection, effective_from=0
            )

    def test_a_partial_does_not_warn(self):
        """The whole point: the supported route is quiet even when persisting."""
        import warnings

        registry = HandlerRegistry(store=StreamStore())
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            registry.register(
                name="p",
                event_match="s",
                fn=functools.partial(projection, fk_exists=probe_a),
                effective_from=0,
            )


class TestPersistenceRoundTrip:
    """A partial-registered handler must survive the meta-stream, which is what
    the unimportable path breaks."""

    def test_a_partial_records_an_importable_module(self):
        store = StreamStore()
        registry = HandlerRegistry(store=store)
        registry.register(
            name="p",
            event_match="s",
            fn=functools.partial(projection, fk_exists=probe_a),
            effective_from=0,
        )
        modules = registry._handler_log.modules()  # type: ignore[attr-defined]
        import importlib

        for module_name in modules:
            importlib.import_module(module_name)
