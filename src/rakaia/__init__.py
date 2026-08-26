"""
Rakaia — Python implementation of the Durable Streams protocol.

A zero-dependency ASGI application that can run standalone (uvicorn/daphne)
or be mounted in Django, FastAPI, or Starlette.

Usage:
    from rakaia import create_app, StreamStore

    # Create ASGI app
    app = create_app()

    # Or with custom store/options
    store = StreamStore()
    app = create_app(store=store)

    # Run with: uvicorn rakaia:app
"""

from __future__ import annotations

import threading
from typing import TYPE_CHECKING, Any

# =============================================================================
# Public API
# =============================================================================
#
# Resolved lazily (PEP 562), the same way `django_rakaia` resolves its own — for
# a different reason, and the reason is worth stating because it is not the
# obvious one.
#
# This package is two tiers in one distribution (ADR 0002): the event-sourcing
# framework, and the Durable Streams protocol server. Eager imports here meant
# that importing the framework loaded the whole server — all ten of its modules
# — and `app = create_app()` at module scope allocated an in-memory
# `StreamStore` in every process that so much as touched `import rakaia`,
# whether or not it ever served a request. Measured before this change: 80ms
# against 37ms for the framework alone, and one store per process nobody asked
# for.
#
# The tier boundary is asserted by `tests/test_rakaia/test_tier_boundary.py`;
# this is what stops the package root quietly undoing it. Nothing is imported
# until a name is touched, so a consumer pays for the tier it uses.
#
# See `docs/public-api.md` for what Tier 1 guarantees, and ADR 0002 for why the
# two tiers still share a distribution.

if TYPE_CHECKING:
    # Re-exported for type checkers only: the runtime path is `__getattr__`
    # below, which pyright cannot follow. Every name here is in `_EXPORTS`, and
    # `test_public_api.py` fails if the two ever disagree.

    # The two names `__getattr__` computes rather than imports. Declared as bare
    # annotations — the standard way to tell a checker that a PEP 562 module
    # provides a name — because `__all__` listing something with no binding is
    # `reportUnsupportedDunderAll` otherwise.
    app: Any
    __version__: str

    from .append import append_if_changed, snapshots_equal
    from .context import get_provenance, provenance
    from .cursor import CursorOptions, calculate_cursor, generate_response_cursor
    from .drift import DriftLedger
    from .effects import (
        AnyEffect,
        Delete,
        DuplicateProducesError,
        Effect,
        EffectCollisionError,
        Exclude,
        Executor,
        ExternalEffect,
        Ref,
        RefResolver,
        Retire,
        RowEffect,
        SpareKeys,
        Transition,
        UnresolvedRefError,
        Update,
        Upsert,
        check_disjoint_defaults,
    )
    from .executors import CollectingExecutor, InMemoryProjections
    from .handler import ServerOptions, create_app
    from .history import (
        envelope_actor,
        history_effects,
        label_marker,
    )
    from .jsonl_store import JsonlStreamStore
    from .migrate import Migration, migrate_all, migrate_stream
    from .offsets import ForeignOffset
    from .projections import (
        project_latest,
        reconcile_aggregate,
        reconcile_by_key,
        reconcile_children,
        reconcile_tree,
    )
    from .protocols import (
        CursorStore,
        ProjectionReader,
        ReadableStore,
        StreamServerStore,
        WritableStore,
    )
    from .registry import (
        HANDLERS_META_STREAM,
        REDUCERS_META_STREAM,
        UPCASTERS_META_STREAM,
        HandlerDriftError,
        HandlerGapError,
        HandlerOverlapError,
        HandlerRegistry,
        HandlerVersion,
        ReducerVersion,
        UpcasterChainError,
        UpcasterConflictError,
        UpcasterRegistry,
        UpcasterVersion,
        get_default_registry,
        get_default_upcaster_registry,
        register_handler,
        register_reducer,
        register_simple,
        register_upcaster,
        reset_default_registries,
        upcast,
    )
    from .replay import ENVELOPE_TS, ReplayResult, TouchedSubject, merge_replay
    from .seed import seed_stream
    from .store import StreamStore
    from .subscription import Poll, PollStatus, poll
    from .types import (
        AppendOptions,
        AppendResult,
        ClosedBy,
        CloseResult,
        ContentTypeMismatch,
        EmptyJsonArray,
        InvalidJson,
        InvalidOffset,
        ProducerAccepted,
        ProducerDuplicate,
        ProducerInvalidEpochSeq,
        ProducerSequenceGap,
        ProducerStaleEpoch,
        ProducerState,
        ProducerStreamClosed,
        ProducerValidationResult,
        SequenceConflict,
        Stream,
        StreamConfigConflict,
        StreamError,
        StreamMessage,
        StreamNotFound,
    )

# `replay` is bound eagerly, and is the one name that has to be.
#
# It is the only export whose name is also a submodule name (asserted by
# `test_public_api.py`), and that collision cannot be resolved lazily. Importing
# `rakaia.replay` makes the import system set `replay` on this package to the
# *module*; once that attribute exists, `__getattr__` is never consulted, so
# `from rakaia import replay` would hand back a module or a function depending
# on whether anything else in the process had imported the submodule first.
#
# Binding here reproduces exactly what the eager version did — the submodule
# loads, then this rebinds the name to the function, and nothing rebinds it
# afterwards. The function wins deterministically, which is the documented
# sharp edge of #161 item 1 and the status quo this change must not alter.
# The cost is that `rakaia.replay` (framework tier) always loads; measured at
# well under a millisecond, and it pulls no protocol-server module.
from .replay import replay

#: name -> the module that defines it.
_EXPORTS: dict[str, str] = {
    # App factory
    "create_app": "rakaia.handler",
    # "app" is computed, not imported — see __getattr__.
    # Store
    "StreamStore": "rakaia.store",
    "JsonlStreamStore": "rakaia.jsonl_store",
    "seed_stream": "rakaia.seed",
    # Moving a log between backends
    "migrate_stream": "rakaia.migrate",
    "migrate_all": "rakaia.migrate",
    "Migration": "rakaia.migrate",
    # Extension protocols (storage / projection seams)
    "ReadableStore": "rakaia.protocols",
    "WritableStore": "rakaia.protocols",
    "StreamServerStore": "rakaia.protocols",
    "CursorStore": "rakaia.protocols",
    "ProjectionReader": "rakaia.protocols",
    "provenance": "rakaia.context",
    "get_provenance": "rakaia.context",
    "append_if_changed": "rakaia.append",
    "snapshots_equal": "rakaia.append",
    "history_effects": "rakaia.history",
    "label_marker": "rakaia.history",
    "envelope_actor": "rakaia.history",
    # Options
    "ServerOptions": "rakaia.handler",
    "CursorOptions": "rakaia.cursor",
    # Types
    "Stream": "rakaia.types",
    "StreamMessage": "rakaia.types",
    "ProducerState": "rakaia.types",
    "ClosedBy": "rakaia.types",
    "AppendOptions": "rakaia.types",
    "AppendResult": "rakaia.types",
    "CloseResult": "rakaia.types",
    # Producer validation
    "ProducerValidationResult": "rakaia.types",
    "ProducerAccepted": "rakaia.types",
    "ProducerDuplicate": "rakaia.types",
    "ProducerStaleEpoch": "rakaia.types",
    "ProducerInvalidEpochSeq": "rakaia.types",
    "ProducerSequenceGap": "rakaia.types",
    "ProducerStreamClosed": "rakaia.types",
    # Store failures (each subclasses the builtin it replaced)
    "StreamError": "rakaia.types",
    "StreamNotFound": "rakaia.types",
    "StreamConfigConflict": "rakaia.types",
    "SequenceConflict": "rakaia.types",
    "ContentTypeMismatch": "rakaia.types",
    "InvalidJson": "rakaia.types",
    "EmptyJsonArray": "rakaia.types",
    "ForeignOffset": "rakaia.offsets",
    "InvalidOffset": "rakaia.types",
    # Cursor
    "calculate_cursor": "rakaia.cursor",
    "generate_response_cursor": "rakaia.cursor",
    # Versioned handlers — effects
    "Effect": "rakaia.effects",
    "AnyEffect": "rakaia.effects",
    "RowEffect": "rakaia.effects",
    "Upsert": "rakaia.effects",
    "Update": "rakaia.effects",
    "Delete": "rakaia.effects",
    "Retire": "rakaia.effects",
    "ExternalEffect": "rakaia.effects",
    "Exclude": "rakaia.effects",
    "SpareKeys": "rakaia.effects",
    "Transition": "rakaia.effects",
    "Executor": "rakaia.effects",
    "EffectCollisionError": "rakaia.effects",
    "DuplicateProducesError": "rakaia.effects",
    "Ref": "rakaia.effects",
    "RefResolver": "rakaia.effects",
    "UnresolvedRefError": "rakaia.effects",
    "check_disjoint_defaults": "rakaia.effects",
    "CollectingExecutor": "rakaia.executors",
    "InMemoryProjections": "rakaia.executors",
    # Versioned handlers — projections
    "reconcile_by_key": "rakaia.projections",
    "reconcile_children": "rakaia.projections",
    "reconcile_tree": "rakaia.projections",
    "reconcile_aggregate": "rakaia.projections",
    "project_latest": "rakaia.projections",
    # Versioned handlers — registry
    "register_handler": "rakaia.registry",
    "register_simple": "rakaia.registry",
    "register_reducer": "rakaia.registry",
    "register_upcaster": "rakaia.registry",
    "HandlerRegistry": "rakaia.registry",
    "UpcasterRegistry": "rakaia.registry",
    "HandlerVersion": "rakaia.registry",
    "ReducerVersion": "rakaia.registry",
    "REDUCERS_META_STREAM": "rakaia.registry",
    "UpcasterVersion": "rakaia.registry",
    "HandlerOverlapError": "rakaia.registry",
    "HandlerGapError": "rakaia.registry",
    "HandlerDriftError": "rakaia.registry",
    "UpcasterConflictError": "rakaia.registry",
    "UpcasterChainError": "rakaia.registry",
    "get_default_registry": "rakaia.registry",
    "get_default_upcaster_registry": "rakaia.registry",
    "reset_default_registries": "rakaia.registry",
    "upcast": "rakaia.registry",
    "HANDLERS_META_STREAM": "rakaia.registry",
    "UPCASTERS_META_STREAM": "rakaia.registry",
    # Versioned handlers — replay
    "replay": "rakaia.replay",
    "merge_replay": "rakaia.replay",
    "ENVELOPE_TS": "rakaia.replay",
    "ReplayResult": "rakaia.replay",
    "TouchedSubject": "rakaia.replay",
    "DriftLedger": "rakaia.drift",
    # Subscriber cursors
    "poll": "rakaia.subscription",
    "Poll": "rakaia.subscription",
    "PollStatus": "rakaia.subscription",
    # Version
    # "__version__" is computed, not imported — see __getattr__.
}

__all__ = [*sorted(_EXPORTS), "app", "__version__"]


def _installed_version() -> str:
    """The installed distribution's version.

    The distribution is `rakaia-streams`; the import name is `rakaia` (plain
    `rakaia` was taken on PyPI), so the lookup cannot use `__name__`.

    Falls back to ``"0.0.0+unknown"`` when the package is not installed at all —
    running straight from a source tree with no metadata. A sentinel is better
    than a plausible-looking number: it cannot be mistaken for a release.
    """
    from importlib.metadata import PackageNotFoundError, version

    try:
        return version("rakaia-streams")
    except PackageNotFoundError:  # pragma: no cover - source tree without install
        return "0.0.0+unknown"


#: Guards the one-time construction of `app`. See `__getattr__`.
_app_lock = threading.Lock()


def __getattr__(name: str) -> Any:
    """Resolve a public name on first use (PEP 562).

    The resolved value is cached in `globals()`, so this runs once per name and
    every later access is an ordinary module attribute.

    Only `app` needs the lock. A mapped name resolves through
    `importlib.import_module`, which is itself serialised and idempotent, so two
    threads racing for one get the same object either way; the worst case is a
    duplicated `getattr`. `app` is *constructed* here, and construction is not
    idempotent — it allocates a `StreamStore`. Without the lock, every thread
    that got past the cache check built its own, and all but the last were
    orphaned: live, holding their own log, and unreachable through
    `rakaia.app`. Measured at 16 threads: 16 stores, 15 of them lost. The eager
    `app = create_app()` this replaced was serialised by the import lock, so the
    lock is restoring a property that used to come for free rather than adding
    one.
    """
    if name == "app":
        # The default ASGI app, for `uvicorn rakaia:app`. Built here rather than
        # at module scope so that importing the framework does not allocate a
        # server's worth of state — this is the singleton the eager version
        # created in every process.
        with _app_lock:
            # Re-checked inside the lock: a thread that waited here while
            # another built it must take that one, not build a second.
            if "app" not in globals():
                from .handler import create_app

                globals()["app"] = create_app()
            return globals()["app"]

    if name == "__version__":
        value: Any = _installed_version()
    else:
        module = _EXPORTS.get(name)
        if module is None:
            raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
        import importlib

        value = getattr(importlib.import_module(module), name)

    globals()[name] = value
    return value


def __dir__() -> list[str]:
    return sorted(__all__)
