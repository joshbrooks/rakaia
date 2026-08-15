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

from .append import append_if_changed, snapshots_equal
from .context import get_provenance, provenance
from .cursor import CursorOptions, calculate_cursor, generate_response_cursor
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
from .replay import ENVELOPE_TS, ReplayResult, TouchedSubject, merge_replay, replay
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


def _installed_version() -> str:
    """The version of the installed distribution.

    Read from package metadata rather than declared here, so there is exactly one
    place the version lives (`pyproject.toml`) instead of two that must be kept in
    step by hand. They drifted: this literal stayed `"0.1.0"` across 23 commits
    and several breaking changes, so `rakaia.__version__` reported a number that
    no longer described the code — to a consumer, confidently.

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


__version__ = _installed_version()

# Default ASGI app for uvicorn: `uvicorn rakaia:app`
app = create_app()

__all__ = [
    # App factory
    "create_app",
    "app",
    # Store
    "StreamStore",
    "seed_stream",
    # Extension protocols (storage / projection seams)
    "ReadableStore",
    "WritableStore",
    "StreamServerStore",
    "CursorStore",
    "ProjectionReader",
    "provenance",
    "get_provenance",
    "append_if_changed",
    "snapshots_equal",
    "history_effects",
    "label_marker",
    "envelope_actor",
    # Options
    "ServerOptions",
    "CursorOptions",
    # Types
    "Stream",
    "StreamMessage",
    "ProducerState",
    "ClosedBy",
    "AppendOptions",
    "AppendResult",
    "CloseResult",
    # Producer validation
    "ProducerValidationResult",
    "ProducerAccepted",
    "ProducerDuplicate",
    "ProducerStaleEpoch",
    "ProducerInvalidEpochSeq",
    "ProducerSequenceGap",
    "ProducerStreamClosed",
    # Store failures (each subclasses the builtin it replaced)
    "StreamError",
    "StreamNotFound",
    "StreamConfigConflict",
    "SequenceConflict",
    "ContentTypeMismatch",
    "InvalidJson",
    "EmptyJsonArray",
    "InvalidOffset",
    # Cursor
    "calculate_cursor",
    "generate_response_cursor",
    # Versioned handlers — effects
    "Effect",
    "AnyEffect",
    "RowEffect",
    "Upsert",
    "Update",
    "Delete",
    "Retire",
    "ExternalEffect",
    "Exclude",
    "SpareKeys",
    "Transition",
    "Executor",
    "EffectCollisionError",
    "DuplicateProducesError",
    "Ref",
    "RefResolver",
    "UnresolvedRefError",
    "check_disjoint_defaults",
    "CollectingExecutor",
    "InMemoryProjections",
    # Versioned handlers — projections
    "reconcile_by_key",
    "reconcile_children",
    "reconcile_tree",
    "reconcile_aggregate",
    "project_latest",
    # Versioned handlers — registry
    "register_handler",
    "register_simple",
    "register_reducer",
    "register_upcaster",
    "HandlerRegistry",
    "UpcasterRegistry",
    "HandlerVersion",
    "ReducerVersion",
    "REDUCERS_META_STREAM",
    "UpcasterVersion",
    "HandlerOverlapError",
    "HandlerGapError",
    "HandlerDriftError",
    "UpcasterConflictError",
    "UpcasterChainError",
    "get_default_registry",
    "get_default_upcaster_registry",
    "reset_default_registries",
    "upcast",
    "HANDLERS_META_STREAM",
    "UPCASTERS_META_STREAM",
    # Versioned handlers — replay
    "replay",
    "merge_replay",
    "ENVELOPE_TS",
    "ReplayResult",
    "TouchedSubject",
    # Subscriber cursors
    "poll",
    "Poll",
    "PollStatus",
    # Version
    "__version__",
]
