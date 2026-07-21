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
    Effect,
    EffectCollisionError,
    EffectOp,
    Executor,
    check_disjoint_defaults,
)
from .executors import CollectingExecutor
from .handler import ServerOptions, create_app
from .history import (
    envelope_actor,
    history_effects,
    label_marker,
    recover_peak_snapshot,
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
    register_upcaster,
    upcast,
)
from .replay import ReplayResult, merge_replay, replay
from .store import StreamStore
from .subscription import Poll, PollStatus, poll
from .types import (
    AppendOptions,
    AppendResult,
    ClosedBy,
    CloseResult,
    ProducerAccepted,
    ProducerDuplicate,
    ProducerInvalidEpochSeq,
    ProducerSequenceGap,
    ProducerStaleEpoch,
    ProducerState,
    ProducerStreamClosed,
    ProducerValidationResult,
    Stream,
    StreamMessage,
)

__version__ = "0.1.0"

# Default ASGI app for uvicorn: `uvicorn rakaia:app`
app = create_app()

__all__ = [
    # App factory
    "create_app",
    "app",
    # Store
    "StreamStore",
    # Extension protocols (storage / projection seams)
    "ReadableStore",
    "WritableStore",
    "CursorStore",
    "ProjectionReader",
    "provenance",
    "get_provenance",
    "append_if_changed",
    "snapshots_equal",
    "history_effects",
    "recover_peak_snapshot",
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
    # Cursor
    "calculate_cursor",
    "generate_response_cursor",
    # Versioned handlers — effects
    "Effect",
    "EffectOp",
    "Executor",
    "EffectCollisionError",
    "check_disjoint_defaults",
    "CollectingExecutor",
    # Versioned handlers — projections
    "reconcile_by_key",
    "reconcile_children",
    "reconcile_tree",
    "reconcile_aggregate",
    "project_latest",
    # Versioned handlers — registry
    "register_handler",
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
    "upcast",
    "HANDLERS_META_STREAM",
    "UPCASTERS_META_STREAM",
    # Versioned handlers — replay
    "replay",
    "merge_replay",
    "ReplayResult",
    # Subscriber cursors
    "poll",
    "Poll",
    "PollStatus",
    # Version
    "__version__",
]
