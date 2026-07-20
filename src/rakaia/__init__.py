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
from .projections import reconcile_by_key, reconcile_children
from .registry import (
    HANDLERS_META_STREAM,
    UPCASTERS_META_STREAM,
    HandlerDriftError,
    HandlerGapError,
    HandlerOverlapError,
    HandlerRegistry,
    HandlerVersion,
    UpcasterChainError,
    UpcasterConflictError,
    UpcasterRegistry,
    UpcasterVersion,
    get_default_registry,
    get_default_upcaster_registry,
    register_handler,
    register_upcaster,
    upcast,
)
from .replay import ReplayResult, replay
from .store import StreamStore
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
    # Versioned handlers — registry
    "register_handler",
    "register_upcaster",
    "HandlerRegistry",
    "UpcasterRegistry",
    "HandlerVersion",
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
    "ReplayResult",
    # Version
    "__version__",
]
