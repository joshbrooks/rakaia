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
from .handler import ServerOptions, create_app
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
    # Version
    "__version__",
]
