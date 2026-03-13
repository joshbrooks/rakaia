"""
Core types for the Durable Streams server.

Defines the data structures that represent streams, messages, producer state,
and protocol constants.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

# =============================================================================
# Protocol constants
# =============================================================================

# Response/request header names
STREAM_NEXT_OFFSET_HEADER = "stream-next-offset"
STREAM_CURSOR_HEADER = "stream-cursor"
STREAM_UP_TO_DATE_HEADER = "stream-up-to-date"
STREAM_CLOSED_HEADER = "stream-closed"
STREAM_SEQ_HEADER = "stream-seq"
STREAM_TTL_HEADER = "stream-ttl"
STREAM_EXPIRES_AT_HEADER = "stream-expires-at"
STREAM_SSE_DATA_ENCODING_HEADER = "stream-sse-data-encoding"

# Producer headers
PRODUCER_ID_HEADER = "producer-id"
PRODUCER_EPOCH_HEADER = "producer-epoch"
PRODUCER_SEQ_HEADER = "producer-seq"
PRODUCER_EXPECTED_SEQ_HEADER = "producer-expected-seq"
PRODUCER_RECEIVED_SEQ_HEADER = "producer-received-seq"

# Query parameter names
OFFSET_QUERY_PARAM = "offset"
LIVE_QUERY_PARAM = "live"
CURSOR_QUERY_PARAM = "cursor"

# SSE control event field names (camelCase per protocol)
SSE_OFFSET_FIELD = "streamNextOffset"
SSE_CURSOR_FIELD = "streamCursor"
SSE_UP_TO_DATE_FIELD = "upToDate"
SSE_CLOSED_FIELD = "streamClosed"

# Offset format: zero-padded 16-digit numbers separated by underscore
INITIAL_OFFSET = "0000000000000000_0000000000000000"

# Default port for standalone servers
DEFAULT_PORT = 4437

# Producer state TTL: 7 days (in seconds)
PRODUCER_STATE_TTL_SECONDS = 7 * 24 * 60 * 60


# =============================================================================
# Data structures
# =============================================================================


@dataclass
class StreamMessage:
    """A single message in a stream."""

    data: bytes
    """The raw bytes of the message."""

    offset: str
    """The offset after this message. Format: '{read_seq}_{byte_offset}'."""

    timestamp: float
    """Timestamp when the message was appended (time.time())."""


@dataclass
class ProducerState:
    """
    Producer state for idempotent writes.
    Tracks epoch and sequence number per producer ID for deduplication.
    """

    epoch: int
    """Current epoch for this producer."""

    last_seq: int
    """Last sequence number received in this epoch."""

    last_updated: float
    """Timestamp when this producer state was last updated."""


@dataclass
class ClosedBy:
    """Tracks which producer tuple closed a stream (for idempotent close)."""

    producer_id: str
    epoch: int
    seq: int


@dataclass
class Stream:
    """Stream metadata and data."""

    path: str
    """The stream URL path (key)."""

    content_type: str | None = None
    """Content type of the stream."""

    messages: list[StreamMessage] = field(default_factory=list)
    """Ordered messages in the stream."""

    current_offset: str = INITIAL_OFFSET
    """Current offset (next offset to write to)."""

    last_seq: str | None = None
    """Last sequence number for writer coordination (Stream-Seq)."""

    ttl_seconds: int | None = None
    """TTL in seconds."""

    expires_at: str | None = None
    """Absolute expiry time (RFC 3339)."""

    created_at: float = 0.0
    """Timestamp when the stream was created."""

    producers: dict[str, ProducerState] = field(default_factory=dict)
    """Producer states for idempotent writes. Maps producer ID to state."""

    closed: bool = False
    """Whether the stream is closed (no further appends permitted)."""

    closed_by: ClosedBy | None = None
    """The producer tuple that closed this stream (for idempotent close)."""


# =============================================================================
# Producer validation result types
# =============================================================================


@dataclass
class ProducerAccepted:
    """Producer validation: append accepted."""

    status: Literal["accepted"] = "accepted"
    is_new: bool = False
    producer_id: str = ""
    proposed_state: ProducerState | None = None


@dataclass
class ProducerDuplicate:
    """Producer validation: duplicate append (idempotent success)."""

    status: Literal["duplicate"] = "duplicate"
    last_seq: int = 0


@dataclass
class ProducerStaleEpoch:
    """Producer validation: stale epoch (zombie fencing)."""

    status: Literal["stale_epoch"] = "stale_epoch"
    current_epoch: int = 0


@dataclass
class ProducerInvalidEpochSeq:
    """Producer validation: new epoch must start at seq=0."""

    status: Literal["invalid_epoch_seq"] = "invalid_epoch_seq"


@dataclass
class ProducerSequenceGap:
    """Producer validation: sequence gap detected."""

    status: Literal["sequence_gap"] = "sequence_gap"
    expected_seq: int = 0
    received_seq: int = 0


@dataclass
class ProducerStreamClosed:
    """Producer validation: stream is already closed."""

    status: Literal["stream_closed"] = "stream_closed"


ProducerValidationResult = (
    ProducerAccepted
    | ProducerDuplicate
    | ProducerStaleEpoch
    | ProducerInvalidEpochSeq
    | ProducerSequenceGap
    | ProducerStreamClosed
)


# =============================================================================
# Operation option/result types
# =============================================================================


@dataclass
class AppendOptions:
    """Options for append operations."""

    seq: str | None = None
    content_type: str | None = None
    producer_id: str | None = None
    producer_epoch: int | None = None
    producer_seq: int | None = None
    close: bool = False


@dataclass
class AppendResult:
    """Result of an append operation."""

    message: StreamMessage | None = None
    producer_result: ProducerValidationResult | None = None
    stream_closed: bool = False


@dataclass
class CloseResult:
    """Result of a close operation."""

    final_offset: str = ""
    already_closed: bool = False
    producer_result: ProducerValidationResult | None = None
