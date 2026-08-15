"""
Core types for the Durable Streams server.

Defines the data structures that represent streams, messages, producer state,
and protocol constants.
"""

from __future__ import annotations

import re
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

# Canonical offset-validation pattern: an opaque digit token — `{digits}` or
# `{digits}_{digits}` — or the sentinels `-1` (stream start) / `now` (current
# tail).
#
# Both documented offset formats must pass: the in-memory `StreamStore` emits
# the compound `{seq}_{byte}` form, the durable `DjangoStreamStore` a
# zero-padded integer. The pattern previously accepted only the compound form,
# which was invisible while the server could only ever be handed the in-memory
# store — the moment the durable store backed it, every resume read (`GET
# ?offset=…`) 400'd on an offset the server itself had just issued.
#
# This is a syntactic guard against junk in a URL, nothing more. The protocol
# mandates that offsets are opaque, not that they share one format (§6), so
# meaning belongs to the store that issued it (#49, #41). Accepting both formats
# therefore means a store *will* be handed the other's: each rejects one it did
# not issue with `InvalidOffset`, rather than reading from whichever position it
# happens to parse to.
VALID_OFFSET_PATTERN = re.compile(r"^(-1|now|\d+(_\d+)?)$")

# Default port for standalone servers
DEFAULT_PORT = 4437

# Producer state TTL: 7 days (in seconds)
PRODUCER_STATE_TTL_SECONDS = 7 * 24 * 60 * 60


# =============================================================================
# Store failures
# =============================================================================
#
# The closed set of failures a store raises and a protocol server maps to a
# status. Before these existed the mapping was a chain of substring tests over
# `str(e)` in `handler.py`, so an f-string reworded in `store.py` silently
# turned a 4xx into an unhandled 500 — and any other store implementation had
# to reproduce five exact English strings to get the same statuses. Naming them
# makes the mapping a lookup and the contract something a store can be tested
# against.
#
# Each subclasses the builtin it replaced (`KeyError` / `ValueError`), so code
# and tests that caught those keep working.


class StreamError(Exception):
    """Base for store failures a protocol server maps to a status."""


class StreamNotFound(StreamError, KeyError):
    """The stream does not exist, or has expired."""


class StreamConfigConflict(StreamError, ValueError):
    """A create names an existing stream with a different configuration."""


class SequenceConflict(StreamError, ValueError):
    """An append's `Stream-Seq` is not above the stream's last seq."""


class ContentTypeMismatch(StreamError, ValueError):
    """An append's content type disagrees with the stream's."""


class InvalidJson(StreamError, ValueError):
    """A JSON-mode payload did not parse."""


class EmptyJsonArray(StreamError, ValueError):
    """A JSON-mode append carried an empty array."""


class InvalidOffset(StreamError, ValueError):
    """An offset is syntactically valid but not one this store can read.

    `VALID_OFFSET_PATTERN` is a syntactic guard shared by every server; it
    cannot tell whether a given token is an offset *this* store issued, because
    the protocol makes offsets opaque rather than uniform (§6). A store that
    cannot interpret the token must say so, rather than parse it into some
    other position and read the wrong window.
    """


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
    """Transport timestamp: when the message was appended (``time.time()``).

    This is *append/wall-clock* time, set by the store — not a logical event
    time. For deterministic merge ordering across a backfill (where append order
    ≠ event order) use ``event_ts``, which a producer can set to a logical time.
    """

    event_ts: float | None = None
    """Envelope timestamp: the event's **logical** time, settable by the producer
    (via ``AppendOptions.event_ts``) and written once at append. A store populates
    it with the append time when the producer leaves it unset, so after a store
    append it is always a float; ``None`` only on a hand-constructed message.

    Distinct from ``timestamp`` (transport time) on purpose: a one-time backfill
    sets ``event_ts`` to the original historical event time while its transport
    ``timestamp`` is ≈now. ``merge_replay(order_key=ENVELOPE_TS)`` orders on this."""

    label: str = ""
    """Optional event-sourcing envelope: the change label (e.g. create/update/
    delete → +/~/-). Empty for pure-protocol messages; ignored by the transport."""

    metadata: dict | None = None
    """Optional event-sourcing envelope: an open metadata dict (actor, url,
    causation, …). None for pure-protocol messages; ignored by the transport."""


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
    """Stream metadata.

    Deliberately *not* the messages: a store returns this from ``get()`` to
    describe a stream, and where the messages actually live is the store's own
    business (the in-memory store keeps them in a side map; a durable store
    keeps them in the database).
    """

    path: str
    """The stream URL path (key)."""

    content_type: str | None = None
    """Content type of the stream."""

    current_offset: str = INITIAL_OFFSET
    """Current offset (next offset to write to)."""

    last_seq: str | None = None
    """Last `Stream-Seq` accepted, for writer coordination.

    An opaque string, compared byte-wise lexicographically as the protocol
    requires — never parsed as a number.
    """

    ttl_seconds: int | None = None
    """TTL in seconds."""

    expires_at: str | None = None
    """Absolute expiry time (RFC 3339)."""

    created_at: float = 0.0
    """Timestamp when the stream was created."""

    last_activity_at: float = 0.0
    """Timestamp of the last TTL-extending activity (sliding-window anchor).

    Reset to now on read/write/close so that Stream-TTL behaves as a sliding
    expiry window. Absolute Stream-Expires-At streams ignore this field.
    """

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
    """`Stream-Seq`: an opaque string compared byte-wise lexicographically."""
    content_type: str | None = None
    producer_id: str | None = None
    producer_epoch: int | None = None
    producer_seq: int | None = None
    close: bool = False
    label: str = ""
    """Event-sourcing envelope label to record on the appended message."""
    metadata: dict | None = None
    """Event-sourcing envelope metadata to record on the appended message."""
    event_ts: float | None = None
    """Event-sourcing envelope timestamp: the event's **logical** time (e.g. a
    live save → ``now()``; a backfill → the original historical event time). The
    store records it on ``StreamMessage.event_ts`` and defaults it to the append
    time when unset. This is the deterministic merge key
    (``merge_replay(order_key=ENVELOPE_TS)``), kept distinct from the transport
    ``StreamMessage.timestamp``."""


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
