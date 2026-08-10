"""
Structural protocols for pluggable storage and projection backends — the
extension seams of the event-sourcing framework (ADR 0002).

`replay()` only ever *reads* a stream, so it depends on the narrow
`ReadableStore` protocol rather than the concrete in-memory `StreamStore`.
Producers and the meta-stream registry additionally *write*, captured by
`WritableStore`. Subscribers additionally need the head offset, captured by
`CursorStore`. Staged handlers read committed projections through
`ProjectionReader`. Any backend satisfying the relevant protocol — the in-memory
`StreamStore`, a durable DB-backed store, or a third-party one — plugs in.

All store-facing protocols live here so they read as one coherent seam:
`ReadableStore` (read), `WritableStore` (+ create/append/has), `CursorStore`
(+ get_current_offset).
"""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any, Protocol, runtime_checkable

from .types import StreamMessage


@runtime_checkable
class ReadableStore(Protocol):
    """A store `replay()` can read events from.

    **Offset contract** (Durable Streams protocol §6). A ``StreamMessage.offset``
    is an **opaque, lexicographically-sortable** token that marks a position in a
    stream, and successive appends to one stream yield strictly increasing
    offsets. Its byte *format* is store-specific — the in-memory `StreamStore`
    uses a compound ``{seq}_{byte}`` string, `DjangoStreamStore` a zero-padded
    integer — so consumers **MUST** treat an offset as opaque: pass it back to
    ``read(path, offset=…)`` to resume, and compare offsets from *the same store*
    lexicographically for ordering. Do **not** parse one (no ``int(offset)``);
    that couples you to one backend and breaks portability. For a monotonic
    per-event number, use the event's position in the read sequence or an envelope
    field — not the offset.
    """

    def read(
        self, path: str, offset: str | None = None
    ) -> tuple[list[StreamMessage], bool]:
        """Return ``(messages, up_to_date)`` for `path`, ordered oldest-first.

        With no `offset`, returns every message; with an `offset`, returns the
        messages after it (offsets are opaque — see the class docstring). Raises
        `KeyError` if the stream does not exist.
        """
        ...


@runtime_checkable
class WritableStore(ReadableStore, Protocol):
    """A store the event-sourcing framework both writes to and reads from.

    This is the **framework** store surface — what producers and the meta-stream
    registry rely on: create a stream, append enveloped events, check existence,
    and read them back (inherited from `ReadableStore`). It deliberately excludes
    the Durable Streams **protocol-server** lifecycle (producer epoch/seq
    fencing, close, TTL, long-poll/`wait_for_messages`), which is not required to
    back `replay()`/projections. That lifecycle is `StreamServerStore` below.
    See ADR 0002.

    Return types stay loose (`Any`) so a backend can return whatever suits it;
    both current stores return `AppendResult` from `append`. What they guarantee
    is the read-back behaviour exercised by the shared conformance suite
    (`tests/store_contract.py`).
    """

    def has(self, path: str) -> bool:
        """Whether a stream exists at `path`."""
        ...

    def create(self, path: str) -> Any:
        """Idempotently create the stream at `path`. `append` requires it to exist.

        Only idempotent creation by `path` is part of the contract. Concrete
        stores may accept extra keyword-only options (content type, TTL, …) via
        their own richer signatures — deliberately *not* on this protocol, so a
        backend that ignores such an option (e.g. `DjangoStreamStore`) can't be
        called with it through the `WritableStore` type and silently no-op.
        """
        ...

    def append(self, path: str, data: bytes, options: Any = None) -> Any:
        """Append one (optionally enveloped) event to `path`.

        Raises `StreamNotFound` (a `KeyError`) if the stream does not exist —
        the one exception every `WritableStore` must raise. A store that also
        implements `StreamServerStore` has further outcomes (closed stream,
        content-type and Stream-Seq conflicts); those belong to that contract,
        and both current stores share them.

        The envelope — `label` and `metadata` on `options` (an `AppendOptions`) —
        is recorded and read back by `read`; ambient `provenance()` merges under
        explicit metadata.
        """
        ...

    def append_many(self, path: str, events: Iterable[tuple[bytes, Any]]) -> list[Any]:
        """Append an ordered batch of `(data, options)` events, returning one
        result per item in input order.

        Semantically identical to calling `append` once per item — same
        `StreamNotFound`-if-missing guarantee and same per-event envelope handling —
        but a backend may collapse the batch into a single transaction (the
        durable `DjangoStreamStore` does, which is the point). An empty batch is
        a no-op returning `[]`. Result element types stay backend-specific and
        loose, exactly as `append`'s do.
        """
        ...


@runtime_checkable
class StreamServerStore(WritableStore, Protocol):
    """A store that can back a Durable Streams **protocol server**.

    `WritableStore` is what `replay()` and projections need. This is the wider
    surface `rakaia.handler.create_app` needs: everything in `WritableStore`,
    plus the protocol lifecycle — producer epoch/seq fencing, close, the TTL
    sliding window, long-poll, and response formatting.

    Until this existed `create_app` was typed against the concrete in-memory
    `StreamStore`, so the durable store could not back the server no matter
    what it implemented — which is why the Django integration grew a second,
    partial implementation of the protocol instead of reusing this one. Two
    adapters satisfy it: the in-memory `StreamStore` and `DjangoStreamStore`.

    Failures are the named ones in `rakaia.types` (`StreamNotFound`,
    `SequenceConflict`, …); a server maps them to statuses by type, so an
    implementation raising a bare `ValueError` will surface as a 500.

    The shared conformance suite for this surface is
    `tests/server_store_contract.py`.
    """

    def get(self, path: str) -> Any:
        """The stream at `path`, or `None` if absent or expired."""
        ...

    def touch(self, path: str) -> None:
        """Extend the TTL sliding window for `path`. No-op if absent or if the
        stream has no TTL. Used for activity that doesn't otherwise mutate the
        stream, such as a caught-up `GET ?offset=now`."""
        ...

    def delete(self, path: str) -> bool:
        """Delete the stream, cancelling any pending long-polls. Returns whether
        it existed. Offsets stay globally monotonic across delete+recreate: a
        recreated path resumes numbering above the retired high mark."""
        ...

    def format_response(self, path: str, messages: list[Any]) -> bytes:
        """Render `messages` as the response body for `path`.

        Content-type aware: a JSON-mode stream yields one JSON array, anything
        else the concatenated payloads.
        """
        ...

    async def append_with_producer(
        self, path: str, data: bytes, options: Any = None
    ) -> Any:
        """Append under producer fencing, serialized per `(path, producer_id)`.

        The result carries a `ProducerValidationResult` when the write was
        refused (stale epoch, sequence gap, duplicate, closed stream) rather
        than raising — those are protocol outcomes with their own statuses and
        headers, not failures.
        """
        ...

    def close_stream(self, path: str) -> Any:
        """Close `path`, returning a `CloseResult` (or `None` if absent).

        Idempotent: closing an already-closed stream reports the same final
        offset rather than failing.
        """
        ...

    async def close_stream_with_producer(
        self, path: str, producer_id: str, producer_epoch: int, producer_seq: int
    ) -> Any:
        """Close `path` under producer fencing. Same fencing outcomes as
        `append_with_producer`, same idempotence as `close_stream`.

        The tuple is passed as three arguments rather than on an
        `AppendOptions`, unlike `append_with_producer` — a close carries no
        body and no envelope, so there is nothing else for an options object to
        hold. A server calls this only when all three are present.
        """
        ...

    async def wait_for_messages(
        self, path: str, offset: str, timeout_seconds: float
    ) -> tuple[list[Any], bool, bool]:
        """Long-poll: wait up to `timeout_seconds` for messages after `offset`.

        Returns `(messages, timed_out, stream_closed)`. Returns immediately if
        messages are already available or the stream is closed; returns an
        empty list on timeout. Raises `StreamNotFound` if the stream is absent.
        """
        ...


@runtime_checkable
class CursorStore(ReadableStore, Protocol):
    """A `ReadableStore` that also exposes its current head offset — what a
    subscriber (`poll()`) needs to detect new messages and a rewound log."""

    def get_current_offset(self, path: str) -> str | None:
        """The offset after the last message, or None if the stream is absent."""
        ...


@runtime_checkable
class ProjectionReader(Protocol):
    """Read-only view over materialised projections.

    During staged replay, `replay()` passes a reader to every stage > 0 handler
    as its second argument, so the handler can resolve facts that earlier stages
    produced (e.g. link a form to the reference entity another form created).
    `replay()` never calls these methods itself — it only forwards the reader —
    so backends are free to shape it to their storage; the Django integration
    provides one over `apps.get_model(...).objects`. Because a reader only ever
    reads committed projections (themselves a pure function of the log), a
    handler using it stays deterministic.
    """

    def get(self, model_label: str, /, **lookup: Any) -> Any | None:
        """Return the single row matching `lookup`, or None."""
        ...

    def filter(self, model_label: str, /, **lookup: Any) -> Any:
        """Return the rows matching `lookup` (a queryset-like iterable)."""
        ...

    def query(self, model_label: str, /) -> Any:
        """Return all rows of the model (a queryset-like iterable)."""
        ...
