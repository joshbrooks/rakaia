"""
In-memory stream storage for the Durable Streams server.

Provides the StreamStore class that manages stream lifecycle, message storage,
producer validation, long-poll waiting, and TTL/expiry.
"""

from __future__ import annotations

import asyncio
import logging
import re
import time
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from .append_decision import StreamFacts, decide_append
from .context import merge_provenance
from .json_mode import (
    format_json_response,
    is_json_content_type,
    normalize_content_type,
    process_json_append,
)
from .producer import is_producer_state_expired
from .types import (
    INITIAL_OFFSET,
    AppendOptions,
    AppendResult,
    CloseResult,
    ContentTypeMismatch,
    InvalidOffset,
    ProducerAccepted,
    ProducerValidationResult,
    SequenceConflict,
    Stream,
    StreamConfigConflict,
    StreamMessage,
    StreamNotFound,
)

# NB: this module generates offsets (the `{seq}_{byte}` format) but never
# *validates* them, so it deliberately does not import `VALID_OFFSET_PATTERN`.
# Offset validation lives in the protocol server (`handler`), which uses the
# one pattern from `.types` (#41).

_log = logging.getLogger("rakaia.store")

# This store's offsets, and nothing else. See `StreamStore._check_offset`.
_COMPOUND_OFFSET = re.compile(r"^\d+_\d+$")


class StreamStore:
    """
    In-memory store for durable streams.

    Thread-safe via asyncio locks for producer serialization.
    """

    def __init__(self) -> None:
        self._streams: dict[str, Stream] = {}
        self._notify_events: dict[str, asyncio.Event] = {}
        self._producer_locks: dict[str, asyncio.Lock] = {}
        # Highest read_seq (offset generation) ever retired at a path, so a
        # recreate issues offsets strictly greater than any prior one (#34).
        self._retired_seq: dict[str, int] = {}

    # =========================================================================
    # Stream lifecycle
    # =========================================================================

    def _is_expired(self, stream: Stream) -> bool:
        """Check if a stream is expired based on TTL or Expires-At.

        Stream-TTL is a sliding window anchored on ``last_activity_at`` (reset
        on read/write/close). Stream-Expires-At is an absolute deadline that
        does not slide.
        """
        now = time.time()

        if (
            stream.ttl_seconds is not None
            and now - stream.last_activity_at > stream.ttl_seconds
        ):
            return True

        if stream.expires_at is not None:
            try:
                expires = datetime.fromisoformat(
                    stream.expires_at.replace("Z", "+00:00")
                )
                # A naive timestamp is taken as UTC; a parsed offset is kept,
                # not overwritten (`replace(tzinfo=...)` would discard it).
                if expires.tzinfo is None:
                    expires = expires.replace(tzinfo=timezone.utc)
                if now > expires.timestamp():
                    return True
            except ValueError:
                # A malformed Stream-Expires-At can't be evaluated, so the
                # stream is treated as non-expiring. Log it rather than
                # swallowing silently so bad data is observable.
                _log.debug(
                    "Ignoring malformed expires_at %r on stream %r",
                    stream.expires_at,
                    stream.path,
                )

        return False

    def _get_if_not_expired(self, path: str) -> Stream | None:
        """Get a stream, deleting it if expired."""
        stream = self._streams.get(path)
        if stream is None:
            return None
        if self._is_expired(stream):
            self.delete(path)
            return None
        return stream

    def _touch(self, stream: Stream) -> None:
        """Extend the sliding TTL window for a stream (no-op without a TTL)."""
        if stream.ttl_seconds is not None:
            stream.last_activity_at = time.time()

    def touch(self, path: str) -> None:
        """Reset the TTL sliding window for a stream if it exists.

        Called for TTL-extending activity that does not otherwise mutate the
        stream (e.g. a ``GET ?offset=now`` catch-up read).
        """
        stream = self._get_if_not_expired(path)
        if stream is not None:
            self._touch(stream)

    def create(
        self,
        path: str,
        *,
        content_type: str | None = None,
        ttl_seconds: int | None = None,
        expires_at: str | None = None,
        initial_data: bytes | None = None,
        closed: bool = False,
    ) -> Stream:
        """
        Create a new stream.

        If the stream already exists with matching config, returns it (idempotent).

        Raises:
            StreamConfigConflict: If stream exists with different config.
        """
        existing = self._get_if_not_expired(path)
        if existing is not None:
            # Check config match for idempotent create
            ct_matches = normalize_content_type(content_type) == normalize_content_type(
                existing.content_type
            )
            ttl_matches = ttl_seconds == existing.ttl_seconds
            expires_matches = expires_at == existing.expires_at
            closed_matches = closed == existing.closed

            if ct_matches and ttl_matches and expires_matches and closed_matches:
                return existing
            raise StreamConfigConflict(
                f"Stream already exists with different configuration: {path}"
            )

        now = time.time()
        # Recreating a path resumes the read_seq (offset generation) above the
        # one it last retired, so offsets stay globally monotonic across
        # delete+recreate (#34). A never-seen path starts at the canonical
        # INITIAL_OFFSET (generation 0).
        prior = self._retired_seq.get(path)
        initial_offset = (
            INITIAL_OFFSET if prior is None else f"{prior + 1:016d}_{0:016d}"
        )
        stream = Stream(
            path=path,
            content_type=content_type,
            current_offset=initial_offset,
            ttl_seconds=ttl_seconds,
            expires_at=expires_at,
            created_at=now,
            last_activity_at=now,
            closed=closed,
        )

        # If initial data is provided, append it
        if initial_data and len(initial_data) > 0:
            self._append_to_stream(stream, initial_data, is_initial_create=True)

        self._streams[path] = stream
        return stream

    async def run_sync(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Run a synchronous store call for an async server.

        In-memory work is immediate, so this calls straight through — no thread
        hop, no executor. The durable store overrides this because a database
        cannot be touched from an async context.
        """
        return fn(*args, **kwargs)

    def get(self, path: str) -> Stream | None:
        """Get a stream by path. Returns None if not found or expired."""
        return self._get_if_not_expired(path)

    def has(self, path: str) -> bool:
        """Check if a stream exists (and is not expired)."""
        return self._get_if_not_expired(path) is not None

    def delete(self, path: str) -> bool:
        """Delete a stream and cancel any pending long-polls."""
        self._cancel_notify(path)
        if path in self._streams:
            # Remember the retired generation so a recreate at this path
            # resumes above it (globally monotonic offsets, #34).
            read_seq = int(self._streams[path].current_offset.split("_")[0])
            self._retired_seq[path] = max(self._retired_seq.get(path, -1), read_seq)
            del self._streams[path]
            return True
        return False

    def clear(self) -> None:
        """Clear all streams and cancel all pending waits."""
        for path in list(self._notify_events.keys()):
            self._cancel_notify(path)
        self._streams.clear()

    def list_paths(self) -> list[str]:
        """Get all stream paths."""
        return list(self._streams.keys())

    # =========================================================================
    # Append operations
    # =========================================================================

    def append(
        self,
        path: str,
        data: bytes,
        options: AppendOptions | None = None,
    ) -> AppendResult:
        """
        Append data to a stream.

        Handles content-type validation, JSON mode, Stream-Seq coordination,
        producer validation, and stream closure.

        Raises:
            StreamNotFound: If stream doesn't exist or is expired.
            InvalidJson / EmptyJsonArray: If the payload fails JSON-mode checks.
            ContentTypeMismatch / SequenceConflict: On the respective conflict.
        """
        opts = options or AppendOptions()
        stream = self._get_if_not_expired(path)
        if stream is None:
            raise StreamNotFound(f"Stream not found: {path}")

        # Admission is decided in `rakaia.append_decision`, shared with the
        # durable store, so the ordering (closed -> content-type -> fencing ->
        # Stream-Seq) cannot drift between the two. This store's only job here
        # is to supply the facts and then persist the verdict.
        verdict = decide_append(
            StreamFacts(
                closed=stream.closed,
                closed_by=stream.closed_by,
                content_type=stream.content_type,
                last_seq=stream.last_seq,
            ),
            opts,
            producer_state=self._producer_state(stream, opts.producer_id),
            now=time.time(),
        )
        if not verdict.write:
            return AppendResult(
                message=None,
                stream_closed=verdict.stream_closed,
                producer_result=verdict.producer_result,
            )
        producer_result = verdict.producer_result

        # Append the data (may raise for invalid JSON). Provenance is merged
        # here at the public-append boundary — not in _append_to_stream — so a
        # stream's initial-create message is never stamped.
        message = self._append_to_stream(
            stream,
            data,
            label=opts.label,
            metadata=merge_provenance(opts.metadata),
            event_ts=opts.event_ts,
        )

        # === STATE MUTATION (only after successful append) ===

        # A write extends the sliding TTL window.
        self._touch(stream)

        if producer_result is not None:
            self._commit_producer_state(stream, producer_result)

        if opts.seq is not None:
            stream.last_seq = opts.seq

        if opts.close:
            stream.closed = True
            if opts.producer_id is not None:
                from .types import ClosedBy

                stream.closed_by = ClosedBy(
                    producer_id=opts.producer_id,
                    epoch=opts.producer_epoch or 0,
                    seq=opts.producer_seq or 0,
                )
            self._notify_closed(path)

        # Notify long-pollers
        self._notify_waiters(path)

        return AppendResult(
            message=message,
            producer_result=producer_result,
            stream_closed=opts.close,
        )

    def append_many(
        self,
        path: str,
        events: Iterable[tuple[bytes, AppendOptions | None]],
    ) -> list[AppendResult]:
        """Append an ordered batch, returning one ``AppendResult`` per item.

        API parity with :meth:`DjangoStreamStore.append_many` so a consumer can
        call one method against either backend. The in-memory store has no
        per-append transaction cost to amortise, so this simply delegates to
        :meth:`append` per item, preserving every append semantic
        (producer/seq/close validation, TTL touch, long-poll notification). An
        empty batch returns ``[]``.

        A content-type or Stream-Seq conflict anywhere in the batch refuses
        the whole batch before anything is written. The durable store's single
        transaction can only be all-or-nothing on a conflict, and the two
        stores must agree — a plain loop here would leave the prefix written.
        """
        items = list(events)
        if not items:
            return []
        stream = self._get_if_not_expired(path)
        if stream is not None and not stream.closed:
            last_seq = stream.last_seq
            for _data, options in items:
                opts = options or AppendOptions()
                if (
                    opts.content_type
                    and stream.content_type
                    and normalize_content_type(opts.content_type)
                    != normalize_content_type(stream.content_type)
                ):
                    raise ContentTypeMismatch(
                        f"Content-type mismatch: expected "
                        f"{stream.content_type}, got {opts.content_type}"
                    )
                if opts.seq is not None:
                    if last_seq is not None and opts.seq <= last_seq:
                        raise SequenceConflict(
                            f"Sequence conflict: {opts.seq} <= {last_seq}"
                        )
                    last_seq = opts.seq
                if opts.close:
                    # Items after a close observe the closed stream and are
                    # refused, not validated.
                    break
        return [self.append(path, data, options) for data, options in items]

    async def append_with_producer(
        self,
        path: str,
        data: bytes,
        options: AppendOptions | None = None,
    ) -> AppendResult:
        """Append with producer serialization for concurrent request handling."""
        options = options or AppendOptions()
        if not options.producer_id:
            return self.append(path, data, options)

        lock = self._get_producer_lock(path, options.producer_id)
        async with lock:
            return self.append(path, data, options)

    # =========================================================================
    # Close operations
    # =========================================================================

    def close_stream(self, path: str) -> CloseResult | None:
        """
        Close a stream without appending data.

        Returns None if stream doesn't exist.
        """
        stream = self._get_if_not_expired(path)
        if stream is None:
            return None

        already_closed = stream.closed
        stream.closed = True

        # A close extends the sliding TTL window.
        self._touch(stream)

        self._notify_closed(path)

        return CloseResult(
            final_offset=stream.current_offset,
            already_closed=already_closed,
        )

    async def close_stream_with_producer(
        self,
        path: str,
        producer_id: str,
        producer_epoch: int,
        producer_seq: int,
    ) -> CloseResult | None:
        """Close a stream with producer headers for idempotent close."""
        lock = self._get_producer_lock(path, producer_id)
        async with lock:
            stream = self._get_if_not_expired(path)
            if stream is None:
                return None

            # A close is admitted on the same terms as a fenced append with no
            # body: already closed is reported (as a duplicate for a retry of
            # the closing tuple), then the producer is fenced.
            verdict = decide_append(
                StreamFacts(closed=stream.closed, closed_by=stream.closed_by),
                AppendOptions(
                    producer_id=producer_id,
                    producer_epoch=producer_epoch,
                    producer_seq=producer_seq,
                ),
                producer_state=self._producer_state(stream, producer_id),
                now=time.time(),
            )
            if not verdict.write:
                return CloseResult(
                    final_offset=stream.current_offset,
                    already_closed=verdict.stream_closed,
                    producer_result=verdict.producer_result,
                )
            producer_result = verdict.producer_result

            # Commit and close
            self._commit_producer_state(stream, producer_result)
            stream.closed = True

            # A close extends the sliding TTL window.
            self._touch(stream)

            from .types import ClosedBy

            stream.closed_by = ClosedBy(
                producer_id=producer_id, epoch=producer_epoch, seq=producer_seq
            )

            self._notify_closed(path)

            return CloseResult(
                final_offset=stream.current_offset,
                already_closed=False,
                producer_result=producer_result,
            )

    # =========================================================================
    # Read operations
    # =========================================================================

    def read(
        self, path: str, offset: str | None = None
    ) -> tuple[list[StreamMessage], bool]:
        """
        Read messages from a stream starting at the given offset.

        Returns (messages, up_to_date).

        Raises:
            StreamNotFound: If stream doesn't exist or is expired.
        """
        stream = self._get_if_not_expired(path)
        if stream is None:
            raise StreamNotFound(f"Stream not found: {path}")

        # A read extends the sliding TTL window.
        self._touch(stream)

        if not offset or offset == "-1":
            return list(stream.messages), True

        self._check_offset(offset)

        # Find messages after the given offset (lexicographic comparison)
        idx = self._find_offset_index(stream, offset)
        if idx == -1:
            return [], True

        return stream.messages[idx:], True

    def format_response(self, path: str, messages: list[StreamMessage]) -> bytes:
        """
        Format messages for HTTP response.
        For JSON mode, wraps concatenated data in array brackets.
        """
        stream = self._get_if_not_expired(path)
        if stream is None:
            raise StreamNotFound(f"Stream not found: {path}")

        # Concatenate all message data
        concatenated = b"".join(m.data for m in messages)

        if is_json_content_type(stream.content_type):
            return format_json_response(concatenated)

        return concatenated

    async def wait_for_messages(
        self,
        path: str,
        offset: str,
        timeout_seconds: float,
    ) -> tuple[list[StreamMessage], bool, bool]:
        """
        Wait for new messages (long-poll).

        Returns (messages, timed_out, stream_closed).

        Raises:
            StreamNotFound: If stream doesn't exist.
        """
        stream = self._get_if_not_expired(path)
        if stream is None:
            raise StreamNotFound(f"Stream not found: {path}")

        # Check for existing messages first
        messages, _ = self.read(path, offset)
        if len(messages) > 0:
            return messages, False, False

        # If closed and at tail, return immediately
        if stream.closed and offset == stream.current_offset:
            return [], False, True

        # Wait for notification
        event = self._get_or_create_notify_event(path)
        try:
            await asyncio.wait_for(self._wait_for_event(event), timeout=timeout_seconds)
        except asyncio.TimeoutError:
            # Check if stream was closed during wait
            current = self._get_if_not_expired(path)
            closed = current.closed if current else False
            return [], True, closed

        # Re-read after notification
        try:
            messages, _ = self.read(path, offset)
        except KeyError:
            return [], False, False

        # Check closure
        current = self._get_if_not_expired(path)
        closed = current is not None and current.closed and len(messages) == 0

        return messages, False, closed

    def get_current_offset(self, path: str) -> str | None:
        """Get the current offset for a stream."""
        stream = self._get_if_not_expired(path)
        return stream.current_offset if stream else None

    # =========================================================================
    # Producer validation
    # =========================================================================

    def _producer_state(self, stream: Stream, producer_id: str | None):
        """The last known state for `producer_id`, or None.

        The one fact `decide_append` cannot work out for itself, since only the
        store knows where producer state lives. Expired states are dropped
        first, so an aged-out producer reads as a new one.
        """
        if producer_id is None:
            return None
        self._cleanup_expired_producers(stream)
        return stream.producers.get(producer_id)

    def _commit_producer_state(
        self, stream: Stream, result: ProducerValidationResult
    ) -> None:
        """Commit producer state after successful append."""
        if not isinstance(result, ProducerAccepted):
            return
        if result.proposed_state is not None:
            stream.producers[result.producer_id] = result.proposed_state

    def _cleanup_expired_producers(self, stream: Stream) -> None:
        """Clean up expired producer states."""
        now = time.time()
        expired = [
            pid
            for pid, state in stream.producers.items()
            if is_producer_state_expired(state, now)
        ]
        for pid in expired:
            del stream.producers[pid]

    # =========================================================================
    # Internal helpers
    # =========================================================================

    def _append_to_stream(
        self,
        stream: Stream,
        data: bytes,
        is_initial_create: bool = False,
        *,
        label: str = "",
        metadata: dict | None = None,
        event_ts: float | None = None,
    ) -> StreamMessage | None:
        """Append data to a stream, handling JSON mode processing."""
        processed_data = data
        if is_json_content_type(stream.content_type):
            processed_data = process_json_append(data, is_initial_create)
            if len(processed_data) == 0:
                return None

        # Parse current offset and calculate new one
        parts = stream.current_offset.split("_")
        read_seq = int(parts[0])
        byte_offset = int(parts[1])

        new_byte_offset = byte_offset + len(processed_data)
        # Offsets are `{read_seq}_{byte_offset}`, each zero-padded to 16 digits
        # so they sort byte-wise lexicographically (the protocol's requirement,
        # and what keeps offsets monotonic across recreate). `:016d` is a minimum
        # width, not a cap: the ordering guarantee holds only while both fields
        # stay < 10**16. That bound is unreachable here — the byte offset is
        # capped by the process's memory (this store holds every message in RAM,
        # so 10**16 bytes = ~10 PB is impossible) and 10**16 recreations of one
        # path equally so. A durable backend that could exceed it must widen the
        # fields (and INITIAL_OFFSET) in lockstep.
        new_offset = f"{read_seq:016d}_{new_byte_offset:016d}"

        # Envelope timestamp defaults to append time when the producer didn't set
        # a logical one, so `event_ts` is always populated after a store append.
        append_time = time.time()
        message = StreamMessage(
            data=processed_data,
            offset=new_offset,
            timestamp=append_time,
            event_ts=event_ts if event_ts is not None else append_time,
            label=label,
            metadata=metadata,
        )

        stream.messages.append(message)
        stream.current_offset = new_offset

        return message

    @staticmethod
    def _check_offset(offset: str) -> None:
        """Reject an offset this store did not issue.

        Offsets here are compound `{seq}_{byte}` tokens and are compared
        lexicographically, which quietly does the wrong thing with a foreign
        one: the durable store's plain integer sorts *above* every offset this
        store emits, so a resume read returns nothing and the client is told it
        is up to date, having skipped the whole stream. The protocol makes
        offsets opaque rather than uniform (§6), so only the issuing store can
        judge — `VALID_OFFSET_PATTERN` accepts both formats by design.
        """
        if not _COMPOUND_OFFSET.match(offset):
            raise InvalidOffset(
                f"Not an offset this store issued: {offset!r}. In-memory-store "
                f"offsets have the compound {{seq}}_{{byte}} form."
            )

    def _find_offset_index(self, stream: Stream, offset: str) -> int:
        """Find first message with offset > given offset (lexicographic)."""
        for i, msg in enumerate(stream.messages):
            if msg.offset > offset:
                return i
        return -1

    def _get_producer_lock(self, path: str, producer_id: str) -> asyncio.Lock:
        """Get or create a lock for serialized producer operations."""
        key = f"{path}:{producer_id}"
        if key not in self._producer_locks:
            self._producer_locks[key] = asyncio.Lock()
        return self._producer_locks[key]

    # =========================================================================
    # Long-poll notification
    # =========================================================================

    def _get_or_create_notify_event(self, path: str) -> asyncio.Event:
        """Get or create an asyncio.Event for long-poll notification."""
        if path not in self._notify_events:
            self._notify_events[path] = asyncio.Event()
        return self._notify_events[path]

    def _notify_waiters(self, path: str) -> None:
        """Notify all waiters for a stream that new data is available."""
        event = self._notify_events.get(path)
        if event:
            event.set()
            # Reset for next wait cycle
            self._notify_events[path] = asyncio.Event()

    def _notify_closed(self, path: str) -> None:
        """Notify waiters that a stream has been closed."""
        self._notify_waiters(path)

    def _cancel_notify(self, path: str) -> None:
        """Cancel notification for a stream (on delete)."""
        event = self._notify_events.pop(path, None)
        if event:
            event.set()

    async def _wait_for_event(self, event: asyncio.Event) -> None:
        """Wait for an asyncio.Event to be set."""
        await event.wait()
