"""
In-memory stream storage for the Durable Streams server.

Provides the StreamStore class that manages stream lifecycle, message storage,
producer validation, long-poll waiting, and TTL/expiry.
"""

from __future__ import annotations

import asyncio
import re
import time
from datetime import datetime, timezone

from .json_mode import (
    format_json_response,
    is_json_content_type,
    normalize_content_type,
    process_json_append,
)
from .types import (
    INITIAL_OFFSET,
    PRODUCER_STATE_TTL_SECONDS,
    AppendOptions,
    AppendResult,
    CloseResult,
    ProducerAccepted,
    ProducerDuplicate,
    ProducerInvalidEpochSeq,
    ProducerSequenceGap,
    ProducerStaleEpoch,
    ProducerStreamClosed,
    ProducerValidationResult,
    Stream,
    StreamMessage,
)

# Valid offset pattern: exactly digits_digits, or sentinel -1, or now
VALID_OFFSET_PATTERN = re.compile(r"^(-1|now|\d+_\d+)$")


class StreamStore:
    """
    In-memory store for durable streams.

    Thread-safe via asyncio locks for producer serialization.
    """

    def __init__(self) -> None:
        self._streams: dict[str, Stream] = {}
        self._notify_events: dict[str, asyncio.Event] = {}
        self._producer_locks: dict[str, asyncio.Lock] = {}

    # =========================================================================
    # Stream lifecycle
    # =========================================================================

    def _is_expired(self, stream: Stream) -> bool:
        """Check if a stream is expired based on TTL or Expires-At."""
        now = time.time()

        if (
            stream.ttl_seconds is not None
            and now - stream.created_at > stream.ttl_seconds
        ):
            return True

        if stream.expires_at is not None:
            try:
                expires = datetime.fromisoformat(
                    stream.expires_at.replace("Z", "+00:00")
                )
                if now > expires.replace(tzinfo=timezone.utc).timestamp():
                    return True
            except ValueError:
                pass

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
            ValueError: If stream exists with different config.
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
            raise ValueError(
                f"Stream already exists with different configuration: {path}"
            )

        stream = Stream(
            path=path,
            content_type=content_type,
            current_offset=INITIAL_OFFSET,
            ttl_seconds=ttl_seconds,
            expires_at=expires_at,
            created_at=time.time(),
            closed=closed,
        )

        # If initial data is provided, append it
        if initial_data and len(initial_data) > 0:
            self._append_to_stream(stream, initial_data, is_initial_create=True)

        self._streams[path] = stream
        return stream

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
            KeyError: If stream doesn't exist or is expired.
            ValueError: If JSON is invalid, content-type mismatches, or seq conflict.
        """
        opts = options or AppendOptions()
        stream = self._get_if_not_expired(path)
        if stream is None:
            raise KeyError(f"Stream not found: {path}")

        # Check if stream is closed
        if stream.closed:
            # Check for idempotent duplicate of closing request
            if (
                opts.producer_id is not None
                and stream.closed_by is not None
                and stream.closed_by.producer_id == opts.producer_id
                and stream.closed_by.epoch == opts.producer_epoch
                and stream.closed_by.seq == opts.producer_seq
            ):
                return AppendResult(
                    message=None,
                    stream_closed=True,
                    producer_result=ProducerDuplicate(last_seq=opts.producer_seq or 0),
                )
            # Stream is closed - reject
            return AppendResult(message=None, stream_closed=True)

        # Check content type match
        if opts.content_type and stream.content_type:
            provided = normalize_content_type(opts.content_type)
            stream_ct = normalize_content_type(stream.content_type)
            if provided != stream_ct:
                raise ValueError(
                    f"Content-type mismatch: expected {stream.content_type}, "
                    f"got {opts.content_type}"
                )

        # Producer validation (before Stream-Seq check for retry dedup)
        producer_result: ProducerValidationResult | None = None
        if (
            opts.producer_id is not None
            and opts.producer_epoch is not None
            and opts.producer_seq is not None
        ):
            producer_result = self._validate_producer(
                stream, opts.producer_id, opts.producer_epoch, opts.producer_seq
            )
            if producer_result.status != "accepted":
                return AppendResult(message=None, producer_result=producer_result)

        # Stream-Seq coordination
        if (
            opts.seq is not None
            and stream.last_seq is not None
            and opts.seq <= stream.last_seq
        ):
            raise ValueError(f"Sequence conflict: {opts.seq} <= {stream.last_seq}")

        # Append the data (may raise for invalid JSON)
        message = self._append_to_stream(stream, data)

        # === STATE MUTATION (only after successful append) ===

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

    async def append_with_producer(
        self,
        path: str,
        data: bytes,
        options: AppendOptions,
    ) -> AppendResult:
        """Append with producer serialization for concurrent request handling."""
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

            if stream.closed:
                # Check for duplicate (same producer tuple)
                if (
                    stream.closed_by is not None
                    and stream.closed_by.producer_id == producer_id
                    and stream.closed_by.epoch == producer_epoch
                    and stream.closed_by.seq == producer_seq
                ):
                    return CloseResult(
                        final_offset=stream.current_offset,
                        already_closed=True,
                        producer_result=ProducerDuplicate(last_seq=producer_seq),
                    )
                return CloseResult(
                    final_offset=stream.current_offset,
                    already_closed=True,
                    producer_result=ProducerStreamClosed(),
                )

            # Validate producer
            producer_result = self._validate_producer(
                stream, producer_id, producer_epoch, producer_seq
            )

            if producer_result.status != "accepted":
                return CloseResult(
                    final_offset=stream.current_offset,
                    already_closed=False,
                    producer_result=producer_result,
                )

            # Commit and close
            self._commit_producer_state(stream, producer_result)
            stream.closed = True
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
            KeyError: If stream doesn't exist or is expired.
        """
        stream = self._get_if_not_expired(path)
        if stream is None:
            raise KeyError(f"Stream not found: {path}")

        if not offset or offset == "-1":
            return list(stream.messages), True

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
            raise KeyError(f"Stream not found: {path}")

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
            KeyError: If stream doesn't exist.
        """
        stream = self._get_if_not_expired(path)
        if stream is None:
            raise KeyError(f"Stream not found: {path}")

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

    def _validate_producer(
        self,
        stream: Stream,
        producer_id: str,
        epoch: int,
        seq: int,
    ) -> ProducerValidationResult:
        """
        Validate producer state WITHOUT mutating.

        Returns proposed state to commit after successful append.
        """
        # Clean up expired producers
        self._cleanup_expired_producers(stream)

        now = time.time()
        state = stream.producers.get(producer_id)

        # New producer
        if state is None:
            if seq != 0:
                return ProducerSequenceGap(expected_seq=0, received_seq=seq)
            from .types import ProducerState

            return ProducerAccepted(
                is_new=True,
                producer_id=producer_id,
                proposed_state=ProducerState(epoch=epoch, last_seq=0, last_updated=now),
            )

        # Epoch validation
        if epoch < state.epoch:
            return ProducerStaleEpoch(current_epoch=state.epoch)

        if epoch > state.epoch:
            if seq != 0:
                return ProducerInvalidEpochSeq()
            from .types import ProducerState

            return ProducerAccepted(
                is_new=True,
                producer_id=producer_id,
                proposed_state=ProducerState(epoch=epoch, last_seq=0, last_updated=now),
            )

        # Same epoch: sequence validation
        if seq <= state.last_seq:
            return ProducerDuplicate(last_seq=state.last_seq)

        if seq == state.last_seq + 1:
            from .types import ProducerState

            return ProducerAccepted(
                is_new=False,
                producer_id=producer_id,
                proposed_state=ProducerState(
                    epoch=epoch, last_seq=seq, last_updated=now
                ),
            )

        # Sequence gap
        return ProducerSequenceGap(expected_seq=state.last_seq + 1, received_seq=seq)

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
            if now - state.last_updated > PRODUCER_STATE_TTL_SECONDS
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
        new_offset = f"{read_seq:016d}_{new_byte_offset:016d}"

        message = StreamMessage(
            data=processed_data,
            offset=new_offset,
            timestamp=time.time(),
        )

        stream.messages.append(message)
        stream.current_offset = new_offset

        return message

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
