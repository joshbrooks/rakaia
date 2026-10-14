"""
In-memory stream storage for the Durable Streams server.

Provides the StreamStore class that manages stream lifecycle, message storage,
producer validation, long-poll waiting, and TTL/expiry.
"""

from __future__ import annotations

import asyncio
import logging
import time
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from .append_decision import (
    AppendPlan,
    PlannedWrite,
    StreamFacts,
    decide_append,
    plan_appends,
    plan_initial_write,
)
from .json_mode import (
    is_json_content_type,
    normalize_content_type,
)
from .offsets import COMPOUND
from .producer import is_producer_state_expired
from .read_decision import page_of
from .types import (
    AppendOptions,
    AppendResult,
    CloseResult,
    ProducerAccepted,
    ProducerValidationResult,
    Stream,
    StreamConfigConflict,
    StreamMessage,
    StreamNotFound,
)

# This module neither renders nor validates the offset format itself: both are
# `COMPOUND`'s job (`rakaia.offsets`). It still does not use
# `offsets.is_syntactically_valid` — that is the protocol server's guard on what
# a *client* may send, and it refuses only what no store could have issued, so it
# cannot tell a foreign offset from one of ours.

_log = logging.getLogger("rakaia.store")


class StreamStore:
    """
    In-memory store for durable streams.

    Not thread-safe, and does not need to be: its `run_sync` calls straight
    through, so the protocol server runs every call on the event loop, and a
    call has no `await` inside it for another to interleave at.
    """

    def __init__(self) -> None:
        self._streams: dict[str, Stream] = {}
        # The messages themselves, keyed by path. They live here rather than on
        # `Stream` because `Stream` is the *metadata* `get()` hands back, and a
        # durable store's `Stream` carries no messages at all — the field was
        # permanently empty there, which made it a lie in the shared type.
        self._messages: dict[str, list[StreamMessage]] = {}
        self._notify_events: dict[str, asyncio.Event] = {}
        # Highest read_seq (offset generation) ever retired at a path, so a
        # recreate issues offsets strictly greater than any prior one (#34).
        self._retired_seq: dict[str, int] = {}
        # The last `event_ts` this store stamped on each path, so its own stamps
        # never go backwards within a stream (#284). Kept beside the stream
        # rather than on it, and not cleared on delete, for the same reason as
        # `_retired_seq`: the durable store keeps the equivalent on its offset
        # watermark, which outlives the stream, so a recreated path carries on.
        self._last_event_ts: dict[str, float] = {}

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
            COMPOUND.first() if prior is None else COMPOUND.render(prior + 1, 0)
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

        if initial_data:
            self._write_plan(
                stream, plan_initial_write(stream.content_type, initial_data)
            )

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
            self._messages.pop(path, None)
            return True
        return False

    def check_protocol_delete(self, path: str) -> None:
        """Every stream here may be deleted over the protocol."""

    def clear(self) -> None:
        """Clear all streams and cancel all pending waits."""
        for path in list(self._notify_events.keys()):
            self._cancel_notify(path)
        self._streams.clear()
        self._messages.clear()

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
        Append data to a stream: a batch of one.

        Raises:
            StreamNotFound: If stream doesn't exist or is expired.
            InvalidJson / EmptyJsonArray: If the payload fails JSON-mode checks.
            ContentTypeMismatch / SequenceConflict: On the respective conflict.
        """
        return self.append_many(path, [(data, options)])[0]

    def append_many(
        self,
        path: str,
        events: Iterable[tuple[bytes, AppendOptions | None]],
    ) -> list[AppendResult]:
        """Append an ordered batch, returning one ``AppendResult`` per item.

        What each item is allowed to do and what it writes are decided by
        `rakaia.append_decision.plan_appends`, shared with the other two stores,
        before anything is stored — so a refusal anywhere in the batch (a
        conflict, a body the stream cannot hold, a bad tag) refuses the whole
        batch with nothing written, as the durable store's single transaction
        does. This store renders the offsets and keeps the result. An empty
        batch returns ``[]``.
        """
        items = list(events)
        if not items:
            return []
        stream = self._get_if_not_expired(path)
        if stream is None:
            raise StreamNotFound(f"Stream not found: {path}")
        producer_ids = {
            pid
            for _d, o in items
            if (pid := getattr(o, "producer_id", None)) is not None
        }
        plan = plan_appends(
            StreamFacts(
                closed=stream.closed,
                closed_by=stream.closed_by,
                content_type=stream.content_type,
                last_seq=stream.last_seq,
            ),
            [options for _data, options in items],
            payloads=[data for data, _options in items],
            producer_states={
                pid: self._producer_state(stream, pid) for pid in producer_ids
            },
            now=time.time(),
        )
        messages = self._write_plan(stream, plan)
        if plan.writes_anything:
            # A write extends the sliding TTL window.
            self._touch(stream)
        for accepted in plan.producer_commits.values():
            self._commit_producer_state(stream, accepted)
        stream.last_seq = plan.last_seq
        if plan.close:
            stream.closed = True
            stream.closed_by = plan.closed_by
        if plan.writes_anything:
            self._notify_waiters(path)

        return [
            AppendResult(
                message=message,
                stream_closed=write.close if write else verdict.stream_closed,
                producer_result=verdict.producer_result,
            )
            for verdict, write, message in zip(
                plan.verdicts, plan.writes, messages, strict=True
            )
        ]

    # =========================================================================
    # Close operations
    # =========================================================================

    def close_stream(
        self,
        path: str,
        *,
        producer_id: str | None = None,
        producer_epoch: int | None = None,
        producer_seq: int | None = None,
    ) -> CloseResult | None:
        """
        Close a stream without appending data, fenced when all three producer
        arguments are given.

        Returns None if stream doesn't exist.
        """
        stream = self._get_if_not_expired(path)
        if stream is None:
            return None

        if producer_id is None or producer_epoch is None or producer_seq is None:
            already_closed = stream.closed
            stream.closed = True

            # A close extends the sliding TTL window.
            self._touch(stream)

            self._notify_closed(path)

            return CloseResult(
                final_offset=stream.current_offset,
                already_closed=already_closed,
            )

        # A close is admitted on the same terms as a fenced append with no
        # body: already closed is reported (as a duplicate for a retry of the
        # closing tuple), then the producer is fenced.
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
        # `decide_append` leaves `producer_result` unset only when no producer
        # tuple was supplied, and this branch has all three, so fencing always
        # ran and a verdict always exists.
        assert producer_result is not None

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
        self, path: str, offset: str | None = None, *, limit: int | None = None
    ) -> tuple[list[StreamMessage], bool]:
        """
        Read messages from a stream starting at the given offset.

        Returns (messages, up_to_date). With a `limit`, at most that many
        messages, and `up_to_date` false when more remain (see
        `StreamServerStore.read`).

        Raises:
            StreamNotFound: If stream doesn't exist or is expired.
        """
        stream = self._get_if_not_expired(path)
        if stream is None:
            raise StreamNotFound(f"Stream not found: {path}")

        # A read extends the sliding TTL window.
        self._touch(stream)

        return page_of(self._messages_after(stream, offset), limit)

    def peek(self, path: str, offset: str | None) -> list[StreamMessage]:
        """`read` without extending the TTL window; see `StreamServerStore.peek`."""
        stream = self._get_if_not_expired(path)
        if stream is None:
            raise StreamNotFound(f"Stream not found: {path}")
        return self._messages_after(stream, offset)

    def _messages_after(
        self, stream: Stream, offset: str | None
    ) -> list[StreamMessage]:
        messages = self._messages.get(stream.path, [])
        if not offset or offset == "-1":
            return list(messages)

        self._check_offset(offset)

        # Find messages after the given offset (lexicographic comparison)
        idx = self._find_offset_index(stream, offset)
        if idx == -1:
            return []

        return messages[idx:]

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

    def _write_plan(
        self, stream: Stream, plan: AppendPlan
    ) -> list[StreamMessage | None]:
        """Store the plan's writes, returning per item the last message it
        became — the stream's new head, which is what a caller resumes from —
        or None for an item that writes nothing."""
        writes, stamp = plan.stamped(self._last_event_ts.get(stream.path), time.time())
        if stamp is not None:
            self._last_event_ts[stream.path] = stamp
        return [self._store(stream, w) if w is not None else None for w in writes]

    def _store(self, stream: Stream, write: PlannedWrite) -> StreamMessage | None:
        json_mode = is_json_content_type(stream.content_type)
        parts = stream.current_offset.split("_")
        read_seq = int(parts[0])
        byte_offset = int(parts[1])
        append_time = time.time()
        message: StreamMessage | None = None
        for payload in write.payloads:
            # A JSON-mode offset is a position in the *response* body, where the
            # payloads are joined by commas — so each message advances the byte
            # offset by its own length plus the separator that will sit after it.
            # Storing the payload unframed (#155) therefore leaves every offset
            # exactly where it was; only `StreamMessage.data` changes.
            byte_offset += len(payload) + (1 if json_mode else 0)
            new_offset = COMPOUND.render(read_seq, byte_offset)
            message = StreamMessage(
                data=payload,
                offset=new_offset,
                timestamp=append_time,
                event_ts=write.event_ts,
                label=write.label,
                metadata=write.metadata,
                tags=write.tags,
                correlation_id=write.correlation_id,
            )
            self._messages.setdefault(stream.path, []).append(message)
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
        judge — the server's guard refuses only what no store could have issued.
        """
        COMPOUND.require(offset)

    def _find_offset_index(self, stream: Stream, offset: str) -> int:
        """Find first message with offset > given offset (lexicographic)."""
        for i, msg in enumerate(self._messages.get(stream.path, [])):
            if msg.offset > offset:
                return i
        return -1

    # =========================================================================
    # Long-poll notification
    # =========================================================================

    def new_messages_event(self, path: str) -> asyncio.Event:
        """An event set on the next append, close or delete at `path`; see
        `SignalsNewMessages`."""
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
