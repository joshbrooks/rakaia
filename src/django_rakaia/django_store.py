"""
Durable, database-backed stream store.

`DjangoStreamStore` implements the read/emit surface of the in-memory
`rakaia.StreamStore` on top of the normalized `Stream` / `StreamEvent` /
`StreamEntry` models. Unlike the in-memory store it survives process restarts,
so you can emit events from a request (e.g. `Submission.save()`) and replay the
stream later, in another process — the adoption path that the in-memory store
cannot support.

It is **JSON-oriented**: `append` expects JSON-encodable bytes and stores the
decoded object in `StreamEvent.data` (a `JSONField`); `read` re-encodes it to
bytes. `replay()` only needs the decoded event, so an exact byte round-trip is
not required.

Scope: the whole `rakaia.StreamServerStore` surface. As well as the
event-sourcing read/emit path, it implements the protocol lifecycle — producer
epoch/seq fencing, close, the TTL sliding window, long-poll and response
formatting — so `rakaia.create_app` can serve the Durable Streams protocol
directly off the database. That is what removed the need for a second,
partial implementation of the protocol in the Django integration.

Both stores are held to one shared conformance suite
(`tests/server_store_contract.py`), and the fencing rules themselves are a
single pure module (`rakaia.producer`) that both call, so the two cannot drift.
"""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Iterable
from datetime import datetime, timezone
from typing import Any

from asgiref.sync import sync_to_async
from django.db import transaction
from django.db.models import Max

from rakaia.context import merge_provenance
from rakaia.json_mode import (
    format_json_response,
    is_json_content_type,
    normalize_content_type,
    process_json_append,
)
from rakaia.producer import validate_producer
from rakaia.types import (
    AppendOptions,
    AppendResult,
    CloseResult,
    ContentTypeMismatch,
    ProducerAccepted,
    ProducerState,
    ProducerStreamClosed,
    ProducerValidationResult,
    SequenceConflict,
    StreamConfigConflict,
    StreamMessage,
    StreamNotFound,
)

from .models import (
    Stream,
    StreamEntry,
    StreamEvent,
    StreamOffsetWatermark,
    StreamProducer,
)

# StreamEvent.event_type is required metadata for the dashboard; raw stream
# appends carry no type, so they are recorded under a single stable label.
_APPEND_EVENT_TYPE = "append"

# Offsets are rendered zero-padded so they sort byte-wise lexicographically, as
# the Durable Streams protocol requires (§3, §5.2). 20 digits covers a
# BigAutoField's range (< 2**63). `read` still parses them numerically, so the
# padding is transparent to filtering.
_OFFSET_WIDTH = 20


def format_offset(value: int) -> str:
    """Render an integer offset as the protocol's opaque, sortable string."""
    return f"{value:0{_OFFSET_WIDTH}d}"


# Long-poll poll interval. The in-memory store wakes waiters with an
# asyncio.Event, which only works in-process; a durable stream can be appended
# to by another process entirely, so this store polls instead. 50ms keeps
# catch-up latency well under the protocol's expectations without making an
# idle waiter expensive.
_POLL_INTERVAL_SECONDS = 0.05


class DjangoStreamStore:
    """A durable store backed by the django_rakaia ORM models.

    Satisfies `rakaia.StreamServerStore`: the event-sourcing read/emit surface
    plus the full protocol lifecycle.
    """

    # =========================================================================
    # Expiry
    # =========================================================================

    @staticmethod
    def _is_expired(stream: Stream) -> bool:
        """Whether the stream has aged out, by sliding TTL or absolute expiry.

        Mirrors the in-memory store, including its treatment of a malformed
        `expires_at` as non-expiring rather than as immediately expired.
        """
        now = time.time()

        if (
            stream.ttl_seconds is not None
            and now - stream.last_activity_at > stream.ttl_seconds
        ):
            return True

        if stream.expires_at:
            try:
                expires = datetime.fromisoformat(
                    stream.expires_at.replace("Z", "+00:00")
                )
            except ValueError:
                return False
            if now > expires.replace(tzinfo=timezone.utc).timestamp():
                return True

        return False

    def _get_if_not_expired(self, path: str) -> Stream | None:
        """The live stream at `path`, deleting it first if it has expired."""
        stream = Stream.objects.filter(stream_id=path).first()
        if stream is None:
            return None
        if self._is_expired(stream):
            self.delete(path)
            return None
        return stream

    def _require(self, path: str) -> Stream:
        stream = self._get_if_not_expired(path)
        if stream is None:
            raise StreamNotFound(f"Stream not found: {path}")
        return stream

    @staticmethod
    def _touch(stream: Stream) -> None:
        """Extend the sliding TTL window. No-op for a stream without a TTL."""
        if stream.ttl_seconds is not None:
            stream.last_activity_at = time.time()
            stream.save(update_fields=["last_activity_at"])

    def touch(self, path: str) -> None:
        stream = self._get_if_not_expired(path)
        if stream is not None:
            self._touch(stream)

    # =========================================================================
    # Lifecycle
    # =========================================================================

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
        """Create the stream, idempotently.

        Re-creating with the *same* configuration returns the existing stream;
        with a different one it raises `StreamConfigConflict`, matching the
        in-memory store. (Before the lifecycle columns existed this store had
        nowhere to record a configuration, so it accepted any re-create
        silently — that divergence is now closed.)
        """
        existing = self._get_if_not_expired(path)
        if existing is not None:
            same = (
                normalize_content_type(content_type)
                == normalize_content_type(existing.content_type)
                and ttl_seconds == existing.ttl_seconds
                and expires_at == existing.expires_at
                and closed == existing.closed
            )
            if same:
                return existing
            raise StreamConfigConflict(
                f"Stream already exists with different configuration: {path}"
            )

        now = time.time()
        stream = Stream.objects.create(
            stream_id=path,
            content_type=content_type,
            ttl_seconds=ttl_seconds,
            expires_at=expires_at,
            last_activity_at=now,
            closed=closed,
        )
        if initial_data:
            self._write(stream, initial_data, AppendOptions(), is_initial_create=True)
        return stream

    def close_stream(self, path: str) -> CloseResult | None:
        """Close the stream. `None` if it does not exist.

        Idempotent: closing an already-closed stream reports the same final
        offset with `already_closed=True` rather than failing.
        """
        stream = self._get_if_not_expired(path)
        if stream is None:
            return None
        if stream.closed:
            return CloseResult(final_offset=stream.current_offset, already_closed=True)
        stream.closed = True
        stream.closed_at = time.time()
        stream.last_activity_at = time.time()
        stream.save(update_fields=["closed", "closed_at", "last_activity_at"])
        return CloseResult(final_offset=stream.current_offset)

    async def close_stream_with_producer(
        self,
        path: str,
        producer_id: str | None = None,
        producer_epoch: int | None = None,
        producer_seq: int | None = None,
    ) -> CloseResult | None:
        """Close under producer fencing.

        A retry of the *same* closing tuple reports `already_closed` instead of
        being refused as a duplicate, so a producer that loses the response can
        safely repeat its close.
        """
        if producer_id is None or producer_epoch is None or producer_seq is None:
            return await sync_to_async(self.close_stream)(path)
        return await sync_to_async(self._close_with_producer_sync)(
            path, producer_id, producer_epoch, producer_seq
        )

    def _close_with_producer_sync(
        self, path: str, producer_id: str, epoch: int, seq: int
    ) -> CloseResult | None:
        with transaction.atomic():
            stream = self._get_if_not_expired(path)
            if stream is None:
                return None

            if stream.closed:
                by = stream.closed_by
                if by is not None and (by.producer_id, by.epoch, by.seq) == (
                    producer_id,
                    epoch,
                    seq,
                ):
                    return CloseResult(
                        final_offset=stream.current_offset, already_closed=True
                    )

            result = self._validate_producer(stream, producer_id, epoch, seq)
            if not isinstance(result, ProducerAccepted):
                return CloseResult(
                    final_offset=stream.current_offset, producer_result=result
                )

            self._commit_producer(stream, result)
            stream.closed = True
            stream.closed_at = time.time()
            stream.last_activity_at = time.time()
            stream.closed_by_producer_id = producer_id
            stream.closed_by_epoch = epoch
            stream.closed_by_seq = seq
            stream.save(
                update_fields=[
                    "closed",
                    "closed_at",
                    "last_activity_at",
                    "closed_by_producer_id",
                    "closed_by_epoch",
                    "closed_by_seq",
                ]
            )
            return CloseResult(final_offset=stream.current_offset)

    def append(self, path: str, data: bytes, options: Any = None) -> AppendResult:
        """Append one event, assigning the next monotonic offset.

        Returns an `AppendResult`, matching the in-memory store — this is what
        lets one protocol server run on either. (It used to return the
        `StreamEntry` row; the row is still reachable via `read`.)

        The event-sourcing envelope on `options` (an `AppendOptions`) is
        persisted: `label` maps to `event_type` (a raw append keeps the stable
        `"append"` label) and `metadata` to the JSON column.

        Runs in a transaction so `get_next_offset()` can lock the stream row:
        concurrent appends serialize on offset allocation instead of racing to
        the same value and failing `unique_together(stream, offset)`.

        Outcomes, all now matching the in-memory store:

        - Raises `StreamNotFound` if the stream is absent or expired.
        - Returns `AppendResult(stream_closed=True, message=None)` if closed.
        - Raises `ContentTypeMismatch` / `SequenceConflict` / `InvalidJson` /
          `EmptyJsonArray` for the corresponding failures.
        """
        opts = options if options is not None else AppendOptions()
        with transaction.atomic():
            stream = self._require(path)

            if stream.closed:
                return AppendResult(message=None, stream_closed=True)

            self._check_content_type(stream, opts)
            self._check_seq(stream, opts)

            message = self._write(stream, data, opts)

            if getattr(opts, "seq", None) is not None:
                stream.last_seq = opts.seq
                stream.save(update_fields=["last_seq"])
            self._touch(stream)
            return AppendResult(message=message)

    # =========================================================================
    # Append helpers
    # =========================================================================

    @staticmethod
    def _check_content_type(stream: Stream, opts: Any) -> None:
        provided = getattr(opts, "content_type", None)
        if (
            provided
            and stream.content_type
            and normalize_content_type(provided)
            != normalize_content_type(stream.content_type)
        ):
            raise ContentTypeMismatch(
                f"Content-type mismatch: expected {stream.content_type}, got {provided}"
            )

    @staticmethod
    def _check_seq(stream: Stream, opts: Any) -> None:
        seq = getattr(opts, "seq", None)
        if seq is not None and stream.last_seq is not None and seq <= stream.last_seq:
            raise SequenceConflict(f"Sequence conflict: {seq} <= {stream.last_seq}")

    def _write(
        self,
        stream: Stream,
        data: bytes,
        opts: Any,
        *,
        is_initial_create: bool = False,
    ) -> StreamMessage:
        """Persist one event and return it in protocol shape.

        In JSON mode the payload is validated first, so a malformed body fails
        before anything is written.
        """
        if is_json_content_type(stream.content_type):
            process_json_append(data, is_initial_create=is_initial_create)

        label = getattr(opts, "label", "") or ""
        metadata = merge_provenance(getattr(opts, "metadata", None))
        event_ts = getattr(opts, "event_ts", None)

        event = StreamEvent.objects.create(
            data=json.loads(data),
            event_type=label or _APPEND_EVENT_TYPE,
            metadata=metadata or {},
            event_ts=event_ts,
        )
        entry = StreamEntry.objects.create(
            stream=stream,
            event=event,
            offset=stream.get_next_offset(),
        )
        return StreamMessage(
            data=json.dumps(event.data).encode("utf-8"),
            offset=format_offset(entry.offset),
            timestamp=entry.created_at.timestamp(),
            event_ts=(
                event_ts if event_ts is not None else entry.created_at.timestamp()
            ),
            label=label,
            metadata=metadata or None,
        )

    # =========================================================================
    # Producer fencing
    # =========================================================================

    def _validate_producer(
        self, stream: Stream, producer_id: str, epoch: int, seq: int
    ) -> ProducerValidationResult:
        """Decide the outcome for one fenced write, without mutating.

        The rules live in `rakaia.producer`, shared with the in-memory store,
        so the two cannot drift. This only supplies the last known state.
        """
        row = StreamProducer.objects.filter(
            stream=stream, producer_id=producer_id
        ).first()
        state = (
            None
            if row is None
            else ProducerState(
                epoch=row.epoch, last_seq=row.last_seq, last_updated=row.last_updated
            )
        )
        return validate_producer(state, producer_id, epoch, seq, time.time())

    @staticmethod
    def _commit_producer(stream: Stream, result: ProducerValidationResult) -> None:
        """Advance producer state — only ever after a successful write."""
        if not isinstance(result, ProducerAccepted):
            return
        if result.proposed_state is None:
            return
        StreamProducer.objects.update_or_create(
            stream=stream,
            producer_id=result.producer_id,
            defaults={
                "epoch": result.proposed_state.epoch,
                "last_seq": result.proposed_state.last_seq,
                "last_updated": result.proposed_state.last_updated,
            },
        )

    async def append_with_producer(
        self, path: str, data: bytes, options: Any = None
    ) -> AppendResult:
        """Append under producer fencing.

        Serialization is by database transaction rather than the in-memory
        store's per-producer asyncio lock, because concurrent writers to a
        durable stream are typically different processes, which an in-process
        lock would not see.
        """
        return await sync_to_async(self._append_with_producer_sync)(path, data, options)

    def _append_with_producer_sync(
        self, path: str, data: bytes, options: Any = None
    ) -> AppendResult:
        opts = options if options is not None else AppendOptions()
        producer_id = getattr(opts, "producer_id", None)
        epoch = getattr(opts, "producer_epoch", None)
        seq = getattr(opts, "producer_seq", None)
        if producer_id is None or epoch is None or seq is None:
            return self.append(path, data, opts)

        with transaction.atomic():
            stream = self._require(path)

            result = self._validate_producer(stream, producer_id, epoch, seq)

            if stream.closed:
                return AppendResult(
                    message=None,
                    stream_closed=True,
                    producer_result=(
                        result
                        if not isinstance(result, ProducerAccepted)
                        else ProducerStreamClosed()
                    ),
                )

            if not isinstance(result, ProducerAccepted):
                return AppendResult(message=None, producer_result=result)

            self._check_content_type(stream, opts)
            self._check_seq(stream, opts)

            message = self._write(stream, data, opts)
            self._commit_producer(stream, result)
            if getattr(opts, "seq", None) is not None:
                stream.last_seq = opts.seq
                stream.save(update_fields=["last_seq"])
            self._touch(stream)
            return AppendResult(message=message, producer_result=result)

    def append_many(
        self, path: str, events: Iterable[tuple[bytes, Any]]
    ) -> list[AppendResult]:
        """Append an ordered batch in ONE transaction, returning the entries in
        input order.

        Semantically identical to calling :meth:`append` once per item — same
        event rows, same envelope mapping (``label``->``event_type``,
        per-event ``merge_provenance`` for ``metadata``, ``event_ts``) — but it
        locks the stream's high-water once, allocates a single contiguous offset
        block for the whole batch (so ``unique_together(stream, offset)`` still
        holds and concurrent writers serialize exactly as ``append`` does), and
        ``bulk_create``s the events then the entries. This collapses an N-append
        seed/replay from N transactions into **one** transaction with a bounded
        number of queries: the events and entries are each ``bulk_create``d,
        chunked under the driver's bind-parameter cap, so it is a handful of
        INSERTs regardless of N (not the 2N a loop of ``append`` issues).

        The entries link to the just-created events by instance, which relies on
        ``bulk_create`` populating primary keys — backends with
        ``INSERT ... RETURNING`` (Postgres, and the modern SQLite used in CI).
        The durable store targets Postgres, where this holds.

        One ``append_many`` holds the watermark lock for the whole batch, which
        is ideal for exclusive backfills; on a live, high-throughput stream a
        very large batch would stall concurrent appends, so chunk it caller-side
        if that matters.

        Each item is a ``(data, options)`` tuple where ``options`` is an
        ``AppendOptions`` or ``None`` (a raw append). An empty batch is a no-op
        that returns ``[]`` without touching the database (so it never raises
        for a missing stream). A non-empty batch raises ``StreamNotFound`` if
        the stream does not exist, like :meth:`append`, and returns one
        ``AppendResult`` per item in input order.
        """
        items = list(events)
        if not items:
            return []

        with transaction.atomic():
            stream = self._require(path)

            # Mirror `append`'s per-event envelope mapping. `merge_provenance`
            # is evaluated per item so ambient provenance is captured for each.
            stream_events = [
                StreamEvent(
                    data=json.loads(data),
                    event_type=(getattr(options, "label", "") or "")
                    or _APPEND_EVENT_TYPE,
                    metadata=merge_provenance(getattr(options, "metadata", None)) or {},
                    event_ts=getattr(options, "event_ts", None),
                )
                for data, options in items
            ]
            StreamEvent.objects.bulk_create(stream_events)

            start = stream.get_next_offset_block(len(stream_events))
            entries = [
                StreamEntry(stream=stream, event=event, offset=start + i)
                for i, event in enumerate(stream_events)
            ]
            StreamEntry.objects.bulk_create(entries)
            self._touch(stream)
            return [
                AppendResult(
                    message=StreamMessage(
                        data=json.dumps(event.data).encode("utf-8"),
                        offset=format_offset(entry.offset),
                        timestamp=entry.created_at.timestamp(),
                        event_ts=(
                            event.event_ts
                            if event.event_ts is not None
                            else entry.created_at.timestamp()
                        ),
                        label=(
                            ""
                            if event.event_type == _APPEND_EVENT_TYPE
                            else event.event_type
                        ),
                        metadata=event.metadata or None,
                    )
                )
                for event, entry in zip(stream_events, entries, strict=True)
            ]

    def read(
        self, path: str, offset: str | None = None
    ) -> tuple[list[StreamMessage], bool]:
        """Return ``(messages, up_to_date)`` ordered oldest-first.

        With no `offset`, returns every message; with one, returns the messages
        strictly after it. Raises `StreamNotFound` if the stream does not exist
        or has expired. A read extends the sliding TTL window.
        """
        stream = self._require(path)
        self._touch(stream)

        entries = stream.entries.select_related("event").order_by("offset")
        if offset not in (None, "", "-1"):
            entries = entries.filter(offset__gt=int(offset))  # type: ignore[arg-type]

        messages = [
            StreamMessage(
                data=json.dumps(entry.event.data).encode("utf-8"),
                offset=format_offset(entry.offset),
                timestamp=entry.created_at.timestamp(),
                # Logical envelope ts if the producer set one, else the append
                # time — mirroring the in-memory store's default.
                event_ts=(
                    entry.event.event_ts
                    if entry.event.event_ts is not None
                    else entry.created_at.timestamp()
                ),
                # `"append"` is the raw-append sentinel → no envelope label;
                # an empty metadata dict → None, matching the in-memory store.
                label=""
                if entry.event.event_type == _APPEND_EVENT_TYPE
                else entry.event.event_type,
                metadata=entry.event.metadata or None,
            )
            for entry in entries
        ]
        return messages, True

    def get(self, path: str) -> Stream | None:
        """Return the stream row, or None if absent or expired.

        The row carries everything a protocol server reads off a stream —
        `current_offset`, `closed`, `content_type`, `ttl_seconds`,
        `expires_at`, `last_seq` — under the same names as the in-memory
        `rakaia.types.Stream`.
        """
        return self._get_if_not_expired(path)

    def has(self, path: str) -> bool:
        return self._get_if_not_expired(path) is not None

    def delete(self, path: str) -> bool:
        """Delete a stream and its entries. Returns whether it existed.

        The offset high-water (`StreamOffsetWatermark`) deliberately survives,
        so a stream recreated at this path resumes numbering above the retired
        mark rather than reissuing offsets a subscriber has already seen.
        """
        deleted, _ = Stream.objects.filter(stream_id=path).delete()
        return deleted > 0

    def format_response(self, path: str, messages: list[StreamMessage]) -> bytes:
        """Render `messages` as the response body for `path`.

        A JSON-mode stream yields one JSON array; anything else the payloads
        concatenated.
        """
        stream = self._get_if_not_expired(path)
        concatenated = b"".join(m.data for m in messages)
        if stream is not None and is_json_content_type(stream.content_type):
            # Stored payloads are standalone JSON documents; the shared
            # formatter expects the store's comma-separated concatenation.
            joined = b",".join(m.data for m in messages)
            return format_json_response(joined + b"," if joined else b"")
        return concatenated

    async def wait_for_messages(
        self, path: str, offset: str, timeout_seconds: float
    ) -> tuple[list[StreamMessage], bool, bool]:
        """Long-poll for messages after `offset`.

        Returns `(messages, timed_out, stream_closed)`, matching the in-memory
        store.

        Polls rather than waiting on an in-process event: a durable stream is
        typically appended to by a different process, which an `asyncio.Event`
        in this one would never observe.
        """
        deadline = time.monotonic() + timeout_seconds
        while True:
            messages, _ = await sync_to_async(self.read)(path, offset)
            if messages:
                return messages, False, False

            stream = await sync_to_async(self._get_if_not_expired)(path)
            if stream is None:
                raise StreamNotFound(f"Stream not found: {path}")
            if stream.closed:
                return [], False, True

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return [], True, False
            await asyncio.sleep(min(_POLL_INTERVAL_SECONDS, remaining))

    def get_current_offset(self, path: str) -> str | None:
        """The latest offset ever issued for the stream as a string, or None if
        the stream is absent.

        Reflects the persisted high-water (``StreamOffsetWatermark``), not only
        the live entries: a stream recreated at the same path reports a head at
        or above its retired high mark even before the first re-append, so a
        stale subscriber cursor reads as ``caught_up`` rather than a spurious
        ``rewound`` (#34, Defect #2). Mirrors ``Stream.get_next_offset``'s
        ``max(entries, watermark)`` so allocation and tail-reporting agree.
        """
        stream = Stream.objects.filter(stream_id=path).first()
        if stream is None:
            return None
        entries_max = stream.entries.aggregate(max_offset=Max("offset"))["max_offset"]
        watermark_high = (
            StreamOffsetWatermark.objects.filter(stream_path=path)
            .values_list("high", flat=True)
            .first()
        )
        return format_offset(max(entries_max or 0, watermark_high or 0))

    def list_paths(self) -> list[str]:
        return list(Stream.objects.values_list("stream_id", flat=True))
