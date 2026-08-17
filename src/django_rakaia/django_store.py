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
not required there.

A protocol stream, though, may declare any content type at all, so a payload
that is not JSON is stored as text — or base64, if it is not valid UTF-8 —
and marked with `StreamEvent.payload_encoding`, which `read` inverts. Those
payloads *do* round-trip byte for byte. See `encode_payload`.

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

from rakaia.append_decision import StreamFacts, decide_append
from rakaia.context import merge_provenance
from rakaia.json_mode import (
    format_json_response,
    is_json_content_type,
    normalize_content_type,
    process_json_append,
)
from rakaia.types import (
    AppendOptions,
    AppendResult,
    CloseResult,
    ContentTypeMismatch,
    ProducerAccepted,
    ProducerState,
    ProducerValidationResult,
    SequenceConflict,
    StreamConfigConflict,
    StreamMessage,
    StreamNotFound,
)
from rakaia.types import Stream as ProtocolStream

from .event_message import (
    APPEND_EVENT_TYPE as _APPEND_EVENT_TYPE,
)
from .event_message import (
    decode_payload,
    encode_payload,
    message_of,
)
from .hermeticity import armed_deny_aliases, deny_database_access
from .models import (
    Stream,
    StreamEntry,
    StreamEvent,
    StreamProducer,
)

# Re-exported: the payload helpers used to live here, and `format_offset` has
# always been reachable through here; both this package and its tests import
# them from this module. Moving the helpers to `event_message` (#153) keeps the
# names at their old home rather than rippling through every call site.
from .offsets import format_offset, parse_offset

__all__ = [
    "DjangoStreamStore",
    "decode_payload",
    "encode_payload",
    "format_offset",
    "message_of",
    "write_enveloped_event",
]


def write_enveloped_event(
    streams: Iterable[Stream],
    data: Any,
    *,
    label: str = "",
    metadata: Any = None,
    event_ts: float | None = None,
    payload_encoding: str | None = None,
) -> tuple[StreamEvent, list[StreamEntry]]:
    """Write one enveloped event into `streams`. The **only** writer of one.

    Returns the `StreamEvent` and its `StreamEntry` rows, in `streams` order.

    Every rule that turns an envelope into columns is resolved here, once:

    * **the label** becomes `event_type`, with a labelless event recorded under
      the stable `"append"` sentinel that `_read_since` inverts. `event_type` is
      required metadata for the dashboard, so there is no "no type" to write;
    * **the metadata** is merged with the ambient `provenance()` block —
      explicit over ambient — and stored as `{}` rather than NULL when there is
      none;
    * **`event_ts`** is passed through, NULL included. NULL means "no logical
      time was set", and readers surface the append time instead. The caller
      decides whether to set one: a raw protocol append has none unless the
      producer supplied it, while `@stream_model` always stamps one;
    * **the offsets** come from `Stream.get_next_offset`, which locks the
      per-path high-water — so this must run inside a transaction.

    **Fan-out is `streams`, plural.** One event appearing in several streams is
    one `StreamEvent` with one envelope and N `StreamEntry` rows, each with its
    own offset in its own stream — not N events that happen to look alike. The
    protocol store only ever passes one stream; `@stream_model` passes as many
    as its `stream_paths` resolved to.

    This exists because there were two of it. `DjangoStreamStore._write` and
    `create_stream_event` each had a copy, and the copies disagreed about the
    sentinel and about which offset helper to call — the drift
    `django_rakaia.envelope`'s module docstring warns about, in the module it
    warns about it in (#131).
    """
    event = StreamEvent.objects.create(
        data=data,
        event_type=label or _APPEND_EVENT_TYPE,
        metadata=merge_provenance(metadata) or {},
        event_ts=event_ts,
        payload_encoding=payload_encoding,
    )
    entries = [
        StreamEntry.objects.create(
            stream=stream,
            event=event,
            offset=stream.get_next_offset(),
        )
        for stream in streams
    ]
    return event, entries


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
            # A naive timestamp is taken as UTC; a parsed offset is kept, not
            # overwritten (`replace(tzinfo=...)` would silently discard it).
            if expires.tzinfo is None:
                expires = expires.replace(tzinfo=timezone.utc)
            if now > expires.timestamp():
                return True

        return False

    def _get_if_not_expired(
        self, path: str, *, for_update: bool = False
    ) -> Stream | None:
        """The live stream at `path`, deleting it first if it has expired.

        `for_update` locks the stream row for the enclosing transaction, so a
        writer's closed/Stream-Seq/producer checks and its write are one
        atomic step against concurrent writers. Only valid inside
        `transaction.atomic()`; a no-op on backends without row locks (SQLite).
        """
        qs = Stream.objects.select_for_update() if for_update else Stream.objects
        stream = qs.filter(stream_id=path).first()
        if stream is None:
            return None
        if self._is_expired(stream):
            self.delete(path)
            return None
        return stream

    def _require(self, path: str, *, for_update: bool = False) -> Stream:
        stream = self._get_if_not_expired(path, for_update=for_update)
        if stream is None:
            raise StreamNotFound(f"Stream not found: {path}")
        return stream

    def _reap_if_expired(self, path: str) -> None:
        """Expire-and-delete `path` now, outside any write transaction.

        The write paths call this before opening `transaction.atomic()`:
        `_require`'s expiry delete *inside* the transaction is rolled back by
        the very `StreamNotFound` unwind that reports it, so the write path
        would otherwise never actually reap.
        """
        self._get_if_not_expired(path)

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

        Runs in a transaction, like every other write path: `initial_data`
        routes through offset allocation, whose `select_for_update()` demands
        one on Postgres. SQLite has no row locks, so a missing transaction
        here is invisible to the test suite but a guaranteed 500 in
        production — hence the contract's create-with-body case and this
        comment. Do not unwrap it.
        """
        self._reap_if_expired(path)
        with transaction.atomic():
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
                self._write(
                    stream, initial_data, AppendOptions(), is_initial_create=True
                )
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
        self._reap_if_expired(path)
        with transaction.atomic():
            stream = self._get_if_not_expired(path, for_update=True)
            if stream is None:
                return None

            # A close is admitted on the same terms as a fenced append with no
            # body, so it asks the same question: an already-closed stream is
            # reported, never re-closed — a different producer's close must not
            # overwrite `closed_by`, which is what makes a retry of the
            # *original* closing tuple recognisable as a duplicate.
            verdict = decide_append(
                StreamFacts(closed=stream.closed, closed_by=stream.closed_by),
                AppendOptions(
                    producer_id=producer_id, producer_epoch=epoch, producer_seq=seq
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

            # `decide_append` leaves `producer_result` unset only when no
            # producer tuple was supplied. This method's producer parameters
            # are required, so fencing always ran and a verdict always exists.
            assert verdict.producer_result is not None
            self._commit_producer(stream, verdict.producer_result)
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

        Admission (closed / content-type / producer fencing / Stream-Seq) is
        decided by `rakaia.append_decision`, shared with the in-memory store, so
        the two cannot reach different verdicts on the same options.

        Outcomes, all matching the in-memory store:

        - Raises `StreamNotFound` if the stream is absent or expired.
        - Returns `AppendResult(stream_closed=True, message=None)` if closed.
        - Raises `ContentTypeMismatch` / `SequenceConflict` / `InvalidJson` /
          `EmptyJsonArray` for the corresponding failures.
        - Honours `options.close`: the append and the close are one atomic
          step, and the result reports `stream_closed=True`.
        """
        opts = options if options is not None else AppendOptions()
        self._reap_if_expired(path)
        with transaction.atomic():
            stream = self._require(path, for_update=True)

            # Admission is decided in `rakaia.append_decision`, shared with the
            # in-memory store. This path used to inline its own shorter sequence
            # and ignore `opts.producer_id` entirely, so the same options
            # reached different verdicts on the two stores while this method's
            # docstring claimed they matched.
            verdict = decide_append(
                StreamFacts(
                    closed=stream.closed,
                    closed_by=stream.closed_by,
                    content_type=stream.content_type,
                    last_seq=stream.last_seq,
                ),
                opts,
                producer_state=self._producer_state(
                    stream, getattr(opts, "producer_id", None)
                ),
                now=time.time(),
            )
            if not verdict.write:
                return AppendResult(
                    message=None,
                    stream_closed=verdict.stream_closed,
                    producer_result=verdict.producer_result,
                )

            messages = self._write(stream, data, opts)

            if verdict.producer_result is not None:
                self._commit_producer(stream, verdict.producer_result)

            if getattr(opts, "seq", None) is not None:
                stream.last_seq = opts.seq
                stream.save(update_fields=["last_seq"])
            close = bool(getattr(opts, "close", False))
            if close:
                self._close_from_append(stream, opts)
            self._touch(stream)
            # A JSON-mode array append writes several messages; the result
            # carries the last, whose offset is the stream's new head — which
            # is what a caller resumes from.
            return AppendResult(
                message=messages[-1] if messages else None,
                stream_closed=close,
                producer_result=verdict.producer_result,
            )

    # =========================================================================
    # Append helpers
    # =========================================================================

    @staticmethod
    def _close_from_append(stream: Stream, opts: Any) -> None:
        """Close the stream as the final step of an append (`Stream-Closed: true`).

        Records the closing producer tuple when there is one, exactly as the
        in-memory store does, so a retried close-with-body is recognisable as a
        duplicate.
        """
        stream.closed = True
        stream.closed_at = time.time()
        fields = ["closed", "closed_at"]
        producer_id = getattr(opts, "producer_id", None)
        if producer_id is not None:
            stream.closed_by_producer_id = producer_id
            stream.closed_by_epoch = getattr(opts, "producer_epoch", None) or 0
            stream.closed_by_seq = getattr(opts, "producer_seq", None) or 0
            fields += ["closed_by_producer_id", "closed_by_epoch", "closed_by_seq"]
        stream.save(update_fields=fields)

    @staticmethod
    def _payloads_for(
        stream: Stream, data: bytes, *, is_initial_create: bool
    ) -> list[tuple[Any, str | None]]:
        """The stored payloads one append produces, in order.

        Usually one. In JSON mode an array append produces one per element:
        the protocol flattens a top-level array so that `[a, b]` is two
        messages, not one message that happens to be an array — the same
        one-level flatten `rakaia.json_mode.process_json_append` performs for
        the in-memory store. Storing the array whole would also hand a *list*
        to everything that reads `StreamEvent.data` expecting an object.

        Validation happens here, before any row is written, so a malformed body
        raises `InvalidJson` / `EmptyJsonArray` rather than leaving a partial
        append behind.
        """
        if not is_json_content_type(stream.content_type):
            return [encode_payload(data, stream.content_type)]

        # `process_json_append` validates and flattens, and now returns one
        # payload per message rather than a framed blob — so its result is the
        # split, and this no longer re-parses the body to redo the same work.
        payloads = process_json_append(data, is_initial_create=is_initial_create)
        if not payloads:
            # An empty array on create: a stream with no messages yet.
            return []
        return [(json.loads(payload), None) for payload in payloads]

    def _write(
        self,
        stream: Stream,
        data: bytes,
        opts: Any,
        *,
        is_initial_create: bool = False,
    ) -> list[StreamMessage]:
        """Persist one append and return its messages in protocol shape.

        One message per stored payload — more than one only for a JSON-mode
        array append (see `_payloads_for`). The envelope on `opts` is copied
        onto each, as the in-memory store does when it flattens.

        The rows themselves are written by `write_enveloped_event`, shared with
        `@stream_model`; what stays here is turning the written rows back into
        protocol messages.
        """
        payloads = self._payloads_for(stream, data, is_initial_create=is_initial_create)
        if not payloads:
            return []

        label = getattr(opts, "label", "") or ""
        event_ts = getattr(opts, "event_ts", None)

        messages: list[StreamMessage] = []
        for value, encoding in payloads:
            event, (entry,) = write_enveloped_event(
                [stream],
                value,
                label=label,
                metadata=getattr(opts, "metadata", None),
                event_ts=event_ts,
                payload_encoding=encoding,
            )
            messages.append(message_of(entry))
        return messages

    # =========================================================================
    # Producer fencing
    # =========================================================================

    @staticmethod
    def _producer_state(
        stream: Stream, producer_id: str | None
    ) -> ProducerState | None:
        """The last known state for `producer_id`, or None.

        The one fact `decide_append` cannot work out for itself, since only the
        store knows where producer state lives — here, a `StreamProducer` row.
        """
        if producer_id is None:
            return None
        row = StreamProducer.objects.filter(
            stream=stream, producer_id=producer_id
        ).first()
        if row is None:
            return None
        return ProducerState(
            epoch=row.epoch, last_seq=row.last_seq, last_updated=row.last_updated
        )

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

        self._reap_if_expired(path)
        with transaction.atomic():
            # The row lock is what makes fencing fence: validation reads the
            # producer's last state, and two concurrent retries of the same
            # (producer_id, epoch, seq) that both read before either commits
            # would both be accepted — the exact duplicate write the fencing
            # exists to prevent. Serialized on the stream row, the loser
            # re-reads the winner's committed state. (The in-memory store's
            # per-producer asyncio.Lock is this lock's in-process analogue.)
            stream = self._require(path, for_update=True)

            # The same shared sequence `append` uses. This path used to run its
            # own: fencing first, then closed, and it never read `closed_by` —
            # so a closed stream answered with the fencing outcome instead of
            # the close, and a producer retrying its own closing append was
            # never recognised.
            verdict = decide_append(
                StreamFacts(
                    closed=stream.closed,
                    closed_by=stream.closed_by,
                    content_type=stream.content_type,
                    last_seq=stream.last_seq,
                ),
                opts,
                producer_state=self._producer_state(stream, producer_id),
                now=time.time(),
            )
            if not verdict.write:
                return AppendResult(
                    message=None,
                    stream_closed=verdict.stream_closed,
                    producer_result=verdict.producer_result,
                )
            result = verdict.producer_result

            messages = self._write(stream, data, opts)
            if result is not None:
                self._commit_producer(stream, result)
            if getattr(opts, "seq", None) is not None:
                stream.last_seq = opts.seq
                stream.save(update_fields=["last_seq"])
            close = bool(getattr(opts, "close", False))
            if close:
                self._close_from_append(stream, opts)
            self._touch(stream)
            return AppendResult(
                message=messages[-1] if messages else None,
                producer_result=result,
                stream_closed=close,
            )

    def append_many(
        self, path: str, events: Iterable[tuple[bytes, Any]]
    ) -> list[AppendResult]:
        """Append an ordered batch in ONE transaction, returning the entries in
        input order.

        Semantically identical to calling :meth:`append` once per item — same
        event rows, same envelope mapping (``label``->``event_type``,
        per-event ``merge_provenance`` for ``metadata``, ``event_ts``), and the
        same frames delivered to live subscribers — but it
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

        The rest of :meth:`append`'s semantics hold per item too: a closed
        stream refuses every item with ``stream_closed=True``; per-item
        ``content_type`` and ``seq`` are validated (the whole batch, before any
        row is written — a conflict raises and writes nothing); and an item
        with ``close=True`` closes the stream, refusing the items after it,
        exactly as a loop of ``append`` would.
        """
        items = list(events)
        if not items:
            return []

        self._reap_if_expired(path)
        with transaction.atomic():
            stream = self._require(path, for_update=True)

            if stream.closed:
                return [AppendResult(message=None, stream_closed=True) for _ in items]

            # Validate before writing, exactly as a loop of `append` would:
            # per-item content type, and a sequentially advancing Stream-Seq.
            # The batch is cut at the first item that closes the stream; items
            # after it observe the closed stream and are refused, not written.
            last_seq = stream.last_seq
            cut = len(items)
            for i, (_data, options) in enumerate(items):
                provided_ct = getattr(options, "content_type", None)
                if (
                    provided_ct
                    and stream.content_type
                    and normalize_content_type(provided_ct)
                    != normalize_content_type(stream.content_type)
                ):
                    raise ContentTypeMismatch(
                        f"Content-type mismatch: expected "
                        f"{stream.content_type}, got {provided_ct}"
                    )
                seq = getattr(options, "seq", None)
                if seq is not None:
                    if last_seq is not None and seq <= last_seq:
                        raise SequenceConflict(
                            f"Sequence conflict: {seq} <= {last_seq}"
                        )
                    last_seq = seq
                if getattr(options, "close", False):
                    cut = i + 1
                    break
            written, refused = items[:cut], items[cut:]

            # Mirror `append`'s per-event envelope mapping and payload encoding.
            # `merge_provenance` is evaluated per item so ambient provenance is
            # captured for each. Unlike `append`, a JSON array is *not* flattened
            # here: this surface promises one result per input item, and an
            # event-sourcing batch item is one event whose payload may well be a
            # list.
            encoded = [encode_payload(data, stream.content_type) for data, _ in written]
            stream_events = [
                StreamEvent(
                    data=value,
                    event_type=(getattr(options, "label", "") or "")
                    or _APPEND_EVENT_TYPE,
                    metadata=merge_provenance(getattr(options, "metadata", None)) or {},
                    event_ts=getattr(options, "event_ts", None),
                    payload_encoding=encoding,
                )
                for (_data, options), (value, encoding) in zip(
                    written, encoded, strict=True
                )
            ]
            StreamEvent.objects.bulk_create(stream_events)

            start = stream.get_next_offset_block(len(stream_events))
            entries = [
                StreamEntry(stream=stream, event=event, offset=start + i)
                for i, event in enumerate(stream_events)
            ]
            StreamEntry.objects.bulk_create(entries)
            # `bulk_create` does not fire `post_save`, so the receiver that
            # publishes single appends never sees these rows — every bulk append
            # used to be invisible to subscribers (issue #82). Publish them
            # explicitly rather than saving one at a time, which would undo the
            # reason this method exists. Inside the transaction, matching the
            # receiver's existing timing on the `append` path.
            self._publish(stream.stream_id, entries)

            if last_seq != stream.last_seq:
                stream.last_seq = last_seq
                stream.save(update_fields=["last_seq"])
            closing = cut < len(items) or bool(
                written and getattr(written[-1][1], "close", False)
            )
            if closing:
                self._close_from_append(stream, written[-1][1])
            self._touch(stream)
            return [
                AppendResult(
                    message=message_of(entry),
                    stream_closed=bool(getattr(options, "close", False)),
                )
                for (_data, options), _event, entry in zip(
                    written, stream_events, entries, strict=True
                )
            ] + [AppendResult(message=None, stream_closed=True) for _ in refused]

    @staticmethod
    def _publish(stream_id: str, entries: list[StreamEntry]) -> None:
        """Publish appended entries to live subscribers.

        The store's own publish step. Delegates to
        `channels_signals.broadcast_entries`, the single definition of the wire
        frame, which the `post_save` receiver also uses — so the two write paths
        cannot describe the same event differently.

        Imported lazily and tolerantly: `channels` is an optional extra
        (ADR 0002 / #41), and a framework-tier consumer that never installed it
        must still be able to append.
        """
        if not entries:
            return
        try:
            from .channels_signals import broadcast_entries
        except ImportError:
            return
        broadcast_entries(stream_id, entries)

    def read(
        self, path: str, offset: str | None = None
    ) -> tuple[list[StreamMessage], bool]:
        """Return ``(messages, up_to_date)`` ordered oldest-first.

        With no `offset`, returns every message; with one, returns the messages
        strictly after it. Raises `StreamNotFound` if the stream does not exist
        or has expired, and `InvalidOffset` if the offset is not one this store
        issued. A read extends the sliding TTL window.
        """
        stream = self._require(path)
        self._touch(stream)
        return self._read_since(stream, offset), True

    def _read_since(
        self, stream: Stream, offset: str | None = None
    ) -> list[StreamMessage]:
        """The messages after `offset`, without extending the TTL window.

        Split out from `read` so long-poll can check for new messages on every
        tick without writing `last_activity_at` on every tick with it.
        """
        entries = stream.entries.select_related("event").order_by("offset")
        if offset not in (None, "", "-1"):
            entries = entries.filter(offset__gt=self._parse_offset(offset))

        return [message_of(entry) for entry in entries]

    @staticmethod
    def _parse_offset(offset: str) -> int:
        """The entry offset `offset` denotes. Delegates to `parse_offset`."""
        return parse_offset(offset)

    async def run_sync(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Run a synchronous store call for an async server, in a thread.

        Django raises `SynchronousOnlyOperation` on any ORM access from an
        async context, so every sync call the protocol server makes has to
        cross into a thread. `thread_sensitive=True` keeps them all on the same
        one, which is what makes them share a transaction and a connection.

        **The hermeticity guard is carried across the hop.** Django's connections
        are thread-local, so a `deny_database_access` armed by the caller is
        invisible on the worker thread — the ORM work would step straight past a
        guard the caller believes is on, and the rebuild gate would report green
        without having checked anything (#147). Re-arming it here is what makes
        the guard mean the same thing on both sides of the boundary.

        A store reading its log from a guarded alias will now raise rather than
        pass. That is the intended answer, not a regression: a hermetic rebuild
        must read its log from somewhere other than the database it is proving it
        can reconstruct, which `deny_database_access` has always documented.
        """
        denied = armed_deny_aliases()
        if not denied:
            return await sync_to_async(fn, thread_sensitive=True)(*args, **kwargs)

        def _guarded(*inner_args: Any, **inner_kwargs: Any) -> Any:
            with deny_database_access(*denied):
                return fn(*inner_args, **inner_kwargs)

        return await sync_to_async(_guarded, thread_sensitive=True)(*args, **kwargs)

    def get(self, path: str) -> ProtocolStream | None:
        """Return a snapshot of the stream's metadata, or None if absent.

        Deliberately **not** the ORM row. A protocol server is async, and an
        ORM row is lazy: reading `stream.current_offset` off it would issue a
        query at attribute access, outside the `run_sync` bridge, which Django
        refuses from an async context. Everything the server reads is resolved
        here, inside the sync call, and handed over inert.

        Returns a `rakaia.types.Stream` — the same type the in-memory store
        returns — carrying metadata only. It never carried messages; read the
        stream with `read()`.
        """
        row = self._get_if_not_expired(path)
        if row is None:
            return None
        return ProtocolStream(
            path=row.stream_id,
            content_type=row.content_type,
            current_offset=row.current_offset,
            last_seq=row.last_seq,
            ttl_seconds=row.ttl_seconds,
            expires_at=row.expires_at,
            created_at=row.created_at.timestamp() if row.created_at else 0.0,
            last_activity_at=row.last_activity_at,
            closed=row.closed,
            closed_by=row.closed_by,
        )

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
        concatenated. Raises `StreamNotFound` for an absent or expired stream,
        matching the in-memory store — returning `b""` here would silently
        drop the JSON-array framing on the expiry race instead of failing.
        """
        stream = self._require(path)
        if is_json_content_type(stream.content_type):
            # Stored payloads are standalone JSON documents, which is now what
            # the shared formatter takes. It used to want the in-memory store's
            # comma-separated concatenation, so this path had to re-frame the
            # payloads just to have them unframed again (#155).
            return format_json_response([m.data for m in messages])
        return b"".join(m.data for m in messages)

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
        entered = False
        while True:
            stream = await sync_to_async(self._get_if_not_expired)(path)
            if stream is None:
                if not entered:
                    raise StreamNotFound(f"Stream not found: {path}")
                # The stream expired mid-wait. The in-memory store reports
                # that as an ordinary timeout, not an error — a client that
                # was legitimately waiting should not get a 404 for it.
                return [], True, False
            entered = True

            messages = await sync_to_async(self._read_since)(stream, offset)
            if messages:
                # The TTL window is extended once, on the way out — not on
                # every tick. Polling through `read` wrote `last_activity_at`
                # 20 times a second for each waiter on a stream with a TTL.
                await sync_to_async(self._touch)(stream)
                return messages, False, False

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

        An expired stream reports ``None`` exactly as an absent one does — the
        in-memory store behaves the same, and every other read on this store
        applies the expiry check.
        """
        stream = self._get_if_not_expired(path)
        if stream is None:
            return None
        # The head itself is `Stream.current_offset`; all this method adds is
        # the expiry check and the absent-stream `None`.
        return stream.current_offset

    def list_paths(self) -> list[str]:
        return list(Stream.objects.values_list("stream_id", flat=True))
