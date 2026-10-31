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
epoch/seq fencing, close, the TTL sliding window, and the reads long-poll is
built from — so `rakaia.create_app` can serve the Durable Streams protocol
directly off the database. That is what removed the need for a second,
partial implementation of the protocol in the Django integration.

Every store is held to one shared conformance suite
(`tests/server_store_contract.py`), and what an append may do and what it writes
are decided by one pure module (`rakaia.append_decision`) that every store
calls, so they cannot drift.
"""

from __future__ import annotations

import json
import time
from collections.abc import Iterable, Iterator, Sequence
from contextlib import ExitStack, contextmanager
from dataclasses import dataclass
from typing import Any

from asgiref.sync import sync_to_async
from django.conf import settings
from django.db import transaction

from rakaia import stream_rules
from rakaia.append_decision import (
    AppendPlan,
    PlannedWrite,
    StreamFacts,
    own_stamp,
    plan_appends,
    plan_initial_write,
)
from rakaia.context import correlation_for, merge_provenance
from rakaia.json_mode import is_json_content_type
from rakaia.read_decision import page_of
from rakaia.types import (
    AppendOptions,
    AppendResult,
    CloseResult,
    ProducerAccepted,
    ProducerState,
    ProducerValidationResult,
    StreamClosed,
    StreamConfigConflict,
    StreamMessage,
    StreamNotFound,
    clean_tags,
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
    StreamEventTag,
    StreamOffsetWatermark,
    StreamProducer,
    delete_events,
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


@dataclass(frozen=True)
class NewEvent:
    """One event row to write, every column already decided.

    ``event_ts`` None means "stamp it": `write_events` fills in rakaia's own
    time, under the offset locks.
    """

    data: Any
    event_type: str = _APPEND_EVENT_TYPE
    metadata: dict[str, Any] | None = None
    event_ts: float | None = None
    payload_encoding: str | None = None
    tags: tuple[str, ...] = ()
    correlation_id: str | None = None


def write_events(
    streams: Sequence[Stream],
    events: Sequence[NewEvent],
    *,
    using: str | None = None,
) -> tuple[list[StreamEvent], list[list[StreamEntry]]]:
    """Write `events` into every one of `streams`. The **only** writer of events.

    Returns the `StreamEvent` rows in `events` order, and the `StreamEntry` rows
    per stream, in `streams` order, each list in `events` order. A protocol append is one stream and one event, a batch is one
    stream and many, and `@stream_model`'s fan-out is one event and many streams:
    one `StreamEvent` with one envelope and an entry in each stream, each with its
    own offset there — not several events that happen to look alike.

    The caller must already hold the `streams` rows' locks, taken in `stream_id`
    order, inside a transaction on their database. This locks the offset
    watermarks — also in `stream_id` order, whatever order `streams` is in, so
    two fan-outs over the same streams cannot deadlock each other (#293) — and
    inserting an entry then needs the stream row, so a caller that skipped the
    row lock could deadlock against a protocol append, which takes the row first
    (#298).

    **Event time.** An event with no `event_ts` gets rakaia's own stamp,
    `own_stamp(the latest last stamp across the streams, now)`, with `now` read
    once the locks are held, so it agrees with the offset order in every stream
    the event lands in (#284). Every watermark is brought up to it, or a stream
    whose last stamp was earlier could stamp its next event before this one.

    Everything is `bulk_create`d — events, tags, entries — so the statement count
    does not grow with the batch, and the entries are published to live
    subscribers explicitly, since `bulk_create` fires no `post_save`.

    There used to be two of this: one event at a time for an append and for
    `@stream_model`, and an inline bulk version in `append_many`. Changes to how
    events are stamped and tagged reached the two unevenly (#324).
    """
    streams = list(streams)
    if not events:
        return [], [[] for _ in streams]
    # The event is written to whatever database its streams came from, so one
    # save is never split across two (#159).
    if streams:
        using = streams[0]._state.db
    # One lock per distinct path, taken in path order. A resolver can name the
    # same path twice, and each occurrence then takes its own block of offsets
    # from that one watermark, rather than both reading the same `high`.
    watermarks: dict[str, StreamOffsetWatermark] = {}
    for stream in sorted(streams, key=lambda stream: stream.stream_id):
        if stream.stream_id not in watermarks:
            watermarks[stream.stream_id] = stream.lock_watermark()

    stamp: float | None = None
    if any(e.event_ts is None for e in events):
        # Read the clock only now, with the locks held: a time read before them
        # belongs to whichever writer got there first, not to these positions.
        last = [
            w.last_event_ts for w in watermarks.values() if w.last_event_ts is not None
        ]
        stamp = own_stamp(max(last) if last else None, time.time())
    starts: list[int] = []
    for stream in streams:
        watermark = watermarks[stream.stream_id]
        starts.append(watermark.high + 1)
        watermark.high += len(events)
    for watermark in watermarks.values():
        fields = ["high"]
        if stamp is not None:
            watermark.last_event_ts = stamp
            fields.append("last_event_ts")
        watermark.save(update_fields=fields)

    rows = [
        StreamEvent(
            data=e.data,
            event_type=e.event_type,
            metadata=e.metadata if e.metadata is not None else {},
            event_ts=e.event_ts if e.event_ts is not None else stamp,
            payload_encoding=e.payload_encoding,
            correlation_id=e.correlation_id,
        )
        for e in events
    ]
    # The entries and tags link to these by the ids `bulk_create` sets, which
    # every supported backend returns (Postgres always; SQLite from 3.35).
    StreamEvent.objects.using(using).bulk_create(rows)
    StreamEventTag.objects.using(using).bulk_create(
        StreamEventTag(event=row, tag=tag)
        for row, e in zip(rows, events, strict=True)
        for tag in e.tags
    )
    per_stream = [
        [
            StreamEntry(stream=stream, event=row, offset=starts[i] + n)
            for n, row in enumerate(rows)
        ]
        for i, stream in enumerate(streams)
    ]
    StreamEntry.objects.using(using).bulk_create(
        entry for entries in per_stream for entry in entries
    )
    for stream, entries in zip(streams, per_stream, strict=True):
        _publish(stream.stream_id, entries)
    return rows, per_stream


def write_enveloped_event(
    streams: Iterable[Stream],
    data: Any,
    *,
    label: str = "",
    metadata: Any = None,
    event_ts: float | None = None,
    payload_encoding: str | None = None,
    tags: tuple[str, ...] = (),
    correlation_id: str | None = None,
) -> tuple[StreamEvent, list[StreamEntry]]:
    """Write one event into `streams` with its envelope resolved here: the door
    `@stream_model` uses, which has no `AppendOptions` to plan from.

    The label becomes `event_type` (the `"append"` sentinel when there is none);
    the metadata has the ambient `provenance()` block merged under it; the tags
    and correlation id are checked, the correlation falling back to the ambient
    one. Returns the event and its entries, in `streams` order. See
    `write_events` for the locking it relies on and the event time it stamps.
    """
    streams = list(streams)
    using = streams[0]._state.db if streams else None
    (event,), per_stream = write_events(
        streams,
        [
            NewEvent(
                data=data,
                event_type=label or _APPEND_EVENT_TYPE,
                metadata=merge_provenance(metadata),
                event_ts=event_ts,
                payload_encoding=payload_encoding,
                tags=clean_tags(tags),
                correlation_id=correlation_for(correlation_id),
            )
        ],
        using=using,
    )
    return event, [entries[0] for entries in per_stream]


def _publish(stream_id: str, entries: list[StreamEntry]) -> None:
    """Publish appended entries to live subscribers.

    Delegates to `channels_signals.broadcast_entries`, the single definition of
    the wire frame. Imported lazily and tolerantly: `channels` is an optional
    extra (ADR 0002 / #41), and a framework-tier consumer that never installed
    it must still be able to append.
    """
    if not entries:
        return
    try:
        from .channels_signals import broadcast_entries
    except ImportError:
        return
    broadcast_entries(stream_id, entries)


# How many event ids `DjangoStreamStore.delete` removes per statement. Well
# under SQLite's bound-parameter limit, and large enough that a stream of many
# thousands of events is a handful of round trips.
_ORPHAN_DELETE_CHUNK = 500


def _permanent_streams() -> bool:
    """Whether `RAKAIA_PERMANENT_STREAMS` is on. Off unless a project sets it.

    Read on every call rather than once at import, so `override_settings` and a
    settings change between calls are both honoured.
    """
    return bool(getattr(settings, "RAKAIA_PERMANENT_STREAMS", False))


class _StreamExpired(Exception):
    """Internal: the locked stream row had aged out. Never leaves this module.

    Raised inside a write transaction and caught immediately outside it, which
    is the only way the reap can survive the rollback that reporting the expiry
    causes. `DjangoStreamStore._locked_write` is the whole of its lifetime.
    """

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.path = path


class DjangoStreamStore:
    """A durable store backed by the django_rakaia ORM models.

    Satisfies `rakaia.StreamServerStore`: the event-sourcing read/emit surface
    plus the full protocol lifecycle.

    Pass ``using`` to read and write the log on a named database alias instead of
    the default — the same seam `DjangoExecutor` and `DjangoProjectionReader`
    already sit on. That is what lets a from-scratch rebuild replay into a
    *disposable* database and be verified against the real ORM without touching
    production (ADR 0003): the projections went to the alias already, and now the
    log they are derived from can come from it too.

    Before this, the log was the one part of a rebuild that could not leave the
    default database, so `hermeticity.py` documented a drain-to-memory as the
    workaround — six lines every consumer wrote, tested nowhere, because the store
    was untestable off ``default``.

    ``using=None`` targets the default alias, exactly as before. Pair with
    ``DjangoExecutor(using=...)`` and ``DjangoProjectionReader(using=...)``.
    """

    def __init__(
        self, *, using: str | None = None, permanent: bool | None = None
    ) -> None:
        self._using = using
        #: Whether every stream here is kept for good (see `rakaia.stream_rules`).
        #: None, the default, reads `RAKAIA_PERMANENT_STREAMS` on every call, so
        #: `override_settings` and a settings change are both honoured.
        self.permanent = permanent

    @property
    def _permanent(self) -> bool:
        if self.permanent is not None:
            return self.permanent
        return _permanent_streams()

    # =========================================================================
    # Alias
    # =========================================================================

    def _streams(self) -> Any:
        """`Stream` rows on this store's alias.

        Every read and write of a model goes through one of these accessors, so
        the alias is applied in one place rather than being remembered at each of
        the ten call sites it used to be absent from. `.using(None)` keeps
        Django's default routing, so this is uniform whether or not an alias was
        given — the same shape as `DjangoExecutor._manager`.

        It also fixes the alias for rows *derived* from a stream: the write path
        takes its alias from the stream row it was handed
        (`write_events`, #159), and `Stream.lock_watermark` from
        ``self._state.db``. Loading the stream on the right alias is therefore
        what puts its event, its entry and its offset high-water there too.
        """
        return Stream.objects.using(self._using)

    def _events(self) -> Any:
        """`StreamEvent` rows on this store's alias."""
        return StreamEvent.objects.using(self._using)

    def _entries(self) -> Any:
        """`StreamEntry` rows on this store's alias."""
        return StreamEntry.objects.using(self._using)

    def _producers(self) -> Any:
        """`StreamProducer` rows on this store's alias."""
        return StreamProducer.objects.using(self._using)

    # =========================================================================
    # Expiry
    # =========================================================================

    def _is_expired(self, stream: Stream) -> bool:
        """Whether the stream has aged out; always false on a permanent store,
        which is what stops every read and write path from reaping, since they
        all ask here."""
        return stream_rules.is_expired(
            stream.ttl_seconds,
            stream.expires_at or None,
            stream.last_activity_at,
            now=time.time(),
            permanent=self._permanent,
        )

    def _get_if_not_expired(self, path: str) -> Stream | None:
        """The live stream at `path`, deleting it first if it has expired.

        The unlocked read, for the read paths. A writer wants
        `_locked_write`, which takes the row lock in the same single SELECT.
        """
        stream = self._streams().filter(stream_id=path).first()
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

    def _reap_if_expired(self, path: str) -> None:
        """Expire-and-delete `path` now, outside any write transaction.

        `create` calls this before opening `transaction.atomic()`. Its own
        in-transaction reap does normally survive — it goes on to insert the
        replacement row and commit — but not if the `initial_data` it was given
        then fails validation, and the rollback that reports *that* takes the
        reap with it.

        The append paths do not need it: `_locked_write` carries the expiry out
        of the transaction itself, and so reaps with one read of the stream row
        rather than two (#202).
        """
        self._get_if_not_expired(path)

    @contextmanager
    def _locked_write(self, path: str) -> Iterator[Stream]:
        """Open a write transaction on `path`'s locked, live stream row.

        The single door every append and every close goes through. It yields
        the `Stream` inside `transaction.atomic()` with the row locked, so a
        writer's closed / content-type / producer checks and its write are one
        indivisible step against concurrent writers. Raises `StreamNotFound`
        for an absent *or* expired stream, and the expired one is reaped.

        **One read of the stream row, not two.** Reaping an expired stream and
        locking a live one look like two different jobs — the reap has to
        commit, and a write transaction that ends in `StreamNotFound` rolls
        back — so the write paths used to do both: an unlocked read to reap
        before `atomic()`, then the locked read inside it. That is one wasted
        SELECT on *every* append, to handle a case that is rare and already
        handled by every read path (#202).

        Instead the locked read decides, and the expiry leaves the transaction
        as `_StreamExpired` — a signal, not an outcome — to be reaped on the
        far side of the rollback and reported as `StreamNotFound`. Nothing has
        been written at that point, so there is nothing for the rollback to
        lose. `TestExpiryReaping` is what holds this to actually deleting the
        row; it is the test that caught the version of this that reported 404s
        forever without ever reaping.

        Only for the *write* paths. A reader wants `_require`, which does not
        lock and does not need a transaction.
        """
        try:
            with transaction.atomic(using=self._using):
                stream = (
                    self._streams().select_for_update().filter(stream_id=path).first()
                )
                if stream is None:
                    raise StreamNotFound(f"Stream not found: {path}")
                if self._is_expired(stream):
                    raise _StreamExpired(path)
                yield stream
        except _StreamExpired:
            self.delete(path)
            raise StreamNotFound(f"Stream not found: {path}") from None

    @staticmethod
    def _facts(stream: Stream) -> StreamFacts:
        """A locked stream row as `rakaia.append_decision` sees it.

        The one place a row is narrowed to the four fields an append is decided
        from, so a fifth fact cannot reach one write path and miss another. A
        close is decided by `rakaia.stream_rules.decide_close`, which takes the
        two fields it needs directly.
        """
        return StreamFacts(
            closed=stream.closed,
            closed_by=stream.closed_by,
            content_type=stream.content_type,
            last_seq=stream.last_seq,
        )

    def _save_changes(self, stream: Stream, plan: AppendPlan) -> None:
        """Record what an append that has already landed changes about the
        stream, as `plan_appends` decided it.

        Producer state is one row per producer, and the batch rule hands back
        only the last accepted outcome for each, so the count of writes does not
        grow with the batch. Everything on the stream row itself — `Stream-Seq`,
        the close and its producer tuple, the TTL window — is one save, and only
        of the columns that moved: an append that changes none of them costs no
        statement here, which is most of them.
        """
        for accepted in plan.producer_commits.values():
            self._commit_producer(stream, accepted)
        fields: list[str] = []
        if plan.last_seq != stream.last_seq:
            stream.last_seq = plan.last_seq
            fields.append("last_seq")
        if plan.close:
            stream.closed = True
            stream.closed_at = time.time()
            fields += ["closed", "closed_at"]
            if plan.closed_by is not None:
                stream.closed_by_producer_id = plan.closed_by.producer_id
                stream.closed_by_epoch = plan.closed_by.epoch
                stream.closed_by_seq = plan.closed_by.seq
                fields += ["closed_by_producer_id", "closed_by_epoch", "closed_by_seq"]
        if plan.writes_anything and stream.ttl_seconds is not None:
            stream.last_activity_at = time.time()
            fields.append("last_activity_at")
        if fields:
            stream.save(update_fields=fields)

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

        On a permanent store a `ttl_seconds` or `expires_at` raises
        `ExpiryNotAllowed` before anything is written, including on an idempotent
        re-create. A stream given one before the switch was turned on compares as
        having none, as `get` reports.
        """
        permanent = self._permanent
        stream_rules.check_expiry_allowed(
            path, ttl_seconds, expires_at, permanent=permanent
        )
        self._reap_if_expired(path)
        with transaction.atomic(using=self._using):
            existing = self._get_if_not_expired(path)
            if existing is not None:
                if stream_rules.same_config(
                    stream_rules.StreamConfig(
                        existing.content_type,
                        existing.ttl_seconds,
                        existing.expires_at,
                        existing.closed,
                    ),
                    stream_rules.StreamConfig(
                        content_type, ttl_seconds, expires_at, closed
                    ),
                    permanent=permanent,
                ):
                    return existing
                raise StreamConfigConflict(
                    f"Stream already exists with different configuration: {path}"
                )

            now = time.time()
            stream = self._streams().create(
                stream_id=path,
                content_type=content_type,
                ttl_seconds=ttl_seconds,
                expires_at=expires_at,
                last_activity_at=now,
                closed=closed,
            )
            if initial_data:
                self._write_plan(
                    stream, plan_initial_write(stream.content_type, initial_data)
                )
            return stream

    def close_stream(
        self,
        path: str,
        *,
        producer_id: str | None = None,
        producer_epoch: int | None = None,
        producer_seq: int | None = None,
    ) -> CloseResult | None:
        """Close the stream. `None` if it does not exist.

        Idempotent: closing an already-closed stream reports the same final
        offset with `already_closed=True` rather than failing. With all three
        producer arguments the close is fenced, and a retry of the *same*
        closing tuple reports `already_closed` instead of being refused as a
        duplicate, so a producer that loses the response can safely repeat it.
        """
        producer = (
            (producer_id, producer_epoch, producer_seq)
            if producer_id is not None
            and producer_epoch is not None
            and producer_seq is not None
            else None
        )
        # An absent or expired stream is reported as `None` here, not raised, so
        # the one `StreamNotFound` `_locked_write` can raise is translated back.
        try:
            return self._close_locked(path, producer)
        except StreamNotFound:
            return None

    def _close_locked(
        self, path: str, producer: tuple[str, int, int] | None
    ) -> CloseResult:
        """The close itself, on the locked row, like every other write."""
        with self._locked_write(path) as stream:
            verdict = stream_rules.decide_close(
                closed=stream.closed,
                closed_by=stream.closed_by,
                producer=producer,
                producer_state=(
                    self._producer_state(stream, producer[0]) if producer else None
                ),
                now=time.time(),
            )
            fields: list[str] = []
            if verdict.close:
                if verdict.producer_result is not None:
                    self._commit_producer(stream, verdict.producer_result)
                stream.closed = True
                stream.closed_at = time.time()
                fields += ["closed", "closed_at"]
                if verdict.closed_by is not None:
                    stream.closed_by_producer_id = verdict.closed_by.producer_id
                    stream.closed_by_epoch = verdict.closed_by.epoch
                    stream.closed_by_seq = verdict.closed_by.seq
                    fields += [
                        "closed_by_producer_id",
                        "closed_by_epoch",
                        "closed_by_seq",
                    ]
            if verdict.touch and stream.ttl_seconds is not None:
                stream.last_activity_at = time.time()
                fields.append("last_activity_at")
            if fields:
                stream.save(update_fields=fields)
            return CloseResult(
                final_offset=stream.current_offset,
                already_closed=verdict.already_closed,
                producer_result=verdict.producer_result,
            )

    def append(self, path: str, data: bytes, options: Any = None) -> AppendResult:
        """Append one event: a batch of one. See `append_many`."""
        return self.append_many(path, [(data, options)])[0]

    def append_to_many(
        self, paths: Sequence[str], value: Any, *, label: str = ""
    ) -> StreamEvent:
        """Append one event, already a JSON value, to every stream in `paths`.

        This is what a model save goes through: the same locks, the same
        closed / content-type checks and the same envelope as `append`, but one
        `StreamEvent` with an entry in each stream, so a fan-out stays one fact.
        A missing stream (or an expired one, reaped first) is created as a JSON
        stream. Raises `StreamClosed` if any stream is closed, having written
        nothing.

        Streams are locked in path order, stream row before offset watermark,
        the order a protocol append takes them in (#298).
        """
        if not paths:
            # An event with nowhere to go is still written, as it always was.
            with transaction.atomic(using=self._using):
                return write_enveloped_event([], value, label=label)[0]
        ordered = sorted(set(paths))
        options = AppendOptions(content_type="application/json", label=label)
        body = json.dumps(value).encode()
        with transaction.atomic(using=self._using), ExitStack() as locks:
            for path in ordered:
                self._reap_if_expired(path)
                self._streams().get_or_create(
                    stream_id=path,
                    defaults={
                        "content_type": "application/json",
                        "last_activity_at": time.time(),
                    },
                )
            locked = {
                path: locks.enter_context(self._locked_write(path)) for path in ordered
            }
            plans = {
                path: plan_appends(
                    self._facts(stream), [options], payloads=[body], now=time.time()
                )
                for path, stream in locked.items()
            }
            for path, plan in plans.items():
                if not plan.writes_anything:
                    raise StreamClosed(f"Stream is closed: {path}")
            first = plans[ordered[0]].writes[0]
            assert first is not None
            (event,), _per_stream = write_events(
                [locked[path] for path in paths],
                [
                    NewEvent(
                        data=value,
                        event_type=first.label or _APPEND_EVENT_TYPE,
                        metadata=first.metadata,
                        event_ts=first.event_ts,
                        tags=first.tags,
                        correlation_id=first.correlation_id,
                    )
                ],
                using=self._using,
            )
            for path, plan in plans.items():
                self._save_changes(locked[path], plan)
            return event

    @staticmethod
    def _stored_value(
        content_type: str | None, payload: bytes
    ) -> tuple[Any, str | None]:
        """One stored payload as the `data` column holds it, with its encoding.

        A JSON-mode payload is a standalone JSON document and is stored parsed;
        anything else goes through `encode_payload`.
        """
        if is_json_content_type(content_type):
            return json.loads(payload), None
        return encode_payload(payload, content_type)

    def _write_plan(
        self, stream: Stream, plan: AppendPlan
    ) -> list[tuple[StreamMessage, int] | None]:
        """Write the plan's events, returning per item the last message it
        became, with its event id — the stream's new head, which is what a
        caller resumes from — or None for an item that writes nothing."""
        writes: list[PlannedWrite] = [w for w in plan.writes if w is not None]
        events = [
            NewEvent(
                data=value,
                event_type=write.label or _APPEND_EVENT_TYPE,
                metadata=write.metadata,
                event_ts=write.event_ts,
                payload_encoding=encoding,
                tags=write.tags,
                correlation_id=write.correlation_id,
            )
            for write in writes
            for value, encoding in (
                self._stored_value(stream.content_type, payload)
                for payload in write.payloads
            )
        ]
        _rows, (entries,) = write_events([stream], events)
        results: list[tuple[StreamMessage, int] | None] = []
        cursor = 0
        for write in plan.writes:
            if write is None or not write.payloads:
                results.append(None)
                continue
            cursor += len(write.payloads)
            last = entries[cursor - 1]
            results.append((message_of(last, tags=write.tags), last.event.id))
        return results

    # =========================================================================
    # Producer fencing
    # =========================================================================

    def _producer_state(
        self, stream: Stream, producer_id: str | None
    ) -> ProducerState | None:
        """The last known state for `producer_id`, or None.

        The one fact `decide_append` cannot work out for itself, since only the
        store knows where producer state lives — here, a `StreamProducer` row.
        """
        if producer_id is None:
            return None
        row = self._producers().filter(stream=stream, producer_id=producer_id).first()
        if row is None:
            return None
        return ProducerState(
            epoch=row.epoch, last_seq=row.last_seq, last_updated=row.last_updated
        )

    def _commit_producer(
        self, stream: Stream, result: ProducerValidationResult
    ) -> None:
        """Advance producer state — only ever after a successful write."""
        if not isinstance(result, ProducerAccepted):
            return
        if result.proposed_state is None:
            return
        self._producers().update_or_create(
            stream=stream,
            producer_id=result.producer_id,
            defaults={
                "epoch": result.proposed_state.epoch,
                "last_seq": result.proposed_state.last_seq,
                "last_updated": result.proposed_state.last_updated,
            },
        )

    def append_many(
        self, path: str, events: Iterable[tuple[bytes, Any]]
    ) -> list[AppendResult]:
        """Append an ordered batch in ONE transaction, returning one result per
        item in input order.

        Semantically a loop of :meth:`append` — the same rows, the same envelope,
        the same frames delivered to live subscribers — because both are this:
        `rakaia.append_decision.plan_appends` decides every item and resolves what
        it writes, shared with the other two stores, and `write_events` writes it.
        A refusal anywhere in the batch (a conflict, a body the stream cannot
        hold, a bad tag) raises before a row is written; a fencing outcome comes
        back in its own item's slot instead; an item with ``close=True`` closes
        the stream for the items after it.

        It locks the stream's high-water once and ``bulk_create``s the events,
        tags and entries, so a batch of N items is a handful of statements
        rather than the 2N of N transactions. One ``append_many`` holds the
        watermark lock for the whole batch, which is ideal for exclusive
        backfills; on a live, high-throughput stream a very large batch would
        stall concurrent appends, so chunk it caller-side if that matters.

        An empty batch is a no-op that returns ``[]`` without touching the
        database, so it never raises for a missing stream; a non-empty one raises
        ``StreamNotFound`` for one. In JSON mode an item whose body is a
        top-level array flattens into one row per element, so a batch of N items
        can write more than N rows, and that item's result carries the last.
        """
        items = list(events)
        if not items:
            return []

        with self._locked_write(path) as stream:
            producer_ids = {
                pid
                for _d, o in items
                if (pid := getattr(o, "producer_id", None)) is not None
            }
            plan = plan_appends(
                self._facts(stream),
                [options for _data, options in items],
                payloads=[data for data, _options in items],
                producer_states={
                    pid: self._producer_state(stream, pid) for pid in producer_ids
                },
                now=time.time(),
            )
            # An all-refused plan writes nothing and allocates nothing: a refused
            # append costs only the read that refused it.
            written = (
                self._write_plan(stream, plan)
                if plan.writes_anything
                else [None] * len(items)
            )
            self._save_changes(stream, plan)

            results: list[AppendResult] = []
            for verdict, write, landed in zip(
                plan.verdicts, plan.writes, written, strict=True
            ):
                message, event_id = landed if landed is not None else (None, None)
                results.append(
                    AppendResult(
                        message=message,
                        stream_closed=write.close if write else verdict.stream_closed,
                        producer_result=verdict.producer_result,
                        event_id=event_id,
                    )
                )
            return results

    def read(
        self, path: str, offset: str | None = None, *, limit: int | None = None
    ) -> tuple[list[StreamMessage], bool]:
        """Return ``(messages, up_to_date)`` ordered oldest-first.

        With no `offset`, returns every message; with one, returns the messages
        strictly after it. With a `limit`, at most that many, fetched with a SQL
        ``LIMIT`` so a first sync of a large stream does not load all of it,
        and `up_to_date` is false when more remain (#289). Raises
        `StreamNotFound` if the stream does not exist or has expired, and
        `InvalidOffset` if the offset is not one this store issued.

        A read of a stream with a TTL extends its sliding window, which is a
        write; the Durable Streams conformance suite requires it ("should extend
        TTL on read"). A stream without a TTL has no window, and its reads write
        nothing.
        """
        stream = self._require(path)
        if limit is not None and limit < 1:
            # After the lookup, so a missing stream is `StreamNotFound` as on
            # every other store; before the TTL write, so a refused read extends
            # nothing; and before `limit + 1` below turns -2 into a negative
            # slice, which Django refuses with an error of its own.
            raise ValueError(f"limit must be >= 1, got {limit}")
        self._touch(stream)
        if limit is None:
            return self._read_since(stream, offset), True
        # One row past the page is how "more remain" is known without a count.
        return page_of(self._read_since(stream, offset, limit=limit + 1), limit)

    def peek(self, path: str, offset: str | None) -> list[StreamMessage]:
        """`read` without extending the TTL window; see `StreamServerStore.peek`."""
        return self._read_since(self._require(path), offset)

    def _read_since(
        self, stream: Stream, offset: str | None = None, *, limit: int | None = None
    ) -> list[StreamMessage]:
        """The messages after `offset`, without extending the TTL window.

        Split out from `read` so `peek` can check for new messages on every
        long-poll tick without writing `last_activity_at` on every tick with it. `limit`
        caps the rows fetched; `None` fetches them all.
        """
        entries = (
            stream.entries.select_related("event")
            .prefetch_related("event__tags")
            .order_by("offset")
        )
        if offset not in (None, "", "-1"):
            entries = entries.filter(offset__gt=self._parse_offset(offset))
        if limit is not None:
            entries = entries[:limit]

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
        stream with `read()`. On a permanent store the TTL and expiry read as
        unset, so a stream given one earlier is not advertised as lapsing.
        """
        row = self._get_if_not_expired(path)
        if row is None:
            return None
        ttl_seconds, expires_at = stream_rules.reported_expiry(
            row.ttl_seconds, row.expires_at, permanent=self._permanent
        )
        return ProtocolStream(
            path=row.stream_id,
            content_type=row.content_type,
            current_offset=row.current_offset,
            last_seq=row.last_seq,
            ttl_seconds=ttl_seconds,
            expires_at=expires_at,
            created_at=row.created_at.timestamp() if row.created_at else 0.0,
            last_activity_at=row.last_activity_at,
            closed=row.closed,
            closed_by=row.closed_by,
        )

    def has(self, path: str) -> bool:
        return self._get_if_not_expired(path) is not None

    def delete(self, path: str, *, force: bool = False) -> bool:
        """Delete a stream, its entries, and the events left in no stream at all.

        An event is deleted only when no entry in *any* stream still points at
        it, so an event fanned into a surviving stream is kept. It all happens in
        one transaction on this store's alias. The offset high-water
        (`StreamOffsetWatermark`) deliberately survives, so a stream recreated at
        this path resumes numbering above the retired mark rather than reissuing
        offsets a subscriber has already seen. Returns whether the stream existed.

        Under `RAKAIA_PERMANENT_STREAMS` this raises `DeleteNotAllowed` unless
        `force=True`, which is how an application rebuilds a stream on purpose.
        0.7.0 carried a Python delete out unconditionally, reasoning that a call
        from Python was already an operator's decision; a management command run
        by hand on a production restore is a Python call too, and it is the
        operation someone switching this on is most likely to think they have
        covered. The decision is still the operator's — it now has to be
        expressed. `force` is accepted and does nothing while the switch is off,
        so a caller may pass it unconditionally rather than branch on a setting.

        Both internal callers are the expiry reap, and neither passes `force`.
        Under the switch `_is_expired` is always false so neither is reachable;
        if that ever stops being true, refusing is the right outcome and is
        precisely what the switch promises.

        The entries are walked in primary-key order, `_ORPHAN_DELETE_CHUNK` at a
        time, reading only ids: each chunk's entries are deleted, then that
        chunk's events that no entry points at any more ("no entry left" is a
        left join), and the stream row goes last. `.only("pk")` keeps Django's
        cascade collector from loading payloads, so neither memory nor the
        query count grows with anything but the number of chunks.
        """
        stream_rules.check_delete(path, permanent=self._permanent, force=force)
        with transaction.atomic(using=self._using):
            stream_pk = (
                self._streams().filter(stream_id=path).values_list("pk", flat=True)
            ).first()
            if stream_pk is None:
                return False
            last = 0
            while True:
                chunk = list(
                    self._entries()
                    .filter(stream_id=stream_pk, pk__gt=last)
                    .order_by("pk")
                    .values_list("pk", "event_id")[:_ORPHAN_DELETE_CHUNK]
                )
                if not chunk:
                    break
                last = chunk[-1][0]
                self._entries().filter(pk__in=[pk for pk, _ in chunk]).delete()
                delete_events(
                    self._events().filter(
                        pk__in={event_id for _, event_id in chunk},
                        entries__isnull=True,
                    )
                )
            self._streams().filter(pk=stream_pk).delete()
        return True

    def check_protocol_delete(self, path: str) -> None:
        """Raise `DeleteNotAllowed` if a client may not delete `path`.

        The protocol server calls this before `delete`, and the server answers
        the refusal with `405 Method Not Allowed`. It refuses every path while
        the store is permanent and none otherwise. `delete()`
        refuses as well unless called with `force=True`; this is the check for
        a client's request, made before that.
        """
        stream_rules.check_protocol_delete(path, permanent=self._permanent)

    def get_current_offset(self, path: str) -> str | None:
        """The latest offset ever issued for the stream as a string, or None if
        the stream is absent.

        Reflects the persisted high-water (``StreamOffsetWatermark``), not only
        the live entries: a stream recreated at the same path reports a head at
        or above its retired high mark even before the first re-append, so a
        stale subscriber cursor reads as ``caught_up`` rather than a spurious
        ``rewound`` (#34, Defect #2). Reads the high-water the same way
        ``Stream.lock_watermark`` reads it for allocation — the watermark row,
        falling back to the entries only when it has never been advanced — so
        allocation and tail-reporting cannot name different heads.

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

    def get_generation(self, path: str) -> str | None:
        """The stream's creation stamp, or None if it is absent or expired.

        Changes when the stream is deleted (or expires) and is created again;
        the offsets keep rising across that, so they cannot show it.
        """
        stream = self._get_if_not_expired(path)
        return None if stream is None else stream.generation

    def adopt_generation(self, path: str, generation: str) -> None:
        """Give the stream another log's stamp, because it is a copy of that log.

        For `rakaia.migrate` alone, and only when the copy kept every offset: a
        consumer's saved position then means the same event here, and the stamp
        it saved beside it must not call this a different log. Anywhere else a
        stream's stamp is what says it is *not* the stream another one was.

        Raises:
            StreamNotFound: if `path` is absent or expired.
        """
        with self._locked_write(path) as stream:
            stream.generation = generation
            stream.save(update_fields=["generation"])

    def list_paths(self) -> list[str]:
        return list(self._streams().values_list("stream_id", flat=True))
