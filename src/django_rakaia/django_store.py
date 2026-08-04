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

Scope: the event-sourcing read/emit path only. The live-protocol-server
concerns of the in-memory store (long-poll `wait_for_messages`, producer-epoch
validation, stream closing) are intentionally not implemented here.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from typing import Any

from django.db import transaction
from django.db.models import Max

from rakaia.context import merge_provenance
from rakaia.types import StreamMessage

from .models import Stream, StreamEntry, StreamEvent, StreamOffsetWatermark

# StreamEvent.event_type is required metadata for the dashboard; raw stream
# appends carry no type, so they are recorded under a single stable label.
_APPEND_EVENT_TYPE = "append"

# Offsets are rendered zero-padded so they sort byte-wise lexicographically, as
# the Durable Streams protocol requires (§3, §5.2). 20 digits covers a
# BigAutoField's range (< 2**63). `read` still parses them numerically, so the
# padding is transparent to filtering.
_OFFSET_WIDTH = 20


def _fmt_offset(value: int) -> str:
    return f"{value:0{_OFFSET_WIDTH}d}"


class DjangoStreamStore:
    """A durable StreamStore backed by the django_rakaia ORM models."""

    def create(self, path: str, **_kwargs: Any) -> Stream:
        """Ensure a stream row exists (idempotent). Extra kwargs are ignored —
        the durable store does not model TTL/expiry/content-type.

        Unlike the in-memory `StreamStore`, a re-`create` with a *different*
        `content_type`/`ttl_seconds`/`expires_at`/`closed` never raises
        `ValueError`: the `Stream` model has no columns for those, so there is
        no stored config to conflict with. This is a deliberate, permanent
        divergence (not a gap to close) — see
        `tests/test_django_rakaia/test_django_store.py::
        test_create_ignores_conflicting_kwargs_instead_of_raising`.
        """
        stream, _ = Stream.objects.get_or_create(stream_id=path)
        return stream

    def append(self, path: str, data: bytes, options: Any = None) -> StreamEntry:
        """Append one JSON event, assigning the next monotonic offset.

        The event-sourcing envelope on `options` (an ``AppendOptions``) is
        persisted: ``label`` maps to ``event_type`` (a raw append keeps the
        stable ``"append"`` label) and ``metadata`` to the JSON column.

        Runs in a transaction so ``get_next_offset()`` can lock the stream row:
        concurrent appends serialize on offset allocation instead of racing to
        the same value and failing the ``unique_together(stream, offset)``
        constraint.

        Unlike the in-memory `StreamStore`, this never raises `ValueError` for
        a Stream-Seq conflict and never reports a closed stream (no raise, no
        `stream_closed`-style signal): there is no `closed` column and
        `options.seq` is not consulted. Stream closing and producer-seq
        fencing are live-protocol-server concerns, out of scope for the
        durable event-sourcing store (module docstring). This is a
        deliberate, permanent divergence — see
        `tests/test_django_rakaia/test_django_store.py::
        test_append_has_no_closed_stream_concept`.
        """
        with transaction.atomic():
            try:
                stream = Stream.objects.get(stream_id=path)
            except Stream.DoesNotExist as exc:
                raise KeyError(f"Stream not found: {path}") from exc

            label = getattr(options, "label", "") or ""
            metadata = merge_provenance(getattr(options, "metadata", None))
            event = StreamEvent.objects.create(
                data=json.loads(data),
                event_type=label or _APPEND_EVENT_TYPE,
                metadata=metadata or {},
                event_ts=getattr(options, "event_ts", None),
            )
            return StreamEntry.objects.create(
                stream=stream,
                event=event,
                offset=stream.get_next_offset(),
            )

    def append_many(
        self, path: str, events: Iterable[tuple[bytes, Any]]
    ) -> list[StreamEntry]:
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
        for a missing stream). A non-empty batch raises ``KeyError`` if the
        stream does not exist, like :meth:`append`.
        """
        items = list(events)
        if not items:
            return []

        with transaction.atomic():
            try:
                stream = Stream.objects.get(stream_id=path)
            except Stream.DoesNotExist as exc:
                raise KeyError(f"Stream not found: {path}") from exc

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
            return entries

    def read(
        self, path: str, offset: str | None = None
    ) -> tuple[list[StreamMessage], bool]:
        """Return ``(messages, up_to_date)`` ordered oldest-first.

        With no `offset`, returns every message; with one, returns the messages
        strictly after it. Raises `KeyError` if the stream does not exist.
        """
        try:
            stream = Stream.objects.get(stream_id=path)
        except Stream.DoesNotExist as exc:
            raise KeyError(f"Stream not found: {path}") from exc

        entries = stream.entries.select_related("event").order_by("offset")
        if offset not in (None, "", "-1"):
            entries = entries.filter(offset__gt=int(offset))  # type: ignore[arg-type]

        messages = [
            StreamMessage(
                data=json.dumps(entry.event.data).encode("utf-8"),
                offset=_fmt_offset(entry.offset),
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
        """Return the stream row, or None if it does not exist."""
        return Stream.objects.filter(stream_id=path).first()

    def has(self, path: str) -> bool:
        return Stream.objects.filter(stream_id=path).exists()

    def delete(self, path: str) -> bool:
        """Delete a stream and its entries. Returns whether it existed."""
        deleted, _ = Stream.objects.filter(stream_id=path).delete()
        return deleted > 0

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
        return _fmt_offset(max(entries_max or 0, watermark_high or 0))

    def list_paths(self) -> list[str]:
        return list(Stream.objects.values_list("stream_id", flat=True))
