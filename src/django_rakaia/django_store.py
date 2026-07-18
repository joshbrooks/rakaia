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
from typing import Any

from rakaia.types import StreamMessage

from .models import Stream, StreamEntry, StreamEvent

# StreamEvent.event_type is required metadata for the dashboard; raw stream
# appends carry no type, so they are recorded under a single stable label.
_APPEND_EVENT_TYPE = "append"


class DjangoStreamStore:
    """A durable StreamStore backed by the django_rakaia ORM models."""

    def create(self, path: str, **_kwargs: Any) -> Stream:
        """Ensure a stream row exists (idempotent). Extra kwargs are ignored —
        the durable store does not model TTL/expiry/content-type."""
        stream, _ = Stream.objects.get_or_create(stream_id=path)
        return stream

    def append(self, path: str, data: bytes, options: Any = None) -> StreamEntry:  # noqa: ARG002
        """Append one JSON event, assigning the next monotonic offset."""
        try:
            stream = Stream.objects.get(stream_id=path)
        except Stream.DoesNotExist as exc:
            raise KeyError(f"Stream not found: {path}") from exc

        event = StreamEvent.objects.create(
            data=json.loads(data),
            event_type=_APPEND_EVENT_TYPE,
        )
        return StreamEntry.objects.create(
            stream=stream,
            event=event,
            offset=stream.get_next_offset(),
        )

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
                offset=str(entry.offset),
                timestamp=entry.created_at.timestamp(),
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
        """The latest offset as a string, or None if the stream is absent."""
        stream = Stream.objects.filter(stream_id=path).first()
        if stream is None:
            return None
        latest = stream.entries.order_by("-offset").values_list("offset", flat=True)
        return str(latest[0]) if latest else "0"

    def list_paths(self) -> list[str]:
        return list(Stream.objects.values_list("stream_id", flat=True))
