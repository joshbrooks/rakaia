"""
Adapters that expose Django data to rakaia's replay pipeline.

Two readers, picked according to where your event log lives:

- `ModelStreamReader` treats an existing Django table as a *virtual*
  stream: replay reads ordered rows directly from the queryset, encoding
  each as a JSON event. Use when you already have a durable, ordered
  source-of-truth and don't want to mirror it.

- `DjangoStreamReader` reads from the `Stream`/`StreamEvent`/`StreamEntry`
  tables that `@stream_model` populates. Use when you've decided to mirror
  saves into a real Rakaia stream (live SSE broadcast, multi-consumer
  cursors).

Both satisfy the same subset of the `StreamStore` interface
(`read(path)` -> `(list[message], bool)`, `has(path)`) that
`rakaia.replay.replay` calls.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from django.db.models import QuerySet

from django_rakaia.models import Stream


@dataclass(frozen=True)
class _ReaderMessage:
    """Minimal message shape that satisfies what `rakaia.replay` reads."""

    data: bytes


class ModelStreamReader:
    """
    A read-only adapter that satisfies the subset of the `StreamStore`
    interface that `rakaia.replay.replay` uses.

    Args:
        queryset_for: Maps a stream path (e.g. "submissions:SF_1_2") to the
            queryset that backs it. Typically returns
            `Submission.objects.filter(form_type=...)`.
        order_by: Field name whose ascending order defines the stream's
            sequence (e.g. "seq", "id", "created_at"). Must be monotonic.
        to_payload: Callable converting a model instance to a JSON-serialisable
            dict. The dict's `schema_version` (default 1) determines which
            upcasters run.
        chunk_size: Number of rows fetched per DB roundtrip (default 1000).

    Notes:
        Materialises one chunk at a time but loads the full stream into
        memory before returning from `read()`. For very large streams,
        prefer narrowing the queryset (e.g. with `start_seq`-equivalent
        filtering) before passing it in.
    """

    def __init__(
        self,
        *,
        queryset_for: Callable[[str], QuerySet[Any]],
        order_by: str,
        to_payload: Callable[[Any], dict[str, Any]],
        chunk_size: int = 1000,
    ) -> None:
        self._queryset_for = queryset_for
        self._order_by = order_by
        self._to_payload = to_payload
        self._chunk_size = chunk_size

    def read(
        self, path: str, offset: str | None = None  # noqa: ARG002
    ) -> tuple[list[_ReaderMessage], bool]:
        """Return (messages, up_to_date) for the given stream path."""
        qs = self._queryset_for(path).order_by(self._order_by)
        messages = [self._encode(obj) for obj in qs.iterator(chunk_size=self._chunk_size)]
        return messages, True

    def has(self, path: str) -> bool:  # noqa: ARG002
        """Always True — the queryset's stream is considered to exist."""
        return True

    def _encode(self, obj: Any) -> _ReaderMessage:
        payload = self._to_payload(obj)
        return _ReaderMessage(data=json.dumps(payload).encode("utf-8"))

    # ------------------------------------------------------------------
    # Convenience helpers for callers
    # ------------------------------------------------------------------

    def iter_payloads(self, path: str) -> Iterable[dict[str, Any]]:
        """Yield decoded payloads — useful for diff harnesses and dry-runs."""
        qs = self._queryset_for(path).order_by(self._order_by)
        for obj in qs.iterator(chunk_size=self._chunk_size):
            yield self._to_payload(obj)


# =============================================================================
# DjangoStreamReader
# =============================================================================


class DjangoStreamReader:
    """
    Read events from the `Stream`/`StreamEvent`/`StreamEntry` tables that
    `@stream_model` (or `create_stream_event`) populates.

    Each `StreamEntry`'s `event.data` (the JSONField written by
    `to_dataclass(instance)`) becomes one event. Entries are read in
    ascending `offset` order.

    Note on sequence numbers: `replay()` uses the position of an event
    within the iteration (0, 1, 2, ...) as its seq for handler-version
    dispatch — *not* the underlying Django `offset` column. So a handler
    registered with `effective_from=0` matches the first event read from
    the stream, regardless of its `StreamEntry.offset` value.

    Args:
        chunk_size: Rows fetched per DB roundtrip. Defaults to 1000.
    """

    def __init__(self, *, chunk_size: int = 1000) -> None:
        self._chunk_size = chunk_size

    def read(
        self, path: str, offset: str | None = None  # noqa: ARG002
    ) -> tuple[list[_ReaderMessage], bool]:
        """Return (messages, up_to_date) for the given stream id."""
        try:
            stream = Stream.objects.get(stream_id=path)
        except Stream.DoesNotExist:
            return [], True
        entries = (
            stream.entries.select_related("event")
            .order_by("offset")
            .iterator(chunk_size=self._chunk_size)
        )
        messages = [
            _ReaderMessage(data=json.dumps(e.event.data).encode("utf-8"))
            for e in entries
        ]
        return messages, True

    def has(self, path: str) -> bool:
        return Stream.objects.filter(stream_id=path).exists()
