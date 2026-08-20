"""
Reading an existing Django table as a *virtual* stream.

`ModelStreamReader` lets `replay()` treat rows you already have as if they were
events: it reads them in a defined order from a queryset and encodes each one
with a `to_payload` callable you supply. Use it when a durable, ordered
source-of-truth already exists and you do not want to mirror it into a stream.

**It carries no envelope, and that is correct.** The rows it reads were never
events, so there is no recorded label, actor or event time to carry — it
synthesises a payload rather than reproducing one. Anything that reads the
envelope (`envelope_actor`, `merge_replay(order_key=...)`,
`history_effects(version_of=...)`) has nothing to read here by construction.

**If your events are actually stored as events, use the store.** For the
`Stream`/`StreamEvent`/`StreamEntry` rows that `@stream_model` and
`create_stream_event` populate, read with `DjangoStreamStore` — it carries the
label, metadata, event timestamp and offset those rows genuinely hold, inverts
`payload_encoding`, takes a `using=` database alias, and is held to
`tests/server_store_contract.py`. A second reader over those same tables used to
live here and returned the payload alone, so a replay through it silently lost
all of that; it had no callers and was deleted (#186).

Satisfies the subset of the `StreamStore` interface `rakaia.replay.replay` calls:
`read(path)` -> `(list[message], bool)`, and `has(path)`.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Any

from django.db.models import QuerySet


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
        self,
        path: str,
        offset: str | None = None,  # noqa: ARG002
    ) -> tuple[list[_ReaderMessage], bool]:
        """Return (messages, up_to_date) for the given stream path."""
        qs = self._queryset_for(path).order_by(self._order_by)
        messages = [
            self._encode(obj) for obj in qs.iterator(chunk_size=self._chunk_size)
        ]
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
