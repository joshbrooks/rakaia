"""
Structural protocols for pluggable storage backends.

`replay()` only ever *reads* a stream, so it depends on the narrow
`ReadableStore` protocol rather than the concrete in-memory `StreamStore`. Any
backend that can return a stream's messages in order — the in-memory
`StreamStore` or a durable, DB-backed store — satisfies it.
"""

from __future__ import annotations

from typing import Protocol, runtime_checkable

from .types import StreamMessage


@runtime_checkable
class ReadableStore(Protocol):
    """A store `replay()` can read events from."""

    def read(
        self, path: str, offset: str | None = None
    ) -> tuple[list[StreamMessage], bool]:
        """Return ``(messages, up_to_date)`` for `path`, ordered oldest-first.

        With no `offset`, returns every message; with an `offset`, returns the
        messages after it. Raises `KeyError` if the stream does not exist.
        """
        ...
