"""
Structural protocols for pluggable storage backends.

`replay()` only ever *reads* a stream, so it depends on the narrow
`ReadableStore` protocol rather than the concrete in-memory `StreamStore`. Any
backend that can return a stream's messages in order — the in-memory
`StreamStore` or a durable, DB-backed store — satisfies it.
"""

from __future__ import annotations

from typing import Any, Protocol, runtime_checkable

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


@runtime_checkable
class ProjectionReader(Protocol):
    """Read-only view over materialised projections.

    During staged replay, `replay()` passes a reader to every stage > 0 handler
    as its second argument, so the handler can resolve facts that earlier stages
    produced (e.g. link a form to the reference entity another form created).
    `replay()` never calls these methods itself — it only forwards the reader —
    so backends are free to shape it to their storage; the Django integration
    provides one over `apps.get_model(...).objects`. Because a reader only ever
    reads committed projections (themselves a pure function of the log), a
    handler using it stays deterministic.
    """

    def get(self, model_label: str, /, **lookup: Any) -> Any | None:
        """Return the single row matching `lookup`, or None."""
        ...

    def filter(self, model_label: str, /, **lookup: Any) -> Any:
        """Return the rows matching `lookup` (a queryset-like iterable)."""
        ...

    def query(self, model_label: str, /) -> Any:
        """Return all rows of the model (a queryset-like iterable)."""
        ...
