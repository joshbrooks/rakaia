"""
Structural protocols for pluggable storage and projection backends — the
extension seams of the event-sourcing framework (ADR 0002).

`replay()` only ever *reads* a stream, so it depends on the narrow
`ReadableStore` protocol rather than the concrete in-memory `StreamStore`.
Producers and the meta-stream registry additionally *write*, captured by
`WritableStore`. Subscribers additionally need the head offset, captured by
`CursorStore`. Staged handlers read committed projections through
`ProjectionReader`. Any backend satisfying the relevant protocol — the in-memory
`StreamStore`, a durable DB-backed store, or a third-party one — plugs in.

All store-facing protocols live here so they read as one coherent seam:
`ReadableStore` (read), `WritableStore` (+ create/append/has), `CursorStore`
(+ get_current_offset).
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
class WritableStore(ReadableStore, Protocol):
    """A store the event-sourcing framework both writes to and reads from.

    This is the **framework** store surface — what producers and the meta-stream
    registry rely on: create a stream, append enveloped events, check existence,
    and read them back (inherited from `ReadableStore`). It deliberately excludes
    the Durable Streams **protocol-server** lifecycle (producer epoch/seq
    fencing, close, TTL, long-poll/`wait_for_messages`); those live on the
    standalone server's store and are not required to back `replay()`/projections.
    See ADR 0002.

    Return types are intentionally loose (`Any`): the in-memory `StreamStore`
    returns rich protocol types (`Stream`/`AppendResult`) while `DjangoStreamStore`
    returns ORM rows (`StreamEntry`). What both guarantee is the read-back
    behaviour exercised by the shared conformance suite (`tests/store_contract.py`).
    """

    def has(self, path: str) -> bool:
        """Whether a stream exists at `path`."""
        ...

    def create(self, path: str) -> Any:
        """Idempotently create the stream at `path`. `append` requires it to exist.

        Only idempotent creation by `path` is part of the contract. Concrete
        stores may accept extra keyword-only options (content type, TTL, …) via
        their own richer signatures — deliberately *not* on this protocol, so a
        backend that ignores such an option (e.g. `DjangoStreamStore`) can't be
        called with it through the `WritableStore` type and silently no-op.
        """
        ...

    def append(self, path: str, data: bytes, options: Any = None) -> Any:
        """Append one (optionally enveloped) event to `path`.

        Three possible outcomes, only the first of which is guaranteed across
        every backend:

        - Raises `KeyError` if the stream does not exist (create it first) —
          the one guaranteed exception every `WritableStore` must raise.
        - May raise `ValueError` for a backend's own validation (the in-memory
          `StreamStore` raises this on a content-type or Stream-Seq conflict);
          `DjangoStreamStore` has no such concept and never raises it.
        - May return normally with a closed-stream signal instead of raising
          (the in-memory `StreamStore`'s `AppendResult.stream_closed=True`,
          with `message=None`, when appending to a closed stream);
          `DjangoStreamStore` has no closed-stream concept and never signals
          this.

        Depend only on the `KeyError` here; treat the `ValueError` and
        `stream_closed=True` cases as backend-specific, not part of the
        `WritableStore` contract (see `tests/store_contract.py` for what is and
        isn't asserted across backends).

        The envelope — `label` and `metadata` on `options` (an `AppendOptions`) —
        is recorded and read back by `read`; ambient `provenance()` merges under
        explicit metadata.
        """
        ...


@runtime_checkable
class CursorStore(ReadableStore, Protocol):
    """A `ReadableStore` that also exposes its current head offset — what a
    subscriber (`poll()`) needs to detect new messages and a rewound log."""

    def get_current_offset(self, path: str) -> str | None:
        """The offset after the last message, or None if the stream is absent."""
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
