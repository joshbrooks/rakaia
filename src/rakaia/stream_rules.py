"""A stream's lifecycle rules — expiry, re-creating, closing, deleting — decided
once for every store.

The counterpart of `rakaia.append_decision`, for everything about a stream that
is not an append (#322). A store's job is the same as for an append: load the
facts, ask, persist the answer.

**Permanent streams.** A store built with ``permanent=True`` keeps every stream
for good: nothing expires, a create may not ask for a TTL or an expiry, a stream
given one earlier reports none, a client's ``DELETE`` is refused, and so is a
``delete()`` from Python unless it passes ``force=True``. Every rule below takes
the flag, so the stores cannot disagree about what it means.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from .json_mode import normalize_content_type
from .producer import validate_producer
from .types import (
    ClosedBy,
    DeleteNotAllowed,
    ExpiryNotAllowed,
    ProducerAccepted,
    ProducerDuplicate,
    ProducerState,
    ProducerStreamClosed,
    ProducerValidationResult,
)

_log = logging.getLogger("rakaia.stream_rules")


def is_expired(
    ttl_seconds: int | None,
    expires_at: str | None,
    last_activity_at: float,
    *,
    now: float,
    permanent: bool = False,
) -> bool:
    """Whether a stream has aged out, by sliding TTL or by absolute expiry.

    Stream-TTL is a window anchored on ``last_activity_at``, which reads, writes
    and closes move. Stream-Expires-At is a deadline that does not move; a naive
    timestamp is taken as UTC, and a parsed offset is kept. A malformed or empty
    one cannot be evaluated, so the stream is treated as non-expiring, and that
    is logged so the bad data is observable.
    """
    if permanent:
        return False
    if ttl_seconds is not None and now - last_activity_at > ttl_seconds:
        return True
    if expires_at is not None:
        try:
            expires = datetime.fromisoformat(expires_at.replace("Z", "+00:00"))
        except ValueError:
            _log.debug("Ignoring malformed expires_at %r", expires_at)
            return False
        if expires.tzinfo is None:
            expires = expires.replace(tzinfo=timezone.utc)
        if now > expires.timestamp():
            return True
    return False


def check_expiry_allowed(
    path: str,
    ttl_seconds: int | None,
    expires_at: str | None,
    *,
    permanent: bool = False,
) -> None:
    """Raise `ExpiryNotAllowed` if a permanent store is asked for an expiry.

    Checked before anything is written, including on an idempotent re-create.
    """
    if permanent and (ttl_seconds is not None or expires_at):
        raise ExpiryNotAllowed(
            f"Streams are permanent: {path} cannot be created with a TTL or an expiry"
        )


@dataclass(frozen=True)
class StreamConfig:
    """What a create asks for, and what an existing stream was created with."""

    content_type: str | None = None
    ttl_seconds: int | None = None
    expires_at: str | None = None
    closed: bool = False


def same_config(
    existing: StreamConfig, requested: StreamConfig, *, permanent: bool = False
) -> bool:
    """Whether re-creating `existing` with `requested` is the idempotent case.

    Content types compare normalised. On a permanent store an expiry the stream
    was given before the switch compares as none, which is also what `get`
    reports for it.
    """
    ttl, expires = reported_expiry(
        existing.ttl_seconds, existing.expires_at, permanent=permanent
    )
    return (
        normalize_content_type(requested.content_type)
        == normalize_content_type(existing.content_type)
        and requested.ttl_seconds == ttl
        and requested.expires_at == expires
        and requested.closed == existing.closed
    )


def reported_expiry(
    ttl_seconds: int | None, expires_at: str | None, *, permanent: bool = False
) -> tuple[int | None, str | None]:
    """The TTL and expiry `get` reports: none at all on a permanent store, which
    serves a stream given one earlier as live."""
    if permanent:
        return None, None
    return ttl_seconds, expires_at


def check_protocol_delete(path: str, *, permanent: bool = False) -> None:
    """Raise `DeleteNotAllowed` if a client may not delete `path`."""
    if permanent:
        raise DeleteNotAllowed(
            f"Streams are permanent: {path} cannot be deleted over the protocol"
        )


def check_delete(path: str, *, permanent: bool = False, force: bool = False) -> None:
    """Raise `DeleteNotAllowed` for a `delete()` from Python on a permanent
    store, unless the caller passed ``force=True``.

    `force` does nothing while the switch is off, so a caller may pass it
    unconditionally rather than branch on a setting.
    """
    if permanent and not force:
        raise DeleteNotAllowed(
            f"Streams are permanent: {path} can only be deleted with "
            f"delete(..., force=True)"
        )


@dataclass(frozen=True)
class CloseVerdict:
    """What a close does.

    ``close`` is whether the stream becomes closed now; ``already_closed`` is
    whether it already was. ``closed_by`` is the producer tuple to record when a
    fenced close lands. ``producer_result`` is the fencing outcome for a fenced
    close — the state to commit when it is a `ProducerAccepted` and ``close`` —
    and None for a plain one. ``touch`` is whether the close counts as activity
    for the TTL window: every plain close does, already closed or not, and a
    fenced close only when it lands.
    """

    close: bool
    already_closed: bool
    producer_result: ProducerValidationResult | None = None
    closed_by: ClosedBy | None = None
    touch: bool = False


def decide_close(
    *,
    closed: bool,
    closed_by: ClosedBy | None,
    producer: tuple[str, int, int] | None = None,
    producer_state: ProducerState | None = None,
    now: float,
) -> CloseVerdict:
    """Decide a close, plain or fenced.

    A plain close is idempotent: an already-closed stream reports the same
    final offset rather than failing. A fenced close is admitted on the same
    terms as a fenced append with no body — already closed is reported first, as
    a duplicate for a retry of the closing tuple and as `ProducerStreamClosed`
    for any other, and only then is the producer fenced.
    """
    if producer is None:
        return CloseVerdict(close=not closed, already_closed=closed, touch=True)

    producer_id, epoch, seq = producer
    if closed:
        if closed_by == ClosedBy(producer_id=producer_id, epoch=epoch, seq=seq):
            return CloseVerdict(
                close=False,
                already_closed=True,
                producer_result=ProducerDuplicate(last_seq=seq),
            )
        return CloseVerdict(
            close=False, already_closed=True, producer_result=ProducerStreamClosed()
        )
    result = validate_producer(producer_state, producer_id, epoch, seq, now)
    if not isinstance(result, ProducerAccepted):
        return CloseVerdict(close=False, already_closed=False, producer_result=result)
    return CloseVerdict(
        close=True,
        already_closed=False,
        producer_result=result,
        closed_by=ClosedBy(producer_id=producer_id, epoch=epoch, seq=seq),
        touch=True,
    )
