"""Per-consumer stream cursors: read a stream incrementally with rewind detection.

A durable stream already carries monotonic offsets, so "give me what changed
since I last looked" is just *remember the last offset, read after it*. This is
the streams-native form of a hand-rolled ``last_change_id`` sync API (a
per-consumer watermark + a "the log was rebuilt, resync" signal).

`poll` is store-agnostic and pure with respect to the store: it derives its
status entirely from the stored cursor versus the current head, so replaying the
same reads yields the same result. It works over any `ReadableStore` that also
exposes ``get_current_offset`` — both the in-memory `StreamStore` and the Django
`DjangoStreamStore` qualify.

The consumer holds the cursor; `django_rakaia` provides a durable place to keep
it (`ConsumerCursor`) plus `load_cursor`/`commit_cursor` helpers, so a consumer
survives restarts and resumes exactly where it left off.

Rewind detection is offset-based: if the stored cursor sorts *after* the current
head, the log shrank beneath it, so the consumer resets and re-reads from the
start. Store offsets are **globally monotonic** — a stream recreated at a path
issues offsets strictly greater than any it issued before (#34, the EventStoreDB
``$all`` / Kinesis model) — so a normal delete+recreate can never collide with a
stale cursor: the recreated content sorts *past* it and is delivered as an
ordinary ``advanced``. ``rewound`` is therefore a **defensive** status: it fires
only for a genuinely truncated log or a cursor carried over from a different
stream, where the head really does sort before the cursor.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from .protocols import CursorStore
from .types import StreamMessage

PollStatus = Literal["fresh", "advanced", "caught_up", "rewound", "absent"]
"""Outcome of a `poll`:

* ``fresh`` — first poll (no prior cursor); every message returned.
* ``advanced`` — new messages since the cursor; the delta returned.
* ``caught_up`` — no messages live above the cursor; nothing new. Covers both
  a cursor at the head and a cursor below a head that only reflects a persisted
  high-water (a stream recreated but not yet re-appended, #34).
* ``rewound`` — cursor sorts past the head (log truncated/rebuilt); re-read from
  the start. The consumer should reset any derived state before applying.
* ``absent`` — the stream does not exist; nothing returned.
"""

# CursorStore lives in rakaia.protocols alongside the other store-facing seams.


@dataclass(frozen=True)
class Poll:
    """The result of polling a stream from a cursor."""

    messages: list[StreamMessage]
    """The messages to apply, oldest-first (empty when caught up or absent)."""

    cursor: str | None
    """The new watermark to persist — the last consumed offset. Unchanged when
    caught up; ``None`` when the stream is absent."""

    status: PollStatus

    @property
    def rewound(self) -> bool:
        """The log was rebuilt beneath the cursor; reset before applying."""
        return self.status == "rewound"

    @property
    def caught_up(self) -> bool:
        """No new messages since the cursor."""
        return self.status == "caught_up"


def poll(store: CursorStore, path: str, cursor: str | None) -> Poll:
    """Read `path` forward from `cursor`, detecting a rewound log.

    Args:
        store: a `ReadableStore` that also exposes ``get_current_offset``.
        path: the stream to poll.
        cursor: the last offset this consumer applied, or ``None`` on first poll.

    Returns:
        A `Poll`. Persist ``.cursor`` only after the messages are applied, so a
        crash mid-apply re-delivers rather than skips (at-least-once). A
        ``rewound`` result re-reads from the start; reset derived state first.
    """
    head = store.get_current_offset(path)
    if head is None:
        return Poll(messages=[], cursor=None, status="absent")

    if cursor is None:
        messages, _ = store.read(path)
        return Poll(messages=messages, cursor=_tail(messages, head), status="fresh")

    if _after(cursor, head):
        # The cursor points beyond the current head: the log was truncated or
        # rebuilt shorter. Re-read from the start and signal the reset.
        messages, _ = store.read(path)
        return Poll(messages=messages, cursor=_tail(messages, head), status="rewound")

    if cursor == head:
        return Poll(messages=[], cursor=cursor, status="caught_up")

    messages, _ = store.read(path, cursor)
    if not messages:
        # The head sorts after the cursor, yet nothing lives above it: the head
        # reflects a persisted high-water for a stream recreated but not yet
        # re-appended (globally-monotonic offsets, #34). There is no delta to
        # apply, so this is `caught_up` — not an `advanced` with an empty delta,
        # which would falsely signal new content to a status-branching consumer.
        return Poll(messages=[], cursor=cursor, status="caught_up")
    return Poll(messages=messages, cursor=_tail(messages, cursor), status="advanced")


def _tail(messages: list[StreamMessage], fallback: str) -> str:
    """The last message's offset, or `fallback` when there are no messages."""
    return messages[-1].offset if messages else fallback


def _offset_key(offset: str) -> tuple[int, ...] | None:
    """Parse a rakaia offset into an orderable tuple of ints, or None.

    Both stores now emit zero-padded, lexicographically-sortable offsets (the
    in-memory ``{seq}_{bytes}`` and the Django store's padded integer, #34), so
    plain string order already matches. Comparing the parsed ints is kept as a
    defensive fallback that also tolerates a non-padded offset (``"9"`` vs
    ``"10"``) should any store or older cursor produce one."""
    try:
        return tuple(int(part) for part in offset.split("_"))
    except ValueError:
        return None


def _after(a: str, b: str) -> bool:
    """True if offset `a` sorts strictly after `b` (chronologically later)."""
    ka, kb = _offset_key(a), _offset_key(b)
    if ka is None or kb is None or len(ka) != len(kb):
        return a > b  # unparseable/mismatched shape: fall back to lexicographic
    return ka > kb
