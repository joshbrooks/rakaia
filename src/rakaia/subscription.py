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

`consume` is the loop around `poll` that rakaia documented and did not have:
poll, apply, record any outcome, commit the cursor — in that order, with the
record written outside whatever transaction the apply used and the commit last.
See ADR 0007 for why each half of that order is load-bearing.

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

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Literal

from .offsets import after as offsets_after
from .outcomes import Outcome
from .protocols import CursorStore, OutcomeStore
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


def _after(a: str, b: str) -> bool:
    """True if offset `a` sorts strictly after `b` (chronologically later).

    Delegates to `offsets.after`, which raises `ForeignOffset` rather than
    guessing when the two tokens are recognisably from different stores. This used
    to hold its own parse-and-compare, with a lexicographic fallback for the case
    it could not line up — and for that one pair the fallback answered *wrongly*
    rather than uncertainly: a padded compound offset sorts *above* a padded plain
    one, decided by the ``'_'`` at its seventeenth character. See `rakaia.offsets`
    for why there is nothing correct to return there, and why byte order is still
    the right answer for a store this library does not recognise.
    """
    return offsets_after(a, b)


OnErrorPolicy = Literal["skip", "halt"]
"""What the consume loop does with an event whose apply raised.

* ``skip`` — record the outcome, advance past it, keep going. What a continuous
  consumer wants: one poisoned event must not stop a live stream, and the record
  is how it is found again.
* ``halt`` — record the outcome and stop, leaving the cursor *below* the event
  that failed. What a rebuild wants: a rebuild's whole claim is that the
  projection it produced is derived from every event, and one silently skipped
  event makes that claim false while the run still reports success.

There is no default, for the same reason `on_drift` has none: the two modes have
opposite invariants, and a shared default is right for one of them and quietly
wrong for the other (ADR 0007, Decision 5).
"""


@dataclass(frozen=True)
class Consumed:
    """What one pass of the consume loop did."""

    status: PollStatus
    """The poll's own verdict — ``rewound`` still means reset derived state."""

    applied: int
    """How many messages were handed to `apply` and did not raise."""

    outcomes: tuple[Outcome, ...]
    """Every outcome recorded this pass, in the order recorded: the ones `apply`
    returned as well as the ones the loop wrote for an exception."""

    cursor: str | None
    """The watermark as it stands now — what was last committed, not what was
    polled. Below the poll's cursor when the pass halted."""

    halted: bool
    """``on_error="halt"`` stopped the pass. Messages after the failure were not
    applied and are still pending."""


def consume(
    store: CursorStore,
    path: str,
    apply: Callable[[StreamMessage], Iterable[Outcome] | None],
    *,
    consumer: str,
    on_error: OnErrorPolicy,
    cursor: str | None = None,
    commit: Callable[[str], None] | None = None,
    outcomes: OutcomeStore | None = None,
    subject_of: Callable[[StreamMessage], str] | None = None,
    sequence_of: Callable[[StreamMessage], str] | None = None,
) -> Consumed:
    """Poll `path`, apply each message, record any outcome, then commit.

    The loop rakaia documented and never had. Every consumer was writing
    ``poll`` / apply / ``commit_cursor`` by hand, which is why there was nowhere
    to record that an apply had failed — and why an event that never applied was
    indistinguishable from one that did (ADR 0007).

    Three properties are the point of it, and each is a defect somewhere else:

    **An outcome is written outside the executor's transaction.** `apply` is
    called, and only once it has returned or raised does the loop record
    anything. A `DjangoExecutor` wraps its batch in ``transaction.atomic``, so a
    record written *inside* rolls back with the batch whose failure it exists to
    record — the alternative this ADR rejected.

    **Do not call `consume` from inside a transaction of your own.** The same
    rollback swallows the outcome again, one frame further out, and this module
    cannot see it: `rakaia` is stdlib-only and knows nothing about a Django
    atomic block. Measured on a database-backed outcome store, a caller-held
    transaction that rolls back leaves **0 of 1** recorded outcomes behind. It is
    a constraint on you, not a guard here.

    **The cursor is committed last, one message at a time.** A cursor committed
    before the side effect lands is at-most-once with silent loss, and under
    Decision 3 it is worse than lost: success writes no record, so an unapplied
    event below the cursor reads as an event that worked. Committing per message
    is what makes ``halt`` mean anything — the watermark stays below the event
    that failed, so the event is still pending and is delivered again. Redelivery
    is expected either way; **apply must be idempotent**.

    **`on_error` is explicit.** See `OnErrorPolicy`: ``"skip"`` for a continuous
    consumer, ``"halt"`` for a rebuild, never inferred from anything.

    Args:
        store: a `ReadableStore` that also exposes ``get_current_offset``.
        path: the stream to consume.
        apply: called once per message. Return an iterable of `Outcome` to record
            facts the apply itself decided — a reducer computing a value from a
            population with a hole in it is the case this exists for — or
            ``None`` when there is nothing to say. Returning outcomes is not a
            failure: the message still counts as applied and the cursor still
            advances.
        consumer: names who this cursor and these outcomes belong to.
        on_error: ``"skip"`` or ``"halt"``. No default, deliberately.
        cursor: the last committed offset, or ``None`` on first poll.
        commit: called with each offset once its message is applied and any
            outcome is recorded. Omit it to run the loop without persisting a
            watermark — the returned ``cursor`` still says where it got to.
        outcomes: where to keep outcomes. Omit it and nothing is recorded, which
            is the pre-ADR behaviour and is offered only so a caller can adopt
            the loop before it has a store.
        subject_of: what an outcome for a message is *about*. Defaults to the
            message's offset, which is honest here because every event this loop
            sees is already in the log; a refused event that never reached the
            log is not this loop's to record.
        sequence_of: what a message is ordered *within*. Defaults to the subject.

    Returns:
        A `Consumed` describing the pass.
    """
    result = poll(store, path, cursor)
    subject_of = subject_of or (lambda message: message.offset)
    sequence_of = sequence_of or subject_of

    committed = cursor
    recorded: list[Outcome] = []
    applied = 0

    def _record(outcome: Outcome) -> None:
        # Outside the executor's transaction by construction: `apply` has already
        # returned or raised by the time anything gets here.
        recorded.append(outcome)
        if outcomes is not None:
            outcomes.record(outcome)

    for message in result.messages:
        try:
            emitted = apply(message)
        except Exception as exc:
            _record(
                Outcome(
                    consumer=consumer,
                    stream_path=path,
                    subject=subject_of(message),
                    offset=message.offset,
                    sequence_key=sequence_of(message),
                    # It is in the log and it was not applied, so a replay
                    # recovers it — the first row of ADR 0007's recovery table.
                    stage="project",
                    status="failed",
                    reasons=(type(exc).__name__,),
                )
            )
            if on_error == "halt":
                # The cursor stays where it was, *below* this message, so the
                # message is still pending. Committing here would convert
                # "unapplied" into "succeeded" the moment the outcome was read.
                return Consumed(
                    status=result.status,
                    applied=applied,
                    outcomes=tuple(recorded),
                    cursor=committed,
                    halted=True,
                )
            # "skip" advances past it. The record is what makes that safe to do.
            _commit(commit, message.offset)
            committed = message.offset
            continue

        applied += 1
        for outcome in emitted or ():
            _record(outcome)
        _commit(commit, message.offset)
        committed = message.offset

    # `caught_up` and `absent` applied nothing, so they committed nothing and
    # `committed` is still the watermark they were handed. That is deliberate for
    # `absent` in particular, where `poll` reports a cursor of `None`: a stream
    # that is not there is no reason to forget how far this consumer once got.
    return Consumed(
        status=result.status,
        applied=applied,
        outcomes=tuple(recorded),
        cursor=committed,
        halted=False,
    )


def _commit(commit: Callable[[str], None] | None, offset: str) -> None:
    """Persist a watermark, if the caller gave somewhere to persist it."""
    if commit is not None:
        commit(offset)
