"""Whether an append is allowed, decided once for every store.

`rakaia.producer` already does this for the *fencing* rules, and the two stores
share it, so they cannot drift on epoch/seq. The rest of the admission sequence —
closed? content-type? fence? Stream-Seq? — was restated in both adapters, and
they had already drifted: `StreamStore.append` validated producer options inline
and recognised the idempotent close-duplicate, while `DjangoStreamStore.append`
ignored `options.producer_id` entirely, all while its docstring claimed
"outcomes, all now matching the in-memory store".

Nothing caught that because every producer test routed through
`append_with_producer`. `WritableStore.append` is public and its `AppendOptions`
carries those fields, so a consumer calling it directly got adapter-dependent
behaviour.

This module owns the sequence. A store's job becomes: gather the facts, ask, then
persist — so a new backend implements *storage*, not protocol semantics.

**The order is the subtle part** and is the reason this is worth centralising:

1. **Closed** is checked first — before fencing, so a producer whose sequence is
   also wrong is told the stream is closed rather than told to fix a sequence
   and retry a write that can never land. A matching `closed_by` tuple is
   reported as a duplicate rather than a bare refusal, so a producer retrying
   the append that closed the stream can tell "my close landed" from "someone
   else closed this"; any other tuple gets `ProducerStreamClosed`, the same
   answer the close paths give.
2. **Content type** next: a mismatch is a caller error regardless of fencing.
3. **Producer fencing before Stream-Seq.** A retried append carries the same
   `Stream-Seq` it did the first time, so checking Stream-Seq first would raise
   `SequenceConflict` on precisely the retry that fencing exists to absorb.
4. **Stream-Seq** last, as a plain monotonicity conflict.

Conflicts are *raised* (`ContentTypeMismatch`, `SequenceConflict`) because they
are caller errors; fencing outcomes are *returned* because they are protocol
results carrying their own statuses and headers.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .json_mode import normalize_content_type
from .producer import validate_producer
from .types import (
    ClosedBy,
    ContentTypeMismatch,
    ProducerDuplicate,
    ProducerState,
    ProducerStreamClosed,
    ProducerValidationResult,
    SequenceConflict,
)


@dataclass(frozen=True)
class StreamFacts:
    """What the decision needs to know about a stream, with no store attached.

    A deliberately small surface: whatever a backend holds, these are the only
    fields the admission rules read.
    """

    closed: bool = False
    closed_by: ClosedBy | None = None
    content_type: str | None = None
    last_seq: str | None = None


@dataclass(frozen=True)
class AppendVerdict:
    """The decision. ``write`` is the only field a caller must branch on.

    ``producer_result`` is carried whether or not the write is allowed: on a
    refusal it is the fencing outcome to report, and on an acceptance it is the
    state to commit *after* the write lands (or None for an unfenced append).
    """

    write: bool
    stream_closed: bool = False
    producer_result: ProducerValidationResult | None = None


def decide_append(
    facts: StreamFacts,
    opts: Any,
    *,
    producer_state: ProducerState | None = None,
    now: float,
) -> AppendVerdict:
    """Decide whether this append may be written. See the module docstring for
    the ordering and why it matters.

    `producer_state` is the last known state for ``opts.producer_id`` — the one
    fact the caller must look up, since only the store knows where it lives.
    """
    producer_id = getattr(opts, "producer_id", None)
    producer_epoch = getattr(opts, "producer_epoch", None)
    producer_seq = getattr(opts, "producer_seq", None)

    # 1. Closed — including the idempotent re-send of the closing append.
    if facts.closed:
        closed_by = facts.closed_by
        if (
            producer_id is not None
            and closed_by is not None
            and closed_by.producer_id == producer_id
            and closed_by.epoch == producer_epoch
            and closed_by.seq == producer_seq
        ):
            return AppendVerdict(
                write=False,
                stream_closed=True,
                producer_result=ProducerDuplicate(last_seq=producer_seq or 0),
            )
        return AppendVerdict(
            write=False, stream_closed=True, producer_result=ProducerStreamClosed()
        )

    # 2. Content type.
    provided_ct = getattr(opts, "content_type", None)
    if (
        provided_ct
        and facts.content_type
        and normalize_content_type(provided_ct)
        != normalize_content_type(facts.content_type)
    ):
        raise ContentTypeMismatch(
            f"Content-type mismatch: expected {facts.content_type}, got {provided_ct}"
        )

    # 3. Producer fencing — before Stream-Seq, so a retry is absorbed as a
    #    duplicate rather than raising on the seq it legitimately repeats.
    producer_result: ProducerValidationResult | None = None
    if (
        producer_id is not None
        and producer_epoch is not None
        and producer_seq is not None
    ):
        producer_result = validate_producer(
            producer_state, producer_id, producer_epoch, producer_seq, now
        )
        if producer_result.status != "accepted":
            return AppendVerdict(write=False, producer_result=producer_result)

    # 4. Stream-Seq monotonicity. The values are opaque strings and `<=` on
    #    `str` is Python's byte-wise lexicographic comparison, which is exactly
    #    what the protocol asks for — no numeric interpretation anywhere.
    seq = getattr(opts, "seq", None)
    if seq is not None and facts.last_seq is not None and seq <= facts.last_seq:
        raise SequenceConflict(f"Sequence conflict: {seq} <= {facts.last_seq}")

    return AppendVerdict(write=True, producer_result=producer_result)
