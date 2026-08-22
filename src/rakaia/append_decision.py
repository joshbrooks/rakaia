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

## Batches

`decide_append_batch` is the same question asked of a whole batch, and it is
here for the same reason: both stores had grown their own version and the two
genuinely disagreed (#181). A batch adds two rules to the per-item one, plus the
payload check the section below covers, and each is easy to get subtly wrong in
isolation:

- **All-or-nothing on a refusal.** Every item is decided before any of them is
  written, so a refusal refuses the batch rather than leaving a written prefix.
  The durable store's single transaction can only behave that way; the
  in-memory store has to be told to.
- **The facts advance across the batch**, exactly as they would across a loop of
  `append` — which is what `append_many` promises on both backends. That means
  they advance *only for an item that is actually written*: an item the fence
  refuses takes no `Stream-Seq`, and does not move its producer's sequence. It
  also means an item with `close=True` closes the stream *for the items after
  it*, which then see a closed stream with a `closed_by` — so a producer
  re-sending its own closing append later in the same batch is recognised as a
  duplicate rather than told a bare "closed".

Each of those three was live: the in-memory store advanced `Stream-Seq` on
refused items (raising a conflict on a sequence nothing had taken), and the
durable store short-circuited a closed stream and its own post-close items
before consulting the rule at all (losing the duplicate).

### Payload validity is part of the batch decision

The rules above read the *options*; `check_payload` reads the *body*. It is here
rather than in each store because the all-or-nothing property above is a claim
about the whole batch, and neither store honoured it for a bad body (#214):

- the in-memory store never looked at a payload during the pre-flight, so a
  batch whose second item was not JSON raised `InvalidJson` from the loop with
  the first item already persisted — a written prefix sitting behind a refusal,
  the exact thing all-or-nothing exists to prevent;
- the durable store encoded each item independently, so the same batch did not
  raise at all and stored a non-JSON body in a stream declared
  `application/json`.

So `decide_append_batch` takes the payloads and checks each one **in the loop,
after that item's admission verdict and only for an item that is to be
written** — which is the order a loop of `append` produces, and the reason the
check cannot simply be a pass over the payloads before or after the scan. A
closed stream must answer "closed", not "invalid JSON"; an item the fence
refuses is never written, so its body is never parsed.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from typing import Any

from .json_mode import (
    is_json_content_type,
    normalize_content_type,
    process_json_append,
)
from .producer import validate_producer
from .types import (
    ClosedBy,
    ContentTypeMismatch,
    ProducerAccepted,
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


def check_payload(content_type: str | None, data: bytes) -> None:
    """Raise if `data` is not a body the stream's content type can hold.

    Only a stream declared `application/json` constrains its bodies: the payload
    must parse, and an append of `[]` is the no-op the protocol rejects. Every
    other content type — including none at all, the event-sourcing shape — takes
    the bytes as they come, so there is nothing to refuse.

    The parsed result is deliberately thrown away. Deciding validity is not the
    same job as deciding *how many messages* a body becomes, and the two stores
    still disagree on the second question (#214, unresolved) — so this answers
    only the first, and each store splits the body its own way afterwards.
    """
    if is_json_content_type(content_type):
        process_json_append(data)


@dataclass(frozen=True)
class BatchVerdict:
    """The decision for a whole batch: one verdict per input item, in order.

    ``verdicts`` is always the same length as the items handed in, so a store
    can zip it back against its inputs — an item the batch refuses keeps its
    slot rather than vanishing and shifting every later item's answer onto the
    wrong input.

    ``closing_opts`` is the options object of the item that closed the stream,
    or None if nothing in the batch closed it. A store needs it to record
    *which* producer tuple did the closing.

    ``producer_commits`` is the fencing state the batch establishes: the *last*
    accepted outcome per producer id, which is the only one worth persisting.
    Committing each accepted item's outcome in turn reaches the same final state
    but costs a write per item, and ``append_many`` promises a query count that
    does not grow with the batch — so the rule hands back the one commit per
    producer instead of leaving each store to work that out.
    """

    verdicts: list[AppendVerdict]
    last_seq: str | None = None
    closing_opts: Any = None
    producer_commits: dict[str, ProducerAccepted] = field(default_factory=dict)

    @property
    def writes_anything(self) -> bool:
        """Whether any item is to be written. A store that allocates an offset
        block up front must check this: a block of zero is an error."""
        return any(v.write for v in self.verdicts)


def decide_append_batch(
    facts: StreamFacts,
    items: Sequence[Any],
    *,
    payloads: Sequence[bytes],
    producer_states: Mapping[str, ProducerState | None] | None = None,
    now: float,
) -> BatchVerdict:
    """Decide a whole batch, as a loop of :func:`decide_append` over advancing
    facts. See the "Batches" section of the module docstring for the rules this
    adds and why they are here rather than in each store.

    ``items`` is the per-item options, in order — the same objects
    ``decide_append`` takes, and ``None`` for a raw append with no options.
    ``payloads`` is the bodies for those same items, in the same order and of
    the same length; it is a required argument rather than an optional one so
    that a store cannot obtain a batch verdict without having its bodies
    checked. ``producer_states`` is the pre-batch state for every producer id
    appearing in the batch; a missing key is read as "no state", the same as
    ``None``.

    Nothing is mutated: ``producer_states`` is copied, and the caller's
    ``facts`` is untouched. A conflict propagates out of the loop, which is what
    makes the batch all-or-nothing — the caller has decided nothing by the time
    it sees the exception, so it writes nothing. That is equally true of the
    payload check: `InvalidJson` from item three leaves items one and two
    undecided and therefore unwritten.
    """
    states = dict(producer_states or {})
    closed = facts.closed
    closed_by = facts.closed_by
    last_seq = facts.last_seq
    closing_opts: Any = None
    commits: dict[str, ProducerAccepted] = {}

    verdicts: list[AppendVerdict] = []
    for opts, data in zip(items, payloads, strict=True):
        producer_id = getattr(opts, "producer_id", None)
        verdict = decide_append(
            StreamFacts(
                closed=closed,
                closed_by=closed_by,
                content_type=facts.content_type,
                last_seq=last_seq,
            ),
            opts,
            producer_state=states.get(producer_id) if producer_id is not None else None,
            now=now,
        )
        verdicts.append(verdict)
        if not verdict.write:
            # Refused, so it is not written, so it moves nothing. This is the
            # half the in-memory store's own scan got wrong: it advanced
            # `last_seq` here, and a later item then collided with a sequence
            # no write had ever taken.
            continue

        # Admitted, so it is going to be written — which is the point at which a
        # loop of `append` would parse the body. Raising here refuses the whole
        # batch with nothing written, on either backend.
        check_payload(facts.content_type, data)

        seq = getattr(opts, "seq", None)
        if seq is not None:
            last_seq = seq
        if producer_id is not None and isinstance(
            verdict.producer_result, ProducerAccepted
        ):
            # The state the store will commit after this item lands, so the
            # next item from the same producer is fenced against this one
            # rather than against the pre-batch row. Overwriting the entry in
            # `commits` is what keeps the store's writes one-per-producer: a
            # later accepted item from the same producer supersedes this one,
            # and only the last state is worth persisting.
            states[producer_id] = verdict.producer_result.proposed_state
            commits[producer_id] = verdict.producer_result
        if getattr(opts, "close", False):
            # The rest of the batch observes the stream this item leaves
            # behind, closing tuple included.
            closed = True
            closing_opts = opts
            if producer_id is not None:
                closed_by = ClosedBy(
                    producer_id=producer_id,
                    epoch=getattr(opts, "producer_epoch", None) or 0,
                    seq=getattr(opts, "producer_seq", None) or 0,
                )

    return BatchVerdict(
        verdicts=verdicts,
        last_seq=last_seq,
        closing_opts=closing_opts,
        producer_commits=commits,
    )
