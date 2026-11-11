"""Whether an append is allowed, and what it writes, decided once for every store.

`rakaia.producer` already does this for the *fencing* rules, and the two stores
share it, so they cannot drift on epoch/seq. The rest of the admission sequence —
closed? content-type? fence? Stream-Seq? — was restated in both adapters, and
they had already drifted: `StreamStore.append` validated producer options inline
and recognised the idempotent close-duplicate, while `DjangoStreamStore.append`
ignored `options.producer_id` entirely, all while its docstring claimed
"outcomes, all now matching the in-memory store".

Nothing caught that because every producer test routed through a separate
fenced-append method. `WritableStore.append` is public and its `AppendOptions`
carries those fields, so a consumer calling it directly got adapter-dependent
behaviour.

This module owns the sequence. A store's job becomes: gather the facts, ask, then
persist — so a new backend implements *storage*, not protocol semantics.

It owns what an admitted append *becomes* as well (#321): the stored payloads,
the envelope each carries, and what the append changes about the stream. Those
were worked out separately in each store, and every rule added to events since
the envelope landed had to be added three times; the copies had started to
differ on when a bad tag was refused and on whether a plain append got an event
time. `plan_appends` answers all of it, and a store renders offsets, encodes for
its own storage and saves.

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

`plan_appends` asks the same question of a whole batch, and it is
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

The rules above read the *options*; `split_payload` reads the *body*. It is here
rather than in each store because the all-or-nothing property above is a claim
about the whole batch, and neither store honoured it for a bad body (#214):

- the in-memory store never looked at a payload during the pre-flight, so a
  batch whose second item was not JSON raised `InvalidJson` from the loop with
  the first item already persisted — a written prefix sitting behind a refusal,
  the exact thing all-or-nothing exists to prevent;
- the durable store encoded each item independently, so the same batch did not
  raise at all and stored a non-JSON body in a stream declared
  `application/json`.

So `plan_appends` takes the payloads and checks each one **in the loop,
after that item's admission verdict and only for an item that is to be
written** — which is the order a loop of `append` produces, and the reason the
check cannot simply be a pass over the payloads before or after the scan. A
closed stream must answer "closed", not "invalid JSON"; an item the fence
refuses is never written, so its body is never parsed.

The envelope's own checks — tags and correlation id — follow the same rule, at
the same point: an item that will not be written is not checked.

## Event time

An append that carries no `event_ts` is stamped by the store, as
`max(the store's last stamp on this stream, now)`, so a stream's times never
contradict its positions when the clock steps back (#284). The plan cannot take
that stamp itself: the database store must read its last stamp, and the clock,
with its offset lock held, and it takes that lock only once it knows something
will be written — a refused append costs one read. So `plan_appends` leaves
those times unset and `AppendPlan.stamped` fills them in, once the store has
what it needs.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

from .context import merge_provenance, tags_and_correlation
from .json_mode import (
    is_json_content_type,
    normalize_content_type,
    process_json_append,
)
from .producer import validate_producer
from .types import (
    AppendOptions,
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
    """What the admission rules need to know about a stream, whatever the backend.

    ``closed_by`` is the producer tuple that closed the stream, if one did, and
    ``last_seq`` is the highest `Stream-Seq` it has accepted. These four fields
    are all the rules read.
    """

    closed: bool = False
    closed_by: ClosedBy | None = None
    content_type: str | None = None
    last_seq: str | None = None


@dataclass(frozen=True)
class AppendVerdict:
    """What the rules decided for one append.

    ``write`` says whether the item is to be written. When it is not,
    ``producer_result`` is the refusal to report (a `ProducerDuplicate`,
    `ProducerStaleEpoch`, `ProducerStreamClosed` and so on) and ``stream_closed``
    says the stream is closed.

    When it is, ``producer_result`` is a `ProducerAccepted` for an append with a
    producer, whose ``proposed_state`` is what to save once the write lands, or
    None for an append with none.
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


def split_payload(
    content_type: str | None, data: bytes, *, is_initial_create: bool = False
) -> list[bytes]:
    """The stored payloads one body becomes, raising if the stream cannot hold it.

    Only a stream declared `application/json` constrains its bodies: the payload
    must parse, an append of `[]` is the no-op the protocol rejects, and a
    top-level array is flattened one level into one payload per element (spec
    7.1). Every other content type — including none at all, the event-sourcing
    shape — takes the bytes as they come, as one payload.

    `is_initial_create` admits the empty array, which on `PUT` is "an empty
    stream" and comes back as no payloads at all.
    """
    if is_json_content_type(content_type):
        return process_json_append(data, is_initial_create)
    return [data]


def own_stamp(last_event_ts: float | None, now: float) -> float:
    """The event time a store stamps: `now`, but never before its last stamp."""
    return now if last_event_ts is None else max(last_event_ts, now)


@dataclass(frozen=True)
class PlannedWrite:
    """One admitted item, as it is to be stored.

    ``payloads`` has one entry per stored message: more than one only when a JSON
    array was split. ``metadata`` has the ambient `provenance()` merged under the
    item's own, ``tags`` and ``correlation_id`` are already checked, and
    ``event_ts`` is the producer's time, or None until `AppendPlan.stamped` fills
    in the store's own. ``close`` is true if this item closes the stream.
    """

    payloads: tuple[bytes, ...]
    label: str = ""
    metadata: dict[str, Any] | None = None
    tags: tuple[str, ...] = ()
    correlation_id: str | None = None
    event_ts: float | None = None
    close: bool = False


@dataclass(frozen=True)
class AppendPlan:
    """The decision for a batch of appends (one append is a batch of one) and what
    to store for it.

    ``verdicts`` and ``writes`` have one entry per input item, in order; an item
    that is not written keeps its slot, with a verdict and ``None``. Once the
    writes land, save ``last_seq`` as the stream's `Stream-Seq`, close the stream
    with ``closed_by`` if ``close`` is set, and save the ``proposed_state`` of each
    `ProducerAccepted` in ``producer_commits`` (one per producer: its last accepted
    item).

    ``writes_anything`` says whether to allocate offsets at all, ``message_count``
    how many a block needs, and ``stamped`` fills in the event times.
    """

    verdicts: list[AppendVerdict]
    writes: list[PlannedWrite | None]
    last_seq: str | None = None
    close: bool = False
    closed_by: ClosedBy | None = None
    producer_commits: dict[str, ProducerAccepted] = field(default_factory=dict)

    @property
    def writes_anything(self) -> bool:
        """Whether any item is to be written. A store that allocates an offset
        block up front must check this: a block of zero is an error."""
        return any(w is not None for w in self.writes)

    @property
    def message_count(self) -> int:
        """How many messages the writes store in all, flattened arrays counted
        element by element — the size of the offset block they need."""
        return sum(len(w.payloads) for w in self.writes if w is not None)

    def stamped(
        self, last_event_ts: float | None, now: float
    ) -> tuple[list[PlannedWrite | None], float | None]:
        """The writes with the store's own event time filled in, and the store's
        new last stamp (None if no write needed one).

        One stamp serves every write in the plan that has no time of its own: they
        are written together, so `own_stamp` would give each the same answer. A
        producer's time is kept exactly as sent and does not move the mark, so a
        backfill dated far in the future cannot pin every later stamp to it.
        """
        if not any(w is not None and w.event_ts is None for w in self.writes):
            return list(self.writes), None
        stamp = own_stamp(last_event_ts, now)
        return [
            replace(w, event_ts=stamp) if w is not None and w.event_ts is None else w
            for w in self.writes
        ], stamp


def plan_appends(
    facts: StreamFacts,
    items: Sequence[AppendOptions | None],
    *,
    payloads: Sequence[bytes],
    producer_states: Mapping[str, ProducerState | None] | None = None,
    now: float,
) -> AppendPlan:
    """Decide a batch of appends and work out what each admitted item stores.

    ``items`` is the `AppendOptions` for each append, in order (``None`` for a
    raw append with no options), and ``payloads`` the bodies for the same items;
    lists of different lengths raise `ValueError`. ``producer_states`` is the
    saved state for each producer id in the batch (leave out an id that has none;
    state old enough to have expired counts as absent), and ``now`` is the
    store's clock.

    A content-type mismatch, a `Stream-Seq` that does not advance, a body the
    stream cannot hold and a bad tag all raise before anything is stored, so the
    batch writes nothing. A closed stream and a producer the fence refuses do not
    raise: that item's verdict has ``write=False`` and its slot in ``writes`` is
    ``None``, while the other items are still decided and written. Build one
    `AppendResult` per item from ``verdict.producer_result`` and
    ``stream_closed`` (``write.close`` for a written item, ``verdict.stream_closed``
    otherwise). Facts advance across the batch only for items that are written, as
    if `append` were called once per item. Nothing you pass in is changed.
    """
    states = dict(producer_states or {})
    closed = facts.closed
    closed_by = facts.closed_by
    last_seq = facts.last_seq
    close = False
    commits: dict[str, ProducerAccepted] = {}

    verdicts: list[AppendVerdict] = []
    writes: list[PlannedWrite | None] = []
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
            # Refused, so it is not written, so it moves nothing and its body
            # and envelope are never read. The in-memory store's own scan once
            # advanced `last_seq` here, and a later item then collided with a
            # sequence no write had ever taken (#181).
            writes.append(None)
            continue

        # Admitted, so it is going to be written — the point at which a loop of
        # `append` would parse the body and check the envelope. Raising here
        # refuses the whole batch with nothing written, on every backend.
        stored = split_payload(facts.content_type, data)
        tags, correlation_id = tags_and_correlation(opts)
        item_closes = bool(getattr(opts, "close", False))
        writes.append(
            PlannedWrite(
                payloads=tuple(stored),
                label=getattr(opts, "label", "") or "",
                metadata=merge_provenance(getattr(opts, "metadata", None)),
                tags=tags,
                correlation_id=correlation_id,
                event_ts=getattr(opts, "event_ts", None),
                close=item_closes,
            )
        )

        seq = getattr(opts, "seq", None)
        if seq is not None:
            last_seq = seq
        if producer_id is not None and isinstance(
            verdict.producer_result, ProducerAccepted
        ):
            # The state the store will commit after this item lands, so the
            # next item from the same producer is fenced against this one
            # rather than against the pre-batch row. Overwriting the entry in
            # `commits` is what keeps the store's writes one-per-producer.
            states[producer_id] = verdict.producer_result.proposed_state
            commits[producer_id] = verdict.producer_result
        if item_closes:
            # The rest of the batch observes the stream this item leaves
            # behind, closing tuple included.
            closed = True
            close = True
            if producer_id is not None:
                closed_by = ClosedBy(
                    producer_id=producer_id,
                    epoch=getattr(opts, "producer_epoch", None) or 0,
                    seq=getattr(opts, "producer_seq", None) or 0,
                )

    return AppendPlan(
        verdicts=verdicts,
        writes=writes,
        last_seq=last_seq,
        close=close,
        closed_by=closed_by if close else None,
        producer_commits=commits,
    )


def plan_initial_write(content_type: str | None, data: bytes) -> AppendPlan:
    """What the body of a create writes, as a plan of one.

    There is no stream yet to fence or sequence against and no envelope is
    recorded, so only the body split and the store's own event time apply. An
    empty JSON array is an empty stream and plans no write.
    """
    payloads = split_payload(content_type, data, is_initial_create=True)
    write = PlannedWrite(payloads=tuple(payloads)) if payloads else None
    return AppendPlan(verdicts=[AppendVerdict(write=write is not None)], writes=[write])
