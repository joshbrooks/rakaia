"""What happened to an event a consumer tried to apply — the record a cursor cannot carry.

A cursor says how far a consumer got. It says nothing about whether it got there
cleanly, so an event that was skipped, refused or lost is indistinguishable from
one applied without incident: **absence of a record reads as success**. This
module is the record that closes that gap. The loop that writes it is not part of
this change; ADR 0007 Decision 2 describes where it goes.

Two things follow from where it is written, and both are load-bearing.

The record is **not** produced inside an executor. A Django executor wraps its
batch in a transaction, so a row written there would roll back with the batch
whose failure it exists to record; and a staged pass buffers many events into one
`apply()`, after which which event produced which effect is no longer
recoverable. The loop that owns the cursor is the only place that still knows
both the event and whether applying it worked.

And only **exceptions** are recorded. The cursor is the success record —
everything below it succeeded unless an outcome says otherwise — so a clean run
writes nothing at all. There is no gap query, and ADR 0007 Decision 3 explains
why one cannot exist here: with success leaving no trace, "no record" *is* the
success case, so lag comes from the head against the cursor and everything else
comes from `latest`.

The `OutcomeStore` seam itself lives in `rakaia.protocols`, with the other
store-facing protocols.

See ADR 0007 for the reasoning in full, including why an outcome is neither an
observation about the data nor a verification result.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

Stage = Literal["append", "project"]
"""How far the event got before something went wrong.

* ``append`` — it never reached the log. ``offset`` is ``None``, and ``status``
  says whether it was lost or declined.
* ``project`` — it is safe in the log and was not applied. Replay recovers it.
"""

OutcomeStatus = Literal["failed", "refused", "skipped"]
"""What happened, and therefore what recovery is wanted.

* ``failed`` — something raised. A bug or a transient fault; worth an alert.
* ``refused`` — a consumer's own rules declined it. Ordinary data quality, not an
  incident, and *deliberate*: with ``stage="append"`` the fact is not lost, it is
  sitting wherever the event was produced from, and fixing the data upstream is
  what recovers it.
* ``skipped`` — deliberately not applied and nothing is wanted. Recorded so that
  a later reader can tell "we decided not to" from "we never saw it".
"""


@dataclass(frozen=True)
class Outcome:
    """One event's failure to be applied cleanly, as a value.

    Immutable, and deliberately so. Nothing here is resolved or retried in place:
    a further attempt is a further outcome with a higher ``attempt``, and "is this
    still failing?" is the latest outcome for the key. A mutable ``resolved_at``
    would be a second answer to a question a consumer's own records already
    answer — its rules layer typically owns the resolution of the very
    observations ``reasons`` names — and two answers eventually disagree.
    """

    consumer: str
    """Which consumer this is about. Pairs with ``ConsumerCursor.consumer_id``."""

    stream_path: str
    """The stream the event belongs to. A plain string, not a reference, so an
    outcome outlives the stream it describes."""

    subject: str
    """What this outcome is *about*, as the consumer identifies it. Opaque here.

    Required, and not defaulted to the offset, because an event that never reached
    the log has no offset — and those are the ones a consumer most needs to tell
    apart. Keying on the offset alone collapsed every refusal on a stream into a
    single row and reported the first as though it spoke for the rest, which is
    the same defect as one bad row making a whole form look fine.

    Distinct from `sequence_key`: the subject is the one thing this outcome
    concerns, the sequence key is what it is *ordered within*. They are often the
    same value, and are not when one decision refuses several subjects together.
    """

    offset: str | None
    """The event's position, opaque and store-issued — or ``None`` when
    ``stage="append"``, because an event that never reached the log has no
    position to name."""

    sequence_key: str
    """What this event is ordered *within*, as the consumer defines it.

    Rakaia records it and does not yet act on it. Its purpose is a later one: a
    failure parks a sequence, and events behind it must wait rather than apply to
    a state that never saw the one in front. Recording it now costs a field;
    deriving it afterwards means re-deciding groupings a consumer has already
    decided. See ADR 0007, Decision 7.
    """

    stage: Stage
    status: OutcomeStatus

    reasons: tuple[str, ...] = ()
    """Why, as codes rather than prose. **Opaque to rakaia** — which codes exist
    and what they mean belong to the consumer, exactly as an offset's meaning
    belongs to the store.

    Plural because one event can breach several rules at once, and flattening
    them into a sentence loses the only property that makes them countable. An
    interpolated message is also where field values leak, which matters for the
    consumers this library was built for.
    """

    params: dict[str, str] = field(default_factory=dict)
    """Bounded context for the reasons: identifiers, counts, verdicts.

    Values must be strings, and that is checked rather than asserted — the
    docstring used to claim "by construction" while a nested dict of personal data
    went in and came back out unchanged. Not leaking field values is this field's
    whole reason for existing instead of a rendered message, so it refuses.
    """

    attempt: int = 1
    """Which try this was, from 1. The natural key is
    ``(consumer, stream_path, subject, attempt)`` — subject rather than offset,
    because a refused event has no offset — so history accumulates instead of
    overwriting itself."""

    def __post_init__(self) -> None:
        # `frozen=True` freezes the *binding*, not the dict behind it: a caller that
        # keeps its own reference can add a key after recording and the stored
        # outcome changes with it. Copying here is what makes immutability real, and
        # it matters most for this field — the one whose entire purpose is that
        # field values do not reach it. Backends that serialise (JSONL) escaped this
        # by accident; the in-memory one did not, so the two disagreed.
        object.__setattr__(self, "params", dict(self.params))
        # `reasons` is the same shape of problem and was missed twice: it is
        # declared a tuple but nothing made it one, so a caller passing a list kept
        # a live handle on it. Coercing here also settles the type — a list and a
        # tuple of the same codes are the same outcome, and only one of them
        # survives being written out.
        object.__setattr__(self, "reasons", tuple(self.reasons))
        if not self.subject:
            raise ValueError(
                "An outcome needs a subject — what it is about — and got an empty one."
            )
        # Keys as well as values. Checking only the values left the same divergence
        # one round later: an integer key survives in memory and comes back a string
        # from anything that serialises, and a key that is hashable but not a
        # primitive records in memory and raises on write. Both halves have to be
        # strings for the two to agree.
        bad_reasons = sorted(repr(r) for r in self.reasons if not isinstance(r, str))
        if bad_reasons:
            raise ValueError(
                f"reasons must be codes, given as strings; {bad_reasons} are not."
            )
        bad_keys = sorted(repr(k) for k in self.params if not isinstance(k, str))
        if bad_keys:
            raise ValueError(f"params keys must be strings; {bad_keys} are not.")
        bad = sorted(k for k, v in self.params.items() if not isinstance(v, str))
        if bad:
            raise ValueError(
                f"params values must be strings so a payload cannot be put here by accident; "
                f"{bad} are not. Render them, or leave them out."
            )
        if self.stage == "append" and self.offset is not None:
            raise ValueError(
                f"An outcome at stage='append' has no offset — the event never reached the log — "
                f"but got offset={self.offset!r}."
            )
        if self.stage == "project" and self.offset is None:
            raise ValueError(
                "An outcome at stage='project' names an event in the log, so it needs an offset."
            )
        if self.attempt < 1:
            raise ValueError(f"attempt counts from 1, got {self.attempt}.")


def _order(outcome: Outcome) -> tuple[bool, str, str]:
    """Sort key shared by every backend, so `latest` returns one order."""
    return (outcome.offset is None, outcome.offset or "", outcome.subject)


class InMemoryOutcomeStore:
    """An `OutcomeStore` held in a list. The reference implementation.

    Every backend is measured against this one through
    `tests/outcome_store_contract.py`, and it is what a test uses when the point
    under test is a consumer's behaviour rather than its storage.

    Not durable, deliberately: an outcome that has to survive a restart needs a
    backend that survives one, and pretending otherwise here would make the
    contract easier to pass than to satisfy.
    """

    def __init__(self) -> None:
        self._recorded: list[Outcome] = []

    def record(self, outcome: Outcome) -> None:
        self._recorded.append(outcome)

    def latest(self, consumer: str, stream_path: str) -> list[Outcome]:
        best: dict[str, Outcome] = {}
        for outcome in self._recorded:
            if outcome.consumer != consumer or outcome.stream_path != stream_path:
                continue
            held = best.get(outcome.subject)
            # `>=` so re-recording an attempt replaces it. A tie is a caller
            # error either way, and last-write-wins is the less surprising of the
            # two answers.
            if held is None or outcome.attempt >= held.attempt:
                best[outcome.subject] = outcome
        # Offsets sort as the opaque strings they are; the append-stage entries
        # have none and go last rather than being compared against one. Subject
        # breaks the remaining ties so the order does not depend on insertion.
        return sorted(best.values(), key=_order)
