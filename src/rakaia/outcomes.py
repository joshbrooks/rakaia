"""What happened to an event a consumer tried to apply — the record a cursor cannot carry.

A cursor says how far a consumer got. It says nothing about whether it got there
cleanly, so an event that was skipped, refused or lost is indistinguishable from
one applied without incident: **absence of a record reads as success**. This
module is the record that closes that gap, and `subscription.consume` is where it
is written.

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
    """Bounded context for the reasons: identifiers, counts, verdicts. Values are
    strings by construction, so a payload cannot be put here by accident."""

    attempt: int = 1
    """Which try this was, from 1. The natural key is
    ``(consumer, stream_path, offset, attempt)``, so history accumulates instead
    of overwriting itself."""

    def __post_init__(self) -> None:
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
        best: dict[str | None, Outcome] = {}
        for outcome in self._recorded:
            if outcome.consumer != consumer or outcome.stream_path != stream_path:
                continue
            held = best.get(outcome.offset)
            if held is None or outcome.attempt > held.attempt:
                best[outcome.offset] = outcome
        # Offsets sort as the opaque strings they are; the append-stage entries
        # have none and go last rather than being compared against one.
        return sorted(
            best.values(),
            key=lambda o: (o.offset is None, o.offset or ""),
        )
