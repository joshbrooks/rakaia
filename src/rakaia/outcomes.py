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

import json
import logging
from dataclasses import dataclass, field, fields
from typing import Literal

logger = logging.getLogger(__name__)

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
        # Copy the two containers so `frozen=True` means what it says: it freezes
        # the binding, not the dict or list behind it, and a caller keeping its own
        # reference could otherwise change what had already been recorded.
        object.__setattr__(self, "params", dict(self.params))
        object.__setattr__(self, "reasons", tuple(self.reasons))

        # Structural invariants — about what an outcome *means*, not how it is kept.
        # An empty name is not merely odd, it collides: a file-backed store maps a
        # name to one path segment, and every escaping of "" lands on the segment
        # some other input already uses. Refusing here is cheaper than making every
        # store's naming injective, and an outcome about nothing is meaningless
        # anyway.
        names = ("consumer", "stream_path", "subject")
        # These three become path segments and dictionary keys, so they carry a
        # constraint the payload does not: they must be writable text. A number is
        # stored happily by anything keeping JSON and raises in a file store's path
        # escaping; a lone surrogate is text by every type check and no path can
        # carry it. Both reach the store and fail there — inside the loop whose job
        # is recording that something failed. One rule, because it is one reason.
        unwritable = []
        for name in names:
            value = getattr(self, name)
            if type(value) is not str:
                unwritable.append(name)
                continue
            try:
                value.encode("utf-8")
            except UnicodeEncodeError:
                unwritable.append(name)
        if unwritable:
            raise ValueError(
                f"A name has to be writable text, and {', '.join(sorted(unwritable))} cannot be encoded."
            )
        empty = sorted(n for n in names if not getattr(self, n))
        if empty:
            raise ValueError(
                f"A name saying what this outcome belongs to cannot be empty: {', '.join(empty)}."
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

        # Storability is one rule and `encode` is where it lives, so this asks the
        # same question a store will ask rather than keeping a second copy of the
        # answer. Five rounds of review closed this one field at a time — values of
        # one map, then its keys, then the list beside it, then the plain fields —
        # because each check was written where the defect was found instead of where
        # the rule belongs. Calling it here only moves *when* the caller hears about
        # it; the rule itself has one home.
        encode(self)


def encode(outcome: Outcome) -> str:
    """The one translation from an outcome to the text a store keeps.

    Returns **text**, not a dict, and that is the whole design. Every store keeps
    this exact string, so there is only one representation of a stored outcome and
    nothing for two backends to disagree about. The in-memory store is then the
    file-backed store without the file, rather than a second implementation that
    happens to behave the same.

    That matters because the previous approach did not work. It validated each
    field against its declared type, and five review rounds plus two more found
    values that satisfy a type and still do not survive storage: a `str` subclass
    that flattens, a lone surrogate a path cannot carry, a string longer than a
    column allows. The set is open-ended, so approximating "survives storage" with
    type rules leaks by construction — the check has to *be* the storing.

    Refuses by raising, because `json.dumps` already refuses everything it cannot
    represent. Nothing here re-states which types those are.
    """
    try:
        return json.dumps(
            {f.name: getattr(outcome, f.name) for f in fields(outcome)}, sort_keys=True
        )
    except (TypeError, ValueError) as exc:
        raise ValueError(
            f"An outcome has to be storable as text and this one is not: {exc}. Render it, or leave it out."
        ) from exc


def decode(stored: str) -> Outcome | None:
    """The inverse of `encode`, or ``None`` if this version cannot build it.

    ``None`` rather than an exception because the caller is usually reading a whole
    file: a line written by a version that added a field, or predating one, must
    cost that line and not the report. Unknown keys are dropped and a missing
    required one gives ``None``.

    Dropping is logged, because a silently discarded line is the failure this whole
    module exists to close — a file entirely of skew would otherwise read back as
    "nothing failed". See ADR 0007 for why this is a log and not yet a count.
    """
    known = {f.name for f in fields(Outcome)}
    try:
        payload = json.loads(stored)
        return Outcome(**{k: v for k, v in payload.items() if k in known})
    except (TypeError, ValueError) as exc:
        logger.warning(
            "rakaia: dropping an outcome this version cannot build (%s)", exc
        )
        return None


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
        self._recorded: list[str] = []

    def record(self, outcome: Outcome) -> None:
        # The encoded *text*, not the object and not a dict. This store is then the
        # file-backed one without the file, so the two cannot disagree about what
        # was stored — which is the disagreement seven review findings were about.
        self._recorded.append(encode(outcome))

    def latest(self, consumer: str, stream_path: str) -> list[Outcome]:
        best: dict[str, Outcome] = {}
        for stored in self._recorded:
            outcome = decode(stored)
            if (
                outcome is None
                or outcome.consumer != consumer
                or outcome.stream_path != stream_path
            ):
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
