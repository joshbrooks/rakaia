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

import logging
from collections.abc import Mapping
from dataclasses import dataclass, field, fields
from functools import lru_cache
from types import UnionType
from typing import Any, Literal, Union, get_args, get_origin, get_type_hints

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
        # A lone surrogate is a `str` and is not text anything can write: a
        # file-backed store raises out of its path escaping before recording
        # anything — inside the very loop whose job is recording that something
        # failed. Refused for the same reason an empty name is: a store cannot turn
        # it into a name, and the declared type cannot tell it apart.
        unwritable = sorted(n for n in names if not _encodable(getattr(self, n)))
        if unwritable:
            raise ValueError(
                f"A name has to be writable text, and {', '.join(unwritable)} cannot be encoded."
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


@lru_cache(maxsize=1)
def _declared() -> dict[str, Any]:
    """Each field's declared type, resolved once."""
    return get_type_hints(Outcome)


def _encodable(value: Any) -> bool:
    """Whether this text can actually be written out.

    A lone surrogate satisfies every type check and still cannot be encoded, so it
    passes construction and fails inside a store instead.
    """
    if type(value) is not str:
        return True  # a wrong type is a different complaint, reported elsewhere
    try:
        value.encode("utf-8")
    except UnicodeEncodeError:
        return False
    return True


def _matches(value: Any, hint: Any) -> bool:
    """Whether `value` is what `hint` says it is, as JSON would keep it.

    `type(...) is` rather than `isinstance`, deliberately. A `str` subclass — a
    `StrEnum` member is the one that turns up — passes `isinstance`, compares equal
    to its own text, and comes back from storage as plain text with a different type
    and repr. `bool` under `int` is the same trap the other way. Equality cannot see
    either, which is why this asks about the type.

    Reading the declaration rather than a list of fields is the whole point. Five
    review rounds each added the field it had just been bitten by, and the list was
    never more complete than the last bug; a field added tomorrow is covered by this
    without anyone remembering it exists.
    """
    origin = get_origin(hint)
    if origin is Literal:
        # `in` is equality, and equality is exactly what a `str` subclass passes —
        # so the two fields declared as a small set of strings were the only ones
        # this function did not protect, which is the case its own docstring names.
        # A member has to be the same type as the alternative it matches.
        return any(value == a and type(value) is type(a) for a in get_args(hint))
    if origin in (UnionType, Union):
        return any(_matches(value, arg) for arg in get_args(hint))
    if hint is type(None):
        return value is None
    if origin in (tuple, list):
        member = get_args(hint)[0]
        return type(value) is list and all(_matches(v, member) for v in value)
    if origin is dict:
        key, val = get_args(hint)
        return type(value) is dict and all(
            _matches(k, key) and _matches(v, val) for k, v in value.items()
        )
    return type(value) is hint


def encode(outcome: Outcome) -> dict[str, Any]:
    """The one translation from an outcome to the shape a store keeps.

    Every backend goes through this, **including the in-memory one**. That is the
    point of it rather than an implementation detail: the in-memory store used to
    keep the object as handed to it while the durable ones had to render it, so it
    accepted values they refused and the two disagreed about what had been
    recorded. A reference implementation more permissive than the real ones makes a
    passing test a weaker promise than production, which is the defect this module
    spent five review rounds on, one field at a time.

    The check walks the fields rather than naming them. Naming them is how it took
    five rounds: each round added the field it had just been bitten by, and the list
    was only ever as complete as the last bug. A field added to `Outcome` tomorrow
    is covered by this without anyone remembering to add it.

    Refuses rather than renders. Rendering here would make a store disagree with its
    caller instead of with another store — quieter, and worse.
    """
    payload: dict[str, Any] = {}
    for f in fields(outcome):
        value = getattr(outcome, f.name)
        if type(value) is tuple:
            value = list(value)
        elif type(value) is dict:
            # Sorted so the two stores cannot disagree about order: one of them
            # writes JSON with sorted keys and the other kept insertion order, and
            # `==` on dicts hides the difference while anything comparing their
            # rendered form does not.
            value = dict(sorted(value.items()))
        payload[f.name] = value

    declared = _declared()
    unstorable = sorted(
        f"{name}={value!r}"
        for name, value in payload.items()
        if not _matches(value, declared[name])
    )
    if unstorable:
        raise ValueError(
            "An outcome is kept as text, so every part of it has to be text already: "
            + ", ".join(unstorable)
            + ". Render it, or leave it out."
        )
    return payload


def decode(payload: Mapping[str, Any]) -> Outcome | None:
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
        self._recorded: list[dict[str, Any]] = []

    def record(self, outcome: Outcome) -> None:
        # Through the codec, not straight into the list. Keeping the object was
        # what made this store more permissive than every real one, so "passes in
        # memory" stopped meaning "will store".
        self._recorded.append(encode(outcome))

    def latest(self, consumer: str, stream_path: str) -> list[Outcome]:
        best: dict[str, Outcome] = {}
        for payload in self._recorded:
            outcome = decode(payload)
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
