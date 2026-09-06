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
`apply()`, after which the question of which event produced which effect is no longer
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
import types
import typing
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

    Keys and values must be strings, and construction refuses anything else — a
    nested map is exactly the shape a whole record of personal data arrives in,
    and not leaking field values is this field's reason for existing instead of
    a rendered message.
    """

    attempt: int = 1
    """Which try this was, from 1. The natural key is
    ``(consumer, stream_path, subject, attempt)`` — subject rather than offset,
    because a refused event has no offset — so history accumulates instead of
    overwriting itself."""

    def __post_init__(self) -> None:
        # Copy the two containers so `frozen=True` means what it says: it freezes
        # the binding, not the dict or list behind it, and a caller keeping its own
        # reference could otherwise change what had already been recorded. Only a
        # container of the declared shape is copied — a bare string is iterable,
        # and `tuple("missing_total")` is thirteen one-letter codes that the walk
        # below would then pass; anything else is left as it came, for the walk to
        # refuse.
        if type(self.params) is dict:
            object.__setattr__(self, "params", dict(self.params))
        if type(self.reasons) in (list, tuple):
            object.__setattr__(self, "reasons", tuple(self.reasons))

        # Every declared field is checked against its declared type, and the
        # declaration is read rather than restated: a field added later is
        # checked the moment it is declared, not when its first defect is found.
        # `type(x) is str` rather than `isinstance`, because a `str` subclass
        # reads back from storage as plain text and only a type check can see
        # the difference; and every string must encode, because a lone surrogate
        # is text by every type check and no path or column can carry it.
        problems = _check_fields(self)
        names = ("consumer", "stream_path", "subject", "offset")
        unwritable = sorted(n for n in problems if n.split("[", 1)[0] in names)
        if unwritable:
            raise ValueError(
                f"A store sorts and names by these, so they have to be writable text: "
                f"{', '.join(unwritable)}."
            )
        if problems:
            detail = "; ".join(f"{name}: {why}" for name, why in problems.items())
            raise ValueError(
                f"An outcome has to be storable as text and this one is not: {detail}. "
                "Render it, or leave it out."
            )
        # An empty name is not merely odd, it collides: a file-backed store maps a
        # name to one path segment, and every escaping of "" lands on the segment
        # some other input already uses. Refusing here is cheaper than making every
        # store's naming injective, and an outcome about nothing is meaningless
        # anyway.
        empty = sorted(
            n for n in ("consumer", "stream_path", "subject") if not getattr(self, n)
        )
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

        # `encode` is still the last word: anything the walk above cannot see
        # and json cannot render is refused there, with the same message.
        encode(self)


def _check_fields(outcome: Outcome) -> dict[str, str]:
    """Every declared field against its declared type; `{field: why}` for each miss.

    Reads the annotations rather than naming the fields, so the rule is exactly the
    declaration: a `Literal` admits only its members, an `int` is not a `bool`, a
    `tuple[str, ...]` and a `dict[str, str]` are checked element by element, and
    every `str` must be encodable. Paths into a container read as
    ``params['k']`` so the message says which value was wrong.
    """
    problems: dict[str, str] = {}
    hints = typing.get_type_hints(Outcome)
    for f in fields(outcome):
        _check_value(f.name, getattr(outcome, f.name), hints[f.name], problems)
    return problems


def _check_value(
    path: str, value: object, hint: object, problems: dict[str, str]
) -> None:
    origin = typing.get_origin(hint)
    args = typing.get_args(hint)
    if origin in (typing.Union, types.UnionType):
        if value is None and type(None) in args:
            return
        others = [a for a in args if a is not type(None)]
        if len(others) == 1:
            _check_value(path, value, others[0], problems)
            return
        raise TypeError(f"Unsupported annotation on {path}: {hint!r}")
    if origin is Literal:
        if type(value) is not str or value not in args:
            problems[path] = (
                f"expected one of {', '.join(map(repr, args))}, got {value!r}"
            )
        return
    if origin is tuple:
        if type(value) is not tuple:
            problems[path] = f"expected a tuple, got {type(value).__name__}"
            return
        for i, item in enumerate(value):
            _check_value(f"{path}[{i}]", item, args[0], problems)
        return
    if origin is dict:
        if type(value) is not dict:
            problems[path] = f"expected a dict, got {type(value).__name__}"
            return
        for k, v in value.items():
            _check_value(f"{path}[{k!r}] (key)", k, args[0], problems)
            _check_value(f"{path}[{k!r}]", v, args[1], problems)
        return
    if hint is str:
        if type(value) is not str:
            problems[path] = f"expected text, got {type(value).__name__}"
            return
        try:
            value.encode("utf-8")
        except UnicodeEncodeError:
            problems[path] = "text that cannot be encoded"
        return
    if hint is int:
        if type(value) is not int:
            problems[path] = f"expected an int, got {type(value).__name__}"
        return
    raise TypeError(f"Unsupported annotation on {path}: {hint!r}")


def encode(outcome: Outcome) -> str:
    """The one translation from an outcome to the text a store keeps.

    Returns **text**, not a dict, and that is the whole design. Every store keeps
    this exact string, so there is only one representation of a stored outcome and
    nothing for two backends to disagree about. The in-memory store is then the
    file-backed store without the file, rather than a second implementation that
    happens to behave the same.

    Refuses by raising, because `json.dumps` already refuses everything it cannot
    represent. Construction has already checked each field against its declared
    type by the time this runs, so this is the backstop for whatever that walk
    does not describe, not the rule itself.
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
        if not isinstance(payload, dict):
            raise ValueError(f"expected an object, got {type(payload).__name__}")
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
