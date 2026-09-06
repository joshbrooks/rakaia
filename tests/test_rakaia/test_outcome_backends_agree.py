"""Every backend stores the same outcome the same way.

The shared contract asks each backend, separately, whether it matches an
expectation. That is not the question five review rounds kept answering wrong.
Every one of those defects was two backends *disagreeing with each other* — a
value the in-memory reference kept as handed to it and a durable store rendered,
refused or altered — and no test compared them, so the class was invisible while
each member of it was found one round at a time.

This asks the question directly. A new backend joins by adding one line to
`backends`, and inherits every case below rather than re-deriving them.
"""

from __future__ import annotations

import uuid
from collections.abc import Hashable
from decimal import Decimal
from enum import Enum
from pathlib import Path

import pytest

from rakaia.jsonl_outcomes import JsonlOutcomeStore
from rakaia.outcomes import InMemoryOutcomeStore, Outcome


#: Values that are *not* text. Each was a real finding, or the next one predicted:
#: rounds fixed a map's values, then its keys, then the list beside it, then the
#: plain fields. A store must refuse all of them, and refuse them identically.
class _StrSubclass(str, Enum):
    """A `str` subclass, which is the shape that defeats every equality check.

    It compares equal to its own text and reads back from storage as plain text
    with a different type — so `==` cannot see the difference and only a type check
    can. A `StrEnum` is the one that turns up in real consumers.
    """

    APPEND = "append"


NOT_TEXT = [
    uuid.uuid4(),
    42,
    None,
    ("a", "b"),
    Decimal("1.5"),
    b"bytes",
    {"k": "v"},
    _StrSubclass.APPEND,
    "\ud800",  # a lone surrogate: a str that cannot be encoded
]


def backends(tmp_path: Path):
    """One of every backend, so a disagreement between any two is visible."""
    return {
        "memory": InMemoryOutcomeStore(),
        "jsonl": JsonlOutcomeStore(tmp_path / "outcomes", fsync=False),
    }


def an_outcome(**kw) -> Outcome:
    base = {
        "consumer": "c",
        "stream_path": "submission/tf611",
        "subject": "row-1",
        "offset": None,
        "sequence_key": "seq",
        "stage": "append",
        "status": "refused",
    }
    return Outcome(**{**base, **kw})


def test_a_recorded_outcome_reads_back_identically_everywhere(tmp_path: Path):
    outcome = an_outcome(reasons=("missing_total", "wrong_period"), params={"row": "3"})
    got = {}
    for name, store in backends(tmp_path).items():
        store.record(outcome)
        got[name] = store.latest("c", "submission/tf611")
    assert len(set(map(repr, got.values()))) == 1, got
    assert got["memory"] == [outcome]


def test_no_backend_keeps_a_handle_on_what_the_caller_passed(tmp_path: Path):
    """Mutating the source after recording must change nothing, anywhere.

    This is the half the type checks cannot reach: the values are all text and
    every store accepts them. What differed was whether the store kept the
    caller's container or rendered it, and only a store that renders was safe by
    accident. Asked across backends because "they agree" is the property; asked
    after `record` because that is when the caller still has the handle.
    """
    reasons = ["missing_total"]
    params = {"row": "3"}
    outcome = an_outcome(reasons=reasons, params=params)

    got = {}
    for name, store in backends(tmp_path).items():
        store.record(outcome)
        got[name] = store.latest("c", "submission/tf611")

    reasons.append("LEAKED")
    params["national_id"] = "1234-LEAK"

    assert len(set(map(repr, got.values()))) == 1, got
    assert got["memory"][0].reasons == ("missing_total",)
    assert got["memory"][0].params == {"row": "3"}


def test_the_backends_agree_on_ordering(tmp_path: Path):
    outcomes = [
        an_outcome(subject="row-3"),
        an_outcome(subject="row-1"),
        an_outcome(
            subject="row-2", offset="0000000001", stage="project", status="failed"
        ),
    ]
    got = {}
    for name, store in backends(tmp_path).items():
        for o in outcomes:
            store.record(o)
        got[name] = [o.subject for o in store.latest("c", "submission/tf611")]
    assert len(set(map(tuple, got.values()))) == 1, got


@pytest.mark.parametrize("value", NOT_TEXT, ids=lambda v: type(v).__name__)
@pytest.mark.parametrize(
    # `stage` and `status` are in the list deliberately. They are declared as a
    # small set of strings, and a `str` subclass compares equal to a member of that
    # set while reading back from storage as something else — the case a check on
    # value alone cannot see.
    "field",
    ["subject", "sequence_key", "consumer", "stream_path", "stage", "status"],
)
def test_no_backend_accepts_what_another_would_refuse(
    tmp_path: Path, field: str, value
):
    """The class itself, as one assertion.

    Each of these used to record in memory and raise on write, so the two stores
    disagreed about whether the outcome existed at all. Refusing at construction is
    what makes that impossible — but the property under test is *agreement*, not
    where the refusal happens, so this stays true if the refusal ever moves.
    """
    outcomes = {}
    for name, store in backends(tmp_path).items():
        try:
            store.record(an_outcome(**{field: value}))
            outcomes[name] = store.latest("c", "submission/tf611")
        except (ValueError, TypeError) as exc:
            outcomes[name] = type(exc).__name__

    assert len(set(map(repr, outcomes.values()))) == 1, (
        f"backends disagree about {field}={value!r}: {outcomes}"
    )


@pytest.mark.parametrize("value", NOT_TEXT, ids=lambda v: type(v).__name__)
def test_the_backends_agree_about_a_bad_reason_or_param(tmp_path: Path, value):
    cases = [{"reasons": (value,)}, {"params": {"k": value}}]
    # A key case only where the value can be one at all — an unhashable value fails
    # in this test's own literal, before a store sees it, which is Python and not a
    # disagreement between backends.
    if isinstance(value, Hashable):
        cases.append({"params": {value: "v"}})
    for kw in cases:
        outcomes = {}
        for name, store in backends(tmp_path).items():
            try:
                store.record(an_outcome(**kw))
                outcomes[name] = store.latest("c", "submission/tf611")
            except (ValueError, TypeError) as exc:
                outcomes[name] = type(exc).__name__
        assert len(set(map(repr, outcomes.values()))) == 1, (
            f"disagree on {kw}: {outcomes}"
        )


def test_every_store_keeps_the_encoded_form_not_the_object():
    """The design's central claim, pinned.

    A review found that reverting the in-memory store to keeping the object left
    the entire suite green: construction validates, so nothing downstream noticed
    which shape the store held. That made the change this commit exists for
    decorative. What the shape actually buys is below — ordering, and any future
    store inheriting one definition instead of writing its own — but a property
    nothing asserts is a property that will be undone.
    """
    store = InMemoryOutcomeStore()
    store.record(an_outcome())
    [held] = store._recorded
    assert isinstance(held, str), (
        f"the store kept a {type(held).__name__}, not the encoded text"
    )
    assert '"subject": "row-1"' in held


def test_the_stores_agree_on_the_order_of_params(tmp_path: Path):
    """The discriminator the codec is load-bearing for.

    One store writes JSON with sorted keys and the other kept whatever order the
    caller used, so the same outcome read back differently from each. `dict.__eq__`
    hides it — this compares the rendered form, which is what anything printing,
    logging or diffing an outcome will see.
    """
    outcome = an_outcome(params={"z": "1", "a": "2", "m": "3"})
    got = {}
    for name, store in backends(tmp_path).items():
        store.record(outcome)
        got[name] = [list(o.params) for o in store.latest("c", "submission/tf611")]
    assert len(set(map(repr, got.values()))) == 1, got
    assert got["memory"] == [["a", "m", "z"]]
