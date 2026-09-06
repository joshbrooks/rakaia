"""What an `Outcome` refuses to be built as.

These had no tests at all: every `raise` in `__post_init__` could be turned into
`if False:` with the full suite green, which means the validation was decoration.
"""

from __future__ import annotations

import uuid
from decimal import Decimal

import pytest

from rakaia.outcomes import Outcome

BASE = {
    "consumer": "c",
    "stream_path": "s",
    "subject": "row-1",
    "sequence_key": "seq",
}


@pytest.mark.parametrize("field", ["consumer", "stream_path", "subject"])
def test_the_names_cannot_be_empty(field):
    """Without a subject there is nothing to key on. The other two matter because a
    file-backed store maps a name to one path segment, and an empty name shares a
    segment with whatever else escapes to the same thing."""
    with pytest.raises(ValueError, match="cannot be empty"):
        Outcome(**{**BASE, field: ""}, offset=None, stage="append", status="refused")


@pytest.mark.parametrize("field", ["consumer", "stream_path", "subject", "offset"])
@pytest.mark.parametrize(
    "value", [42, uuid.uuid4(), ("a", "b")], ids=lambda v: type(v).__name__
)
def test_what_a_store_sorts_and_names_by_has_to_be_text(field, value):
    """Not a type rule for its own sake — a rule about four specific fields.

    A number stored as a name raises in a file store's path escaping. A number
    stored as an *offset* is kept identically by every store and then makes
    `latest` raise the moment a text offset sits beside it, because the two cannot
    be compared: consistent, and still broken. Everything an outcome merely carries
    is settled by storing it instead.
    """
    kw = {**BASE, "offset": None, "stage": "append", "status": "refused"}
    if field == "offset":
        kw |= {"offset": value, "stage": "project", "status": "failed"}
    else:
        kw[field] = value
    with pytest.raises(ValueError, match="writable text"):
        Outcome(**kw)


def test_a_text_offset_and_a_missing_one_sort_together():
    """The failure the rule above prevents, as the read path sees it."""
    from rakaia.outcomes import InMemoryOutcomeStore

    store = InMemoryOutcomeStore()
    store.record(Outcome(**BASE, offset="0000000001", stage="project", status="failed"))
    store.record(
        Outcome(
            **{**BASE, "subject": "row-2"},
            offset=None,
            stage="append",
            status="refused",
        )
    )
    assert [o.offset for o in store.latest("c", "s")] == ["0000000001", None]


@pytest.mark.parametrize("field", ["consumer", "stream_path", "subject"])
def test_a_name_that_cannot_be_written_is_refused(field):
    """A lone surrogate is text by every type check and no path can carry it, so it
    passes construction and raises inside the store — in the loop whose job is
    recording that something failed. The one storage constraint the codec cannot
    reach, because it is about the name rather than the payload."""
    with pytest.raises(ValueError, match="writable text"):
        Outcome(
            **{**BASE, field: "x\ud800"}, offset=None, stage="append", status="refused"
        )


def test_an_appended_outcome_cannot_carry_an_offset():
    with pytest.raises(ValueError, match="no offset"):
        Outcome(**BASE, offset="0000000001", stage="append", status="refused")


def test_a_projected_outcome_needs_an_offset():
    with pytest.raises(ValueError, match="needs an offset"):
        Outcome(**BASE, offset=None, stage="project", status="failed")


def test_attempts_count_from_one():
    with pytest.raises(ValueError, match="counts from 1"):
        Outcome(**BASE, offset=None, stage="append", status="refused", attempt=0)


@pytest.mark.parametrize(
    "value",
    [uuid.uuid4(), Decimal("1.5"), b"bytes", object()],
    ids=lambda v: type(v).__name__,
)
def test_anything_that_cannot_be_stored_as_text_is_refused(value):
    """The whole storability rule, and the only one there is.

    It refuses by *doing the storing* rather than by describing which types are
    allowed. Seven review findings were values that satisfied a type rule and still
    did not survive storage — a text subclass that flattens, a lone surrogate, a
    string longer than a column. That set is open-ended, so a description of it
    leaks by construction.
    """
    with pytest.raises(ValueError, match="storable as text"):
        Outcome(
            **BASE, offset=None, stage="append", status="refused", params={"k": value}
        )


def test_an_outcome_does_not_change_when_the_caller_mutates_what_it_was_built_from():
    """`frozen=True` freezes the binding, not the containers behind it."""
    reasons = ["missing_total"]
    params = {"row": "3"}
    outcome = Outcome(
        **BASE,
        offset=None,
        stage="append",
        status="refused",
        reasons=reasons,
        params=params,
    )
    reasons.append("LEAKED")
    params["national_id"] = "1234-LEAK"
    assert outcome.reasons == ("missing_total",)
    assert outcome.params == {"row": "3"}
