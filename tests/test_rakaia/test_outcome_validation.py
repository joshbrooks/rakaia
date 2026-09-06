"""What an `Outcome` refuses to be built as.

These had no tests at all: every `raise` in `__post_init__` could be turned into
`if False:` with the full suite green, which means the validation was decoration.
"""

from __future__ import annotations

import uuid

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
    """Without a subject there is nothing to key on, and every refusal on a stream
    collapses into whichever was recorded first. The other two matter for a
    different reason: a file-backed store maps a name to one path segment, and an
    empty name shares a segment with whatever else escapes to the same thing.
    """
    with pytest.raises(ValueError, match="cannot be empty"):
        Outcome(**{**BASE, field: ""}, offset=None, stage="append", status="refused")


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
    [
        {"national_id": "1234", "bank_acct": "999"},
        ["a", "b"],
        42,
        None,
    ],
)
def test_params_refuses_anything_that_is_not_a_string(value):
    """The reason this field exists instead of a rendered message is that field
    values must not reach it. That was a docstring claim and nothing else: a
    nested dict of personal data went in and came back out of storage unchanged.
    """
    with pytest.raises(ValueError, match="has to be text"):
        Outcome(
            **BASE,
            offset=None,
            stage="append",
            status="refused",
            params={"leak": value},
        )


def test_a_refusal_names_the_field_and_shows_its_contents():
    """A refusal that does not say what was wrong is one someone works around.

    The message names the field and prints it, rather than listing offending keys:
    one rule that walks the declaration cannot report per-key without re-deriving
    per-field knowledge, and the printed value is enough to see which entry is bad.
    """
    with pytest.raises(ValueError, match=r"params=\{"):
        Outcome(
            **BASE,
            offset=None,
            stage="append",
            status="refused",
            params={"a": "fine", "b": 1, "c": None},
        )


@pytest.mark.parametrize("key", [1, None, True, ("a", "b")])
def test_params_refuses_a_key_that_is_not_a_string(key):
    """The hole the values check left, found one round later.

    A non-string key does not fail loudly, it makes the backends disagree: an
    integer key survives in memory and comes back as a string from anything that
    serialises, and a key that is hashable but not a primitive records in memory
    and raises on write. Both halves have to be strings for the two to agree.
    """
    with pytest.raises(ValueError, match="has to be text"):
        Outcome(
            **BASE,
            offset=None,
            stage="append",
            status="refused",
            params={key: "x"},
        )


@pytest.mark.parametrize("reason", [1, None, object(), ("nested",)])
def test_reasons_must_be_codes_given_as_strings(reason):
    """Same class as the params checks, and the field the pattern pointed at.

    Rounds fixed `params` values, then `params` keys; `reasons` had both problems
    and neither check. A non-string element records in memory and raises on write,
    so the backends disagree about whether the outcome exists at all.
    """
    with pytest.raises(ValueError, match="has to be text"):
        Outcome(
            **BASE, offset=None, stage="append", status="refused", reasons=(reason,)
        )


@pytest.mark.parametrize(
    "field", ["consumer", "stream_path", "subject", "sequence_key"]
)
@pytest.mark.parametrize("value", [uuid.uuid4(), 42, ("a", "b"), None])
def test_the_name_fields_must_be_strings(field, value):
    """The fifth instance of a defect four rounds closed one field at a time.

    These are declared `str` and nothing made them so. A UUID subject is the case
    that matters — the field is required, consumer-supplied, and documented as
    opaque, which invites a model key — and it records in memory then raises on
    write, so the two backends disagree about whether the record exists.
    """
    # Two refusals are correct here and which one fires depends on the field:
    # `subject=None` is caught as an *empty* subject before anything asks whether it
    # is text, and that is the better message for it. The test pins that one of them
    # happens, not which — naming a single message would make it pass for the wrong
    # reason on that one case.
    # Which refusal fires depends on the value: `None` is caught as an empty name
    # before anything asks whether it is text, and both answers are correct. The
    # test pins that one of them happens, not which — naming a single message would
    # make it pass for the wrong reason on that case.
    with pytest.raises(ValueError):
        Outcome(**{**BASE, field: value}, offset=None, stage="append", status="refused")


@pytest.mark.parametrize("value", [42, ("a", "b"), uuid.uuid4()])
def test_an_offset_must_be_a_string_when_there_is_one(value):
    """An offset is an opaque token, never a number — parsing one is already
    forbidden, and storing a non-string is the same coupling from the other end."""
    with pytest.raises(ValueError, match="has to be text"):
        Outcome(**BASE, offset=value, stage="project", status="failed")


def test_an_outcome_does_not_change_when_the_caller_mutates_what_it_was_built_from():
    """`frozen=True` freezes the binding, not the containers behind it.

    Asserted on the object with no store involved, deliberately. The codec copies
    on the way into storage, so every store is safe whether or not this holds — and
    that is exactly why it needs its own test: without one, the copy here is
    unpinned and reads as redundant, while the promise `frozen=True` makes to
    anyone holding an outcome quietly stops being true.
    """
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
