"""What an `Outcome` refuses to be built as.

These had no tests at all: every `raise` in `__post_init__` could be turned into
`if False:` with the full suite green, which means the validation was decoration.
"""

from __future__ import annotations

import pytest

from rakaia.outcomes import Outcome

BASE = {
    "consumer": "c",
    "stream_path": "s",
    "subject": "row-1",
    "sequence_key": "seq",
}


def test_an_outcome_needs_a_subject():
    """Without one there is nothing to key on, and every refusal on a stream
    collapses into whichever was recorded first."""
    with pytest.raises(ValueError, match="needs a subject"):
        Outcome(
            **{**BASE, "subject": ""}, offset=None, stage="append", status="refused"
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
    with pytest.raises(ValueError, match="must be strings"):
        Outcome(
            **BASE,
            offset=None,
            stage="append",
            status="refused",
            params={"leak": value},
        )


def test_params_names_the_offending_keys():
    """A refusal that does not say which key is a refusal someone works around."""
    with pytest.raises(ValueError, match=r"\['b', 'c'\]"):
        Outcome(
            **BASE,
            offset=None,
            stage="append",
            status="refused",
            params={"a": "fine", "b": 1, "c": None},
        )
