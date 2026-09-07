"""Producing side: split a progress form into row events, and refuse the bad rows.

This is the half of ADR 0007's worked example that happens *before* the log. A
form is a document of repeating rows; each row is checked, the acceptable ones
are appended and get real offsets, and a row the rules decline is dropped from
the batch — neither appended nor projected, on the stated principle that the log
must not carry a row the consumer declined to accept.

Refusing is not losing. The fact is still sitting in whatever the form was filled
in from, so the recovery is to fix it there and submit again; that is why the
record is ``stage="append"``, ``status="refused"`` and carries no offset. It is
written through the same outcome store the consuming loop writes to, so one query
answers "what did not get through today" for both sides of the log.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any

from rakaia.outcomes import Outcome
from rakaia.protocols import OutcomeStore

from .gate import check_row


@dataclass(frozen=True)
class Submitted:
    """What one form did: the offsets it produced, and the rows it did not."""

    appended: tuple[str, ...]
    """One offset per row that reached the log, in row order."""

    refused: tuple[Outcome, ...]
    """The record written for each row the gate declined."""


def row_key(form: dict[str, Any], row: dict[str, Any]) -> str:
    """What an outcome about this row is *about*: suku, output and period.

    Stable across submissions of the same form, so a row refused twice is two
    attempts at one subject rather than two unrelated records.
    """
    return f"{form['suku']}/{row.get('output')}/{form['period']}"


def submit_form(
    form: dict[str, Any],
    *,
    store: Any,
    outcomes: OutcomeStore,
    consumer: str,
    path: str,
) -> Submitted:
    """Append every acceptable row of `form`, and record the refused ones.

    `store` is any store that appends — the durable one in the example. The
    siblings of a refused row are appended exactly as they would have been: the
    rows of one form are independent facts, and declining one is not declining the
    submission.
    """
    appended: list[str] = []
    refused: list[Outcome] = []

    for row in form["rows"]:
        reasons = check_row(row)
        if reasons:
            outcome = Outcome(
                consumer=consumer,
                stream_path=path,
                subject=row_key(form, row),
                # No offset: the event was never written, so there is no position
                # to name and nothing for a replay to find.
                offset=None,
                sequence_key=form["key"],
                stage="append",
                status="refused",
                reasons=reasons,
                params={"form": form["key"], "reported": str(row.get("percent"))},
            )
            outcomes.record(outcome)
            refused.append(outcome)
            continue

        result = store.append(path, json.dumps(_event(form, row)).encode())
        assert result.message is not None  # an accepted append always has one
        appended.append(result.message.offset)

    return Submitted(appended=tuple(appended), refused=tuple(refused))


def _event(form: dict[str, Any], row: dict[str, Any]) -> dict[str, Any]:
    """One row of a form, as the event the log carries."""
    return {
        "schema_version": 1,
        "form_type": form["form_type"],
        "form_key": form["key"],
        "row_key": row_key(form, row),
        "suku": form["suku"],
        "period": form["period"],
        "output": row["output"],
        "percent": row["percent"],
    }
