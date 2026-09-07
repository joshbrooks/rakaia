"""Consuming side: what one progress-row event does when the loop hands it over.

`django_consumer` owns the reading position and the records; this module owns the
one decision the loop cannot make for anyone — what applying an event means. Three
answers, and each is one row of ADR 0007's recovery table:

* it applies, and nothing is recorded. The position moving past it *is* the
  success record;
* it raises, because the event names a suku that has not been registered yet. The
  event is safe in the log and unapplied, so a later run recovers it — and under
  ``halt`` the position stays below it so that later run is simply the next one;
* it returns an outcome and writes nothing, because the reporting period is
  closed. Returning one is not failing: the event applied, in the sense that the
  decision about it was made, and the position advances.

The write itself is behind `ProgressRows` so the decision can be read without the
tables. `rows.DatabaseRows` is the real one.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from typing import Protocol

from rakaia.outcomes import Outcome
from rakaia.types import StreamMessage


class UnknownSuku(LookupError):
    """The event names a suku no reference row exists for yet.

    A real fault rather than a rule: the row is legitimate and the administrative
    data behind it has not arrived, so the answer is to load it and let the event
    be delivered again — not to declare the row unacceptable.
    """


class ProgressRows(Protocol):
    """Where a progress row lands, and what the projection already knows."""

    def period_is_open(self, period: str) -> bool:
        """Whether the reporting period still accepts rows."""
        ...

    def upsert(self, *, suku: str, output: str, period: str, percent: int) -> None:
        """Write one row. Raises `UnknownSuku` when the suku is not registered."""
        ...


def make_apply(
    rows: ProgressRows,
    *,
    consumer: str,
    path: str,
) -> Callable[[StreamMessage], Iterable[Outcome] | None]:
    """The `apply` to hand to `Consumer.run`, writing into `rows`.

    Idempotent, as the loop requires: the write is an upsert keyed on the row's
    own identity, so an event delivered twice — after a halt, or after a crash
    between applying and committing — lands on the same row with the same value.
    """

    def apply(message: StreamMessage) -> Iterable[Outcome] | None:
        event = json.loads(message.data)

        if not rows.period_is_open(event["period"]):
            return [
                Outcome(
                    consumer=consumer,
                    stream_path=path,
                    subject=message.offset,
                    offset=message.offset,
                    sequence_key=event["form_key"],
                    stage="project",
                    status="skipped",
                    reasons=("period_closed",),
                    params={"period": event["period"], "row": event["row_key"]},
                )
            ]

        rows.upsert(
            suku=event["suku"],
            output=event["output"],
            period=event["period"],
            percent=event["percent"],
        )
        return None

    return apply
