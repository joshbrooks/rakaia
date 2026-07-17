"""Reference implementation: FormKit-Ninja's *direct* `to_model()` write path.

This is the baseline the prototype validates against. It reproduces, in plain
imperative Django, what `SeparatedSubmission.to_model()` does today: flatten a
submission and `update_or_create` the typed rows directly — no stream, no
handlers, no replay.

The `demo_submissions` command runs this alongside rakaia's replay and asserts
the two projections are byte-identical. Both call the shared pure helpers in
`mapping.py`, so any equality we observe is attributable to the rakaia
*plumbing* (flatten -> Effect -> executor) being faithful to a direct write —
which is the real question when deciding whether to adopt rakaia.

Two policies are offered:

* ``policy_seq`` given  -> *time-correct* population: each submission uses the
  rule that was in force at its sequence number. This matches what rakaia's
  versioned handlers do, and is what parity is asserted against.
* ``policy_seq=None``   -> *always-current* population: every row uses today's
  lenient rule. This is what a naive `to_model()` re-run does — and it silently
  rewrites history. The command uses it to quantify what versioning buys you.
"""

from __future__ import annotations

from typing import Any

from . import mapping
from .models import ActivityProgress, MonitoringVisit


def to_model(event: dict[str, Any], *, lenient: bool) -> None:
    """Populate the typed rows for one submission (FormKit-Ninja Stage 3)."""
    MonitoringVisit.objects.update_or_create(
        submission_id=event["submission_id"],
        defaults=mapping.visit_defaults(event, lenient=lenient),
    )
    for index, activity in enumerate(mapping.activities(event)):
        ActivityProgress.objects.update_or_create(
            submission_id=event["submission_id"],
            activity_index=index,
            defaults=mapping.activity_defaults(activity),
        )


def populate(events: list[dict[str, Any]], *, policy_seq: int | None) -> None:
    """Run to_model() over every submission.

    `policy_seq` selects the completion rule per submission: time-correct when
    given (rule in force at that seq), always-lenient when None.
    """
    for seq, event in enumerate(events):
        lenient = True if policy_seq is None else seq >= policy_seq
        to_model(event, lenient=lenient)
