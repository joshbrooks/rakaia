"""The status quo: what `django-pghistory` records today.

`@pghistory.track()` on ``Submission`` writes one ``pgh_event`` row per save —
a full post-change snapshot, the trigger label, a timestamp, and (via
``pghistory.middleware.HistoryMiddleware``) the acting user pulled from the
request context. This module reproduces that faithfully into ``PghEventGolden``
so the spike has a golden audit table to check the stream against.

This is the thing issue #11 proposes retiring; the stream must reproduce every
column a `/history` response and `repair_blank_save_dataloss` read from here.
"""

from __future__ import annotations

from typing import Any

from .envelope import OP_TO_PGH
from .models import PghEventGolden


def simulate(saves: list[dict[str, Any]]) -> None:
    """Populate the golden pghistory table exactly as tracking would."""
    PghEventGolden.objects.all().delete()
    for save in saves:
        PghEventGolden.objects.create(
            submission_id=save["key"],
            pgh_label=OP_TO_PGH[save["op"]],
            pgh_context_user=save["actor"],
            pgh_created_at=save["ts"],
            fields=save["fields"],
        )


def recover_peak_snapshot(submission_id: str) -> dict[str, Any]:
    """`repair_blank_save_dataloss`, pghistory edition.

    Restore a truncated submission from its historical *peak* — the tracked
    snapshot with the most fields. This is the query the real recovery command
    runs against ``pgh_event``.
    """
    rows = PghEventGolden.objects.filter(submission_id=submission_id)
    return max((r.fields for r in rows), key=len, default={})
