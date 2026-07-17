"""Read-only view of the materialized submission projection.

The rows shown here are written by replaying the submissions stream (see the
`demo_submissions` management command). This view never mutates anything.
"""

from typing import Any

from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET

from .models import ActivityProgress, MonitoringVisit
from .seed import POLICY_CHANGE_SEQ


@require_GET
def index(request: Any) -> HttpResponse:
    visits = list(MonitoringVisit.objects.all())
    by_submission: dict[str, list[ActivityProgress]] = {}
    for activity in ActivityProgress.objects.all():
        by_submission.setdefault(activity.submission_id, []).append(activity)
    rows = [(v, by_submission.get(v.submission_id, [])) for v in visits]
    return render(
        request,
        "submissions/index.html",
        {"rows": rows, "policy_change_seq": POLICY_CHANGE_SEQ},
    )
