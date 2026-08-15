"""Versioned handlers for the `submissions` stream.

Autodiscovered by django_rakaia on app ready(): importing this module runs the
`@register_handler` decorators below.

This is the rakaia re-expression of FormKit-Ninja's Stage 3 hydration
(`SeparatedSubmission.to_model()`). Two handler *names* fire on every
submission event, independently:

* ``visit_summary`` — writes the root ``MonitoringVisit`` row. Two versions
  split at the completion-policy change (seq 2). Visits recorded before it keep
  the strict 100% rule; later ones use the 90% tolerance rule. This is the
  time-correctness guarantee that plain Django signals cannot give you: fixing
  the rule going forward never rewrites historical rows.

* ``activity_rows`` — a *fan-out* handler. It returns a **list** of Effects,
  one ``ActivityProgress`` row per repeater child, keyed by
  ``(submission_id, activity_index)``. It writes a different model than
  ``visit_summary``, so the two never collide on a row.

Handlers are pure: they take an (already upcasted) event dict and return
``Effect`` descriptions — no I/O, no DB writes. The DjangoExecutor applies them,
which is what makes replay idempotent and re-runnable.
"""

from __future__ import annotations

from typing import Any

from rakaia import Effect, reconcile_children, register_handler

from . import mapping
from .seed import POLICY_CHANGE_SEQ

VISIT_MODEL = "submissions.MonitoringVisit"
ACTIVITY_MODEL = "submissions.ActivityProgress"


def _visit_effect(event: dict[str, Any], *, lenient: bool) -> Effect:
    return Effect(
        op="update_or_create",
        model_label=VISIT_MODEL,
        lookup={"submission_id": event["submission_id"]},
        defaults=mapping.visit_defaults(event, lenient=lenient),
    )


# ---------------------------------------------------------------------------
# visit_summary v1 — strict rule: COMPLETE only at 100%. Active for seq [0, 2).
# ---------------------------------------------------------------------------
@register_handler(
    name="visit_summary",
    event_match="submissions",
    effective_from=0,
    effective_to=POLICY_CHANGE_SEQ,
)
def visit_summary_v1(event: dict[str, Any]) -> Effect:
    return _visit_effect(event, lenient=False)


# ---------------------------------------------------------------------------
# visit_summary v2 — tolerance rule: COMPLETE at 90%. Active for seq [2, None).
# ---------------------------------------------------------------------------
@register_handler(
    name="visit_summary",
    event_match="submissions",
    effective_from=POLICY_CHANGE_SEQ,
    effective_to=None,
)
def visit_summary_v2(event: dict[str, Any]) -> Effect:
    return _visit_effect(event, lenient=True)


# ---------------------------------------------------------------------------
# activity_rows — fan-out over the repeater, all seqs. Writes ActivityProgress
# (disjoint from visit_summary's MonitoringVisit). reconcile_children emits one
# upsert per current activity PLUS a reconcile delete, so a resubmission with
# fewer activities prunes the dropped rows instead of orphaning them.
# ---------------------------------------------------------------------------
@register_handler(
    name="activity_rows",
    event_match="submissions",
    effective_from=0,
    effective_to=None,
)
def activity_rows(event: dict[str, Any]) -> list[Effect]:
    return reconcile_children(
        model_label=ACTIVITY_MODEL,
        parent_lookup={"submission_id": event["submission_id"]},
        child_key="activity_index",
        items=mapping.activities(event),
        defaults_fn=mapping.activity_defaults,
    )
