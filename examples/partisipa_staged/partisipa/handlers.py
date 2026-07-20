"""Stage-aware handlers for the staged-replay spike.

These prototype the API proposed in issue #7 (feature #1): a handler declares a
**stage**, and stage > 0 handlers receive a read-only ``refs`` accessor over the
projections materialized by earlier stages. Handlers stay pure with respect to
the event; ``refs`` is itself a pure function of the log (all earlier-stage
events applied), so replay stays deterministic.

* stage 0 — ``project_registry`` builds the reference ``Project`` rows.
* stage 1 — ``sf12_link`` links each SF_1_2 to its Project via ``refs``.

Because stage 0 fully completes before stage 1 runs, an SF_1_2 that arrived in
the stream *before* its TF_6_1_1 still links — the late-arrival problem that
Partisipa solves today with reactive re-save signals + backfill tasks.
"""

from __future__ import annotations

from typing import Any

from rakaia.effects import Effect

PROJECT_MODEL = "partisipa.Project"
SF12_MODEL = "partisipa.Sf12"


def project_registry(event: dict[str, Any], refs: Any) -> Effect:  # noqa: ARG001
    """TF_6_1_1 -> a Project keyed by (suku, output). Ignores refs (stage 0)."""
    return Effect(
        op="update_or_create",
        model_label=PROJECT_MODEL,
        lookup={"suku": event["suku"], "output": event["output"]},
        defaults={"name": event["project_name"]},
    )


def sf12_link(event: dict[str, Any], refs: Any) -> Effect:
    """SF_1_2 -> an Sf12 row linked to its Project, resolved via refs (stage 1)."""
    project = refs.get(PROJECT_MODEL, suku=event["suku"], output=event["output"])
    return Effect(
        op="update_or_create",
        model_label=SF12_MODEL,
        lookup={"submission_id": event["key"]},
        defaults={
            "suku": event["suku"],
            "output": event["output"],
            "cost": event["cost"],
            "project_id": project.pk if project else None,
            "link_reason": "NM" if project else "NPO",
        },
    )


# Handlers grouped by (form_type, fn). The demo dispatches an event to the
# handlers whose form_type matches event["form_type"] — the same routing idea
# as the shipped `match_field`, done explicitly here so the spike is self-
# contained.
STAGE_0 = [("TF_6_1_1", project_registry)]
STAGE_1 = [("SF_1_2", sf12_link)]

# The proposed staged shape: stage index -> handlers active in that stage.
STAGED: dict[int, list[tuple[str, Any]]] = {0: STAGE_0, 1: STAGE_1}

# The "signals today" shape: every handler in one pass, applied per event.
ALL_ONE_STAGE = STAGE_0 + STAGE_1
