"""Stage-aware handlers for the close-precondition state machine.

Three replay stages, each visible to the next through a read-only ``refs`` view
(the staged-replay shape validated in ``examples/partisipa_staged``):

* stage 0 — per-event facts: ``Project`` progress, ``Meeting`` verification, and
  one ``FinanceLine`` per FINANCE event.
* stage 1 — the ``Balance`` **aggregate**, *recomputed* from all FinanceLine rows
  for a suku (never incremented), so replay stays idempotent.
* stage 2 — the ``CycleClose`` **guarded transition**: a POM_1 close event is
  ACCEPTED only if every precondition holds against the stage-0/1 projections;
  otherwise REJECTED with the specific failing reasons.

`close_preconditions` is the analogue of Partisipa's real gate.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from rakaia import Effect, Upsert

PROJECT = "lifecycle.Project"
MEETING = "lifecycle.Meeting"
FINANCE_LINE = "lifecycle.FinanceLine"
BALANCE = "lifecycle.Balance"
CYCLE_CLOSE = "lifecycle.CycleClose"

REQUIRED_VERIFIED_MEETINGS = 2


# -- stage 0: per-event facts -----------------------------------------------


def progress(event: dict[str, Any], refs: Any) -> Effect:  # noqa: ARG001
    return Upsert(
        model_label=PROJECT,
        lookup={"suku": event["suku"], "output": event["output"]},
        defaults={"percent": event["percent"]},
    )


def meeting(event: dict[str, Any], refs: Any) -> Effect:  # noqa: ARG001
    return Upsert(
        model_label=MEETING,
        lookup={"suku": event["suku"], "meeting_id": event["meeting_id"]},
        defaults={"verified": event["verified"]},
    )


def finance_line(event: dict[str, Any], refs: Any) -> Effect:  # noqa: ARG001
    return Upsert(
        model_label=FINANCE_LINE,
        lookup={"submission_id": event["key"]},
        defaults={
            "suku": event["suku"],
            "account": event["account"],
            "delta": event["delta"],
        },
    )


# -- stage 1: the replay-safe aggregate -------------------------------------


def balance_rollup(refs: Any) -> list[Effect]:
    """Recompute each suku's balances from its FinanceLine rows.

    This is the aggregate analogue of ``reconcile_children``: on every replay we
    *recompute* the total from the contributing rows and emit one idempotent
    upsert. An increment would double-count on re-replay; a recompute cannot.
    """
    lines = list(refs.query(FINANCE_LINE))
    sukus = sorted({line.suku for line in lines})
    effects: list[Effect] = []
    for suku in sukus:
        rows = [line for line in lines if line.suku == suku]
        operational = sum(
            (r.delta for r in rows if r.account == "operational"), Decimal("0")
        )
        infrastructure = sum(
            (r.delta for r in rows if r.account == "infrastructure"), Decimal("0")
        )
        effects.append(
            Upsert(
                model_label=BALANCE,
                lookup={"suku": suku},
                defaults={
                    "operational": operational,
                    "infrastructure": infrastructure,
                },
            )
        )
    return effects


# -- stage 2: the guarded transition ----------------------------------------


def close_preconditions(suku: str, refs: Any) -> list[str]:
    """Evaluate the POM_1 close gate; return the list of *failing* reasons.

    An empty list means the cycle may close. Mirrors ``close_preconditions.py``:
    all projects 100 %, two verified accountability meetings, both balances ≥ 0.
    """
    reasons: list[str] = []

    projects = list(refs.filter(PROJECT, suku=suku))
    if not projects or any(p.percent < 100 for p in projects):
        reasons.append("incomplete_projects")

    verified = refs.filter(MEETING, suku=suku, verified=True).count()
    if verified < REQUIRED_VERIFIED_MEETINGS:
        reasons.append("insufficient_meetings")

    balance = refs.get(BALANCE, suku=suku)
    if balance is None or balance.operational < 0:
        reasons.append("negative_operational_balance")
    if balance is None or balance.infrastructure < 0:
        reasons.append("negative_infrastructure_balance")

    return reasons


def cycle_close(event: dict[str, Any], refs: Any) -> Effect:
    """POM_1 -> a CycleClose whose status is a pure function of prior stages."""
    reasons = close_preconditions(event["suku"], refs)
    return Upsert(
        model_label=CYCLE_CLOSE,
        lookup={"suku": event["suku"]},
        defaults={
            "status": "ACCEPTED" if not reasons else "REJECTED",
            "reasons": reasons,
        },
    )


# Stage plan: event handlers dispatched per matching form_type, plus per-stage
# `reduce` steps that run once against the prior stages' projections.
STAGES: dict[int, dict[str, Any]] = {
    0: {
        "events": [
            ("PROGRESS", progress),
            ("MEETING", meeting),
            ("FINANCE", finance_line),
        ]
    },
    1: {"reduce": [balance_rollup]},
    2: {"events": [("POM_1", cycle_close)]},
}
