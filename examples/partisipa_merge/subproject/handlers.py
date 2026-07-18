"""Handlers for the multi-stream merge spike.

Identical *shape* to the close-precondition spike, but the events feeding them
come from three merged streams instead of one. Three stages:

* stage 0 — per-event facts: ``Project`` / ``Meeting`` / ``FinanceLine``.
* stage 1 — the ``Balance`` aggregate, recomputed from ``FinanceLine``.
* stage 2 — the ``Readiness`` rollup, derived per suku from all prior stages.

Nothing here knows or cares which stream an event came from — merge happens
before dispatch — so the same handlers work for the single-stream baseline and
the three-stream merge. That is the whole point of the parity check.
"""

from __future__ import annotations

from decimal import Decimal
from typing import Any

from rakaia.effects import Effect

PROJECT = "subproject.Project"
MEETING = "subproject.Meeting"
FINANCE_LINE = "subproject.FinanceLine"
BALANCE = "subproject.Balance"
READINESS = "subproject.Readiness"
CLAIM = "subproject.Claim"

REQUIRED_VERIFIED_MEETINGS = 2


# -- stage 0: per-event facts -----------------------------------------------


def progress(event: dict[str, Any], refs: Any) -> Effect:  # noqa: ARG001
    return Effect(
        op="update_or_create",
        model_label=PROJECT,
        lookup={"suku": event["suku"], "output": event["output"]},
        defaults={"percent": event["percent"]},
    )


def meeting(event: dict[str, Any], refs: Any) -> Effect:  # noqa: ARG001
    return Effect(
        op="update_or_create",
        model_label=MEETING,
        lookup={"suku": event["suku"], "meeting_id": event["meeting_id"]},
        defaults={"verified": event["verified"]},
    )


def finance_line(event: dict[str, Any], refs: Any) -> Effect:  # noqa: ARG001
    return Effect(
        op="update_or_create",
        model_label=FINANCE_LINE,
        lookup={"submission_id": event["key"]},
        defaults={
            "suku": event["suku"],
            "account": event["account"],
            "delta": event["delta"],
        },
    )


def claim(event: dict[str, Any], refs: Any) -> Effect | None:  # noqa: ARG001
    """Fires only for events carrying a ``slot`` — the cross-stream LWW witness.

    Registered for both MEETING and FINANCE, but only the tied pair in the seed
    carries a slot, so exactly those two write the row; the one that comes later
    in the merged order wins. Returns None (skipped) for every other event.
    """
    if "slot" not in event:
        return None
    return Effect(
        op="update_or_create",
        model_label=CLAIM,
        lookup={"slot": event["slot"]},
        defaults={"claimed_by": event["key"], "ts": event["ts"]},
    )


# -- stage 1: replay-safe aggregate -----------------------------------------


def balance_rollup(refs: Any) -> list[Effect]:
    lines = list(refs.query(FINANCE_LINE))
    effects: list[Effect] = []
    for suku in sorted({line.suku for line in lines}):
        rows = [line for line in lines if line.suku == suku]
        operational = sum(
            (r.delta for r in rows if r.account == "operational"), Decimal("0")
        )
        infrastructure = sum(
            (r.delta for r in rows if r.account == "infrastructure"), Decimal("0")
        )
        effects.append(
            Effect(
                op="update_or_create",
                model_label=BALANCE,
                lookup={"suku": suku},
                defaults={
                    "operational": operational,
                    "infrastructure": infrastructure,
                },
            )
        )
    return effects


# -- stage 2: cross-stream readiness rollup ---------------------------------


def readiness_reasons(suku: str, refs: Any) -> list[str]:
    reasons: list[str] = []
    projects = list(refs.filter(PROJECT, suku=suku))
    if not projects or any(p.percent < 100 for p in projects):
        reasons.append("incomplete_projects")
    if refs.filter(MEETING, suku=suku, verified=True).count() < REQUIRED_VERIFIED_MEETINGS:
        reasons.append("insufficient_meetings")
    balance = refs.get(BALANCE, suku=suku)
    if balance is None or balance.operational < 0:
        reasons.append("negative_operational_balance")
    if balance is None or balance.infrastructure < 0:
        reasons.append("negative_infrastructure_balance")
    return reasons


def readiness_rollup(refs: Any) -> list[Effect]:
    """One Readiness row per suku seen in any stream — the SQL-stitch analogue."""
    sukus = {p.suku for p in refs.query(PROJECT)} | {
        m.suku for m in refs.query(MEETING)
    } | {line.suku for line in refs.query(FINANCE_LINE)}
    effects: list[Effect] = []
    for suku in sorted(sukus):
        reasons = readiness_reasons(suku, refs)
        effects.append(
            Effect(
                op="update_or_create",
                model_label=READINESS,
                lookup={"suku": suku},
                defaults={"ready": not reasons, "reasons": reasons},
            )
        )
    return effects


STAGES: dict[int, dict[str, Any]] = {
    0: {
        "events": [
            ("PROGRESS", progress),
            ("MEETING", meeting),
            ("MEETING", claim),
            ("FINANCE", finance_line),
            ("FINANCE", claim),
        ]
    },
    1: {"reduce": [balance_rollup]},
    2: {"reduce": [readiness_rollup]},
}
