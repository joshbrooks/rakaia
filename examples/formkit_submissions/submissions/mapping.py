"""Pure mapping helpers shared by the handlers *and* the reference `to_model`.

Keeping the field derivation in one place is deliberate: the whole point of the
prototype is to prove that rakaia's replay pipeline (flatten -> Effect ->
executor) reproduces FormKit-Ninja's direct `to_model()` write. If the two
paths derived fields differently, an equal result would be luck. Sharing these
pure functions means any parity we observe is attributable to the *plumbing*
being equivalent — which is the actual risk surface of adopting rakaia.

None of these functions touch the database or the event's `schema_version`; by
the time a handler runs, the upcaster has already normalised the payload (see
upcasters.py — legacy `pct` is renamed to `progress_pct`).
"""

from __future__ import annotations

from decimal import ROUND_HALF_UP, Decimal
from typing import Any


def root_fields(event: dict[str, Any]) -> dict[str, Any]:
    """The submission's scalar root fields (Stage 3 hydration inputs)."""
    fields = event.get("fields", {})
    return {
        "form_type": event.get("form_type", ""),
        "project_code": fields.get("project_code", ""),
        "suku": fields.get("suku", ""),
        "monitor": fields.get("monitor", ""),
        "visit_date": fields.get("visit_date", ""),
    }


def activities(event: dict[str, Any]) -> list[dict[str, Any]]:
    """The repeater children (FormKit-Ninja's `repeater_parent` rows)."""
    return list(event.get("fields", {}).get("activities", []))


def _budget(activity: dict[str, Any]) -> Decimal:
    return Decimal(str(activity.get("budget", "0")))


def _progress(activity: dict[str, Any]) -> int:
    return int(activity.get("progress_pct", 0))


def total_budget(event: dict[str, Any]) -> Decimal:
    return sum((_budget(a) for a in activities(event)), Decimal(0))


def overall_progress(event: dict[str, Any]) -> Decimal:
    """Budget-weighted mean progress across activities, to 2 dp."""
    acts = activities(event)
    total = total_budget(event)
    if not acts or total == 0:
        return Decimal("0.00")
    weighted = sum((_budget(a) * _progress(a) for a in acts), Decimal(0))
    return (weighted / total).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


# The tolerance threshold introduced by the v2 policy.
LENIENT_THRESHOLD = Decimal(90)
STRICT_THRESHOLD = Decimal(100)


def visit_status(progress: Decimal, *, lenient: bool) -> str:
    """COMPLETE vs IN_PROGRESS, under the strict (v1) or lenient (v2) rule."""
    threshold = LENIENT_THRESHOLD if lenient else STRICT_THRESHOLD
    return "COMPLETE" if progress >= threshold else "IN_PROGRESS"


def visit_defaults(event: dict[str, Any], *, lenient: bool) -> dict[str, Any]:
    """The full `defaults=` payload for a MonitoringVisit update_or_create."""
    progress = overall_progress(event)
    return {
        **root_fields(event),
        "total_budget": total_budget(event),
        "overall_progress": progress,
        "status": visit_status(progress, lenient=lenient),
    }


def activity_defaults(activity: dict[str, Any]) -> dict[str, Any]:
    """The `defaults=` payload for one ActivityProgress row."""
    return {
        "name": activity.get("name", ""),
        "budget": _budget(activity),
        "progress_pct": _progress(activity),
    }
