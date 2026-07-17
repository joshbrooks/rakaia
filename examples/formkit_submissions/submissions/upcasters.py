"""Schema upcasters for the `submissions` stream.

Autodiscovered by django_rakaia on app ready(): importing this module runs the
`@register_upcaster` decorator.

This is the rakaia answer to FormKit form drift at the *producer* level: an
older version of the form emitted `pct` for repeater progress; the current form
emits `progress_pct`. Rather than teaching every handler both spellings (or
running a destructive data migration over historical `Submission` rows), we
normalise on read. Every handler downstream sees only `progress_pct`, no matter
how old the submission is.
"""

from __future__ import annotations

from typing import Any

from rakaia.registry import register_upcaster


def _rename_pct(activity: dict[str, Any]) -> dict[str, Any]:
    """Return a copy of a repeater child with `pct` renamed to `progress_pct`."""
    if "pct" not in activity:
        return activity
    renamed = {k: v for k, v in activity.items() if k != "pct"}
    renamed["progress_pct"] = activity["pct"]
    return renamed


@register_upcaster(event_match="submissions", from_version=1)
def upcast_pct_to_progress_pct(event: dict[str, Any]) -> dict[str, Any]:
    """v1 -> v2: rename the legacy `pct` repeater key to `progress_pct`.

    Pure: never mutates the input event. Rebuilds `fields.activities` with the
    renamed key and returns a new event dict.
    """
    fields = event.get("fields", {})
    activities = [_rename_pct(a) for a in fields.get("activities", [])]
    return {**event, "fields": {**fields, "activities": activities}}
