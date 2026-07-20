"""The event envelope this spike proposes making first-class in rakaia (issue #11).

A naive ``stream.append(fields)`` records *what* the row became. Partisipa's three
`django-pghistory` consumers also need *who* changed it, *when*, and *what kind*
of change it was — the audit metadata. The envelope carries that alongside the
payload:

    {"schema_version": 1, "key": <submission uuid>,
     "op": "create" | "update" | "delete",
     "actor": <user>, "ts": <iso8601>,
     "fields": {<full post-change snapshot>}}

``op`` is the one fact a plain append throws away; everything the `/history` API
and the recovery command read is derivable from it. ``op`` maps to the
pghistory trigger label and to the `/history` ``+``/``~``/``-`` diff marker.
"""

from __future__ import annotations

import json
from typing import Any

# op -> `/history` API diff marker (schema.py maps pgh_label the same way)
OP_TO_LABEL = {"create": "+", "update": "~", "delete": "-"}
# op -> django-pghistory trigger label recorded in pgh_event.pgh_label
OP_TO_PGH = {"create": "insert", "update": "update", "delete": "delete"}
# and back, so a pghistory row can be compared to a stream-derived entry
PGH_TO_LABEL = {"insert": "+", "update": "~", "delete": "-"}


def make_event(save: dict[str, Any]) -> dict[str, Any]:
    """Build a stream envelope from a raw save record (the source of truth)."""
    return {
        "schema_version": 1,
        "key": save["key"],
        "op": save["op"],
        "actor": save["actor"],
        "ts": save["ts"],
        "fields": save["fields"],
    }


def canonical(fields: dict[str, Any]) -> str:
    """Deterministic JSON for byte-for-byte snapshot comparison."""
    return json.dumps(fields, sort_keys=True, separators=(",", ":"))
