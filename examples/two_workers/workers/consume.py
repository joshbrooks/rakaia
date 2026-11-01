"""The apply both the demo and a worker process use.

It writes one ``AppliedEvent`` row per message and nothing else. That is enough
to answer the two questions the demo asks — did every event land exactly once,
and which worker applied what — and it keeps the demo about the cursor rather
than about a projection.
"""

from __future__ import annotations

import json

from workers.models import AppliedEvent
from workers.seed import CONSUMER


def apply_event(worker: str, message) -> None:
    """Project one message; a duplicate raises on the unique offset."""
    event = json.loads(message.data)
    AppliedEvent.objects.create(
        consumer=CONSUMER,
        offset=message.offset,
        worker=worker,
        key=event["key"],
    )
