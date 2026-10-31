"""The apply half of the migrate demo: one translation event, one snapshot row.

Absolute values, not deltas, so delivering an event twice writes the same row
twice. That is the property `Consumer.run` requires of every apply, and it is
what makes a migration that leaves the position alone safe to test.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable

from rakaia.outcomes import Outcome
from rakaia.types import StreamMessage

from .models import TranslationSnapshot


def make_apply() -> Callable[[StreamMessage], Iterable[Outcome] | None]:
    """An idempotent apply: set one snapshot row to the event's absolute value."""

    def apply(message: StreamMessage) -> Iterable[Outcome] | None:
        event = json.loads(message.data)
        TranslationSnapshot.objects.update_or_create(
            langcode=event["langcode"],
            msgid=event["msgid"],
            defaults={"msgstr": event.get("msgstr") or ""},
        )
        return None

    return apply
