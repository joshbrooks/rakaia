"""
Test handlers module — exercised by the autodiscovery test.

Importing this module registers a single sentinel handler with the
process-wide default registry, which the autodiscovery test asserts on.
"""

from __future__ import annotations

from rakaia.effects import Effect
from rakaia.registry import register_handler

AUTODISCOVERED_HANDLER_NAME = "test_autodiscovered_handler"


@register_handler(
    name=AUTODISCOVERED_HANDLER_NAME,
    event_match="test_autodiscover_stream",
    effective_from=0,
)
def autodiscovered_handler(event: dict) -> Effect:
    return Effect(
        op="update_or_create",
        model_label="test_django_rakaia.Area",
        lookup={"name": event["name"]},
        defaults={},
    )
