"""
Test upcasters module — exercised by the autodiscovery test.

Importing this module registers a single sentinel upcaster with the
process-wide default registry.
"""

from __future__ import annotations

from rakaia.registry import register_upcaster

AUTODISCOVERED_UPCASTER_MATCH = "test_autodiscover_stream"


@register_upcaster(event_match=AUTODISCOVERED_UPCASTER_MATCH, from_version=1)
def autodiscovered_upcaster(event: dict) -> dict:
    return {**event, "added_by_autodiscover": True}
