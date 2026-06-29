"""Tests for django_rakaia.handlers_autodiscover."""

from __future__ import annotations

from django_rakaia.handlers_autodiscover import autodiscover
from rakaia.registry import (
    get_default_registry,
    get_default_upcaster_registry,
)

from .handlers import AUTODISCOVERED_HANDLER_NAME
from .upcasters import AUTODISCOVERED_UPCASTER_MATCH


class TestAutodiscover:
    def test_handler_was_autodiscovered(self):
        # Django app ready() ran autodiscover() during pytest setup.
        # The test handler should now be in the default registry.
        names = [v.name for v in get_default_registry().all_versions()]
        assert AUTODISCOVERED_HANDLER_NAME in names

    def test_upcaster_was_autodiscovered(self):
        matches = [
            u.event_match for u in get_default_upcaster_registry().all_upcasters()
        ]
        assert AUTODISCOVERED_UPCASTER_MATCH in matches

    def test_autodiscover_is_idempotent(self):
        before = len(get_default_registry().all_versions())
        autodiscover()
        autodiscover()
        after = len(get_default_registry().all_versions())
        assert before == after
