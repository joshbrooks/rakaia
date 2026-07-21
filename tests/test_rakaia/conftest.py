"""Shared fixtures for the framework-tier (`rakaia`) test suite.

The autouse `_isolate_default_registries` fixture resets the process-wide default
handler/upcaster registries around every test, so registrations made against the
default (via a bare `@register_handler` / `@register_upcaster` / `@register_reducer`)
cannot leak from one test into the next. This is the test-isolation tripwire #38
asks for; tests that construct and inject their own `HandlerRegistry()` are
unaffected. See the test-isolation section of ``docs/versioned-handlers.md``.
"""

from __future__ import annotations

import pytest

from rakaia.registry import reset_default_registries


@pytest.fixture(autouse=True)
def _isolate_default_registries():
    reset_default_registries()
    yield
    reset_default_registries()
