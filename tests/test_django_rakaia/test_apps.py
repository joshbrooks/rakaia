"""Tests for `DjangoRakaiaConfig.ready()` SSE-signal gating (issue #41).

The framework tier (handler/upcaster autodiscovery) must load with no
`channels` dependency; the protocol tier (SSE broadcast via `channels_signals`)
must only be wired when it is actually wanted. These tests pin that boundary:

* `RAKAIA_ENABLE_SSE=False` — never wire, even if `channels` is installed.
* unset + `channels` importable — auto-wire (preserves today's behaviour).
* unset + `channels` missing — skip silently (framework-only consumer).
* `RAKAIA_ENABLE_SSE=True` + `channels` missing — fail loud (you asked for it).
"""

from __future__ import annotations

import sys

import pytest
from django.apps import apps
from django.test import override_settings

_SIGNALS = "django_rakaia.channels_signals"


def _config():
    return apps.get_app_config("django_rakaia")


class TestSseSignalGating:
    def test_disabled_setting_never_wires_signals(self, monkeypatch):
        # Explicit opt-out: channels_signals must not be (re)imported even though
        # `channels` is installed in this environment.
        monkeypatch.delitem(sys.modules, _SIGNALS, raising=False)
        with override_settings(RAKAIA_ENABLE_SSE=False):
            _config()._wire_sse_signals()
        assert _SIGNALS not in sys.modules

    def test_auto_detect_wires_when_channels_available(self, monkeypatch):
        # Default (no setting) + channels present → signals wired.
        monkeypatch.delitem(sys.modules, _SIGNALS, raising=False)
        _config()._wire_sse_signals()
        assert _SIGNALS in sys.modules

    def test_auto_detect_skips_silently_when_channels_missing(self, monkeypatch):
        # Framework-only consumer: no `channels`, no setting → skip, no raise.
        monkeypatch.setitem(sys.modules, "channels", None)
        monkeypatch.setitem(sys.modules, "channels.layers", None)
        monkeypatch.delitem(sys.modules, _SIGNALS, raising=False)
        _config()._wire_sse_signals()  # must not raise
        assert _SIGNALS not in sys.modules

    def test_explicit_enable_raises_when_channels_missing(self, monkeypatch):
        # Opting in but forgetting the dependency is a real error, surfaced loudly.
        monkeypatch.setitem(sys.modules, "channels", None)
        monkeypatch.setitem(sys.modules, "channels.layers", None)
        monkeypatch.delitem(sys.modules, _SIGNALS, raising=False)
        with override_settings(RAKAIA_ENABLE_SSE=True), pytest.raises(ImportError):
            _config()._wire_sse_signals()
