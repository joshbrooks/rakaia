"""`rakaia.W003`: `@stream_model` with a store that forgets on restart (#341)."""

from __future__ import annotations

import pytest

from django_rakaia import decorators
from django_rakaia.checks import check_stream_model_store

from .models import Area


@pytest.fixture
def registered(monkeypatch):
    monkeypatch.setattr(decorators, "streamed_models", [Area])


@pytest.mark.usefixtures("registered")
def test_warns_for_a_streamed_model_on_the_memory_store(settings):
    settings.RAKAIA_STORE = "memory"
    assert [w.id for w in check_stream_model_store(None)] == ["rakaia.W003"]


@pytest.mark.usefixtures("registered")
def test_warns_when_the_store_was_never_set(settings):
    del settings.RAKAIA_STORE
    assert [w.id for w in check_stream_model_store(None)] == ["rakaia.W003"]


@pytest.mark.usefixtures("registered")
def test_silent_on_the_durable_store(settings):
    settings.RAKAIA_STORE = "durable"
    assert check_stream_model_store(None) == []


def test_silent_with_no_streamed_models(settings, monkeypatch):
    monkeypatch.setattr(decorators, "streamed_models", [])
    settings.RAKAIA_STORE = "memory"
    assert check_stream_model_store(None) == []
