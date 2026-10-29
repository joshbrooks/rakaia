"""GitHub #341: `@stream_model` bypasses the store's append rules and the chosen store."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from django.test import override_settings

from django_rakaia.decorators import create_stream_event
from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import Stream, StreamEntry
from django_rakaia.store import get_store, reset_store_cache

from .models import FinanceLine

pytestmark = pytest.mark.django_db


@dataclass
class _Payload:
    x: int = 1


def _save_event(path: str) -> None:
    line = FinanceLine(submission_id="a", suku="s", delta=1)
    create_stream_event(path, lambda _obj: _Payload(), line, "update")


@pytest.mark.xfail(
    strict=True, reason="GitHub #341: a model save appends to a closed stream"
)
def test_model_save_does_not_append_to_a_closed_stream():
    store = DjangoStreamStore()
    store.create("closed-one")
    store.close_stream("closed-one")

    _save_event("closed-one")

    stream = Stream.objects.get(stream_id="closed-one")
    assert StreamEntry.objects.filter(stream=stream).count() == 0


@pytest.mark.xfail(strict=True, reason="GitHub #341: a model save ignores RAKAIA_STORE")
def test_model_save_goes_to_the_chosen_store():
    reset_store_cache()
    try:
        with override_settings(RAKAIA_STORE="memory"):
            _save_event("chosen")
            messages, _ = get_store().read("chosen")
            assert len(messages) == 1
    finally:
        reset_store_cache()
