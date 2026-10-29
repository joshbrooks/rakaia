"""GitHub #341: `@stream_model` bypasses the store's append rules and the chosen store."""

from __future__ import annotations

from dataclasses import dataclass

import pytest
from django.test import override_settings

from django_rakaia.decorators import create_stream_event
from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import Stream, StreamEntry
from django_rakaia.store import get_store, reset_store_cache
from rakaia.types import StreamClosed

from .models import FinanceLine

pytestmark = pytest.mark.django_db


@dataclass
class _Payload:
    x: int = 1


def _save_event(path: str) -> None:
    line = FinanceLine(submission_id="a", suku="s", delta=1)
    create_stream_event(path, lambda _obj: _Payload(), line, "update")


def test_model_save_does_not_append_to_a_closed_stream():
    store = DjangoStreamStore()
    store.create("closed-one")
    store.close_stream("closed-one")

    with override_settings(RAKAIA_STORE="durable"), pytest.raises(StreamClosed):
        _save_event("closed-one")

    stream = Stream.objects.get(stream_id="closed-one")
    assert StreamEntry.objects.filter(stream=stream).count() == 0


def test_model_save_goes_to_the_chosen_store():
    reset_store_cache()
    try:
        with override_settings(RAKAIA_STORE="memory"):
            _save_event("chosen")
            messages, _ = get_store().read("chosen")
            assert len(messages) == 1
    finally:
        reset_store_cache()


@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
def test_a_refused_fan_out_leaves_nothing_behind_on_its_own_database():
    # `a` is closed and `b` does not exist yet. The refusal must undo the
    # creation of `b` on the *save's* database; with no `using=` on the
    # transaction, `b` would already be committed there.
    store = DjangoStreamStore(using="overlay")
    store.create("a")
    store.close_stream("a")

    line = FinanceLine(submission_id="a", suku="s", delta=1)
    with override_settings(RAKAIA_STORE="durable"), pytest.raises(StreamClosed):
        create_stream_event(
            ["a", "b"], lambda _obj: _Payload(), line, "update", using="overlay"
        )

    assert not Stream.objects.using("overlay").filter(stream_id="b").exists()
    assert not StreamEntry.objects.using("overlay").exists()


@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
def test_a_model_save_event_lands_on_the_saves_database():
    line = FinanceLine(submission_id="a", suku="s", delta=1)
    with override_settings(RAKAIA_STORE="durable"):
        create_stream_event(
            "here", lambda _obj: _Payload(), line, "update", using="overlay"
        )

    assert Stream.objects.using("overlay").filter(stream_id="here").exists()
    assert not Stream.objects.using("default").filter(stream_id="here").exists()
