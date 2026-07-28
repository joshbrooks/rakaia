"""Tests for ``django_rakaia.replay.replay_stream`` — the Django-side replay
convenience that defaults the executor AND the reader, so a staged replay does
not fail with "stage > 0 but no reader" (#68 minor)."""

from __future__ import annotations

import json

import pytest

from django_rakaia.replay import replay_stream
from django_rakaia.store import get_store
from rakaia.effects import Effect
from rakaia.registry import HandlerRegistry, UpcasterRegistry

from .models import Area


def _ref(event):
    return Effect(
        op="update_or_create",
        model_label="test_django_rakaia.Area",
        lookup={"name": event["name"]},
        defaults={},
    )


def _dep(event, reader):
    ref = reader.get("test_django_rakaia.Area", name=event["ref"])
    tag = "FOUND" if ref is not None else "MISSING"
    return Effect(
        op="update_or_create",
        model_label="test_django_rakaia.Area",
        lookup={"name": f"{event['key']}->{tag}"},
        defaults={},
    )


@pytest.mark.django_db
class TestReplayStream:
    def test_staged_replay_defaults_the_reader(self):
        store = get_store()
        store.delete("s")
        store.create("s")
        # Dependent arrives before the reference it needs — only a staged replay
        # with a reader resolves it.
        for event in (
            {"schema_version": 1, "kind": "DEP", "key": "d1", "ref": "the-ref"},
            {"schema_version": 1, "kind": "REF", "key": "r1", "name": "the-ref"},
        ):
            store.append("s", json.dumps(event).encode("utf-8"))

        reg = HandlerRegistry()
        reg.register("ref", "REF", _ref, 0, None, match_field="kind", stage=0)
        reg.register("dep", "DEP", _dep, 0, None, match_field="kind", stage=1)

        # No executor, no reader passed — both default to the Django ones.
        replay_stream(
            "s",
            handler_registry=reg,
            upcaster_registry=UpcasterRegistry(),
        )

        assert Area.objects.filter(name="d1->FOUND").exists()
        assert not Area.objects.filter(name="d1->MISSING").exists()

    def test_returns_replay_result(self):
        store = get_store()
        store.delete("s2")
        store.create("s2")
        store.append(
            "s2",
            json.dumps({"schema_version": 1, "kind": "REF", "name": "solo"}).encode(),
        )

        reg = HandlerRegistry()
        reg.register("ref", "REF", _ref, 0, None, match_field="kind", stage=0)

        result = replay_stream(
            "s2", handler_registry=reg, upcaster_registry=UpcasterRegistry()
        )
        assert result.events_processed == 1
        assert Area.objects.filter(name="solo").exists()
