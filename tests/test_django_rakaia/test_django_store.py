"""Tests for the durable, DB-backed DjangoStreamStore."""

from __future__ import annotations

import json

import pytest

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import Stream, StreamEntry
from rakaia import CollectingExecutor
from rakaia.effects import Effect
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay
from rakaia.store import StreamStore


@pytest.mark.django_db
class TestDjangoStreamStore:
    def test_create_persists_stream_row(self):
        DjangoStreamStore().create("submissions")
        assert Stream.objects.filter(stream_id="submissions").exists()

    def test_create_is_idempotent(self):
        store = DjangoStreamStore()
        store.create("s")
        store.create("s")
        assert Stream.objects.filter(stream_id="s").count() == 1

    def test_append_monotonic_offset(self):
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}')
        store.append("s", b'{"id": 2}')
        offsets = list(
            StreamEntry.objects.filter(stream__stream_id="s")
            .order_by("offset")
            .values_list("offset", flat=True)
        )
        assert offsets == [1, 2]

    def test_append_requires_existing_stream(self):
        with pytest.raises(KeyError):
            DjangoStreamStore().append("missing", b"{}")

    def test_read_returns_events_in_order(self):
        store = DjangoStreamStore()
        store.create("s")
        for ev in [{"id": 1}, {"id": 2}, {"id": 3}]:
            store.append("s", json.dumps(ev).encode("utf-8"))

        messages, up_to_date = store.read("s")

        assert up_to_date is True
        assert [json.loads(m.data) for m in messages] == [
            {"id": 1},
            {"id": 2},
            {"id": 3},
        ]

    def test_read_from_offset_partial(self):
        store = DjangoStreamStore()
        store.create("s")
        for ev in [{"id": 1}, {"id": 2}, {"id": 3}]:
            store.append("s", json.dumps(ev).encode("utf-8"))

        messages, _ = store.read("s")
        rest, _ = store.read("s", offset=messages[0].offset)
        assert [json.loads(m.data) for m in rest] == [{"id": 2}, {"id": 3}]

    def test_read_missing_stream_raises(self):
        with pytest.raises(KeyError):
            DjangoStreamStore().read("nope")

    def test_delete_removes_stream_and_entries(self):
        store = DjangoStreamStore()
        store.create("s")
        store.append("s", b'{"id": 1}')

        assert store.delete("s") is True
        assert not Stream.objects.filter(stream_id="s").exists()
        assert StreamEntry.objects.count() == 0

    def test_delete_returns_false_when_absent(self):
        assert DjangoStreamStore().delete("nope") is False

    def test_has_and_list_paths(self):
        store = DjangoStreamStore()
        assert store.has("s") is False
        store.create("s")
        store.create("t")
        assert store.has("s") is True
        assert set(store.list_paths()) == {"s", "t"}

    def test_get_current_offset(self):
        store = DjangoStreamStore()
        assert store.get_current_offset("s") is None
        store.create("s")
        store.append("s", b'{"id": 1}')
        store.append("s", b'{"id": 2}')
        assert store.get_current_offset("s") == "2"

    def test_durability_across_instances(self):
        # The property the in-memory store lacks: a fresh instance sees data
        # written by an earlier one, because state lives in the database.
        DjangoStreamStore().create("s")
        DjangoStreamStore().append("s", b'{"id": 42}')
        messages, _ = DjangoStreamStore().read("s")
        assert [json.loads(m.data) for m in messages] == [{"id": 42}]


@pytest.mark.django_db
class TestDjangoStreamStoreReplay:
    def _register(self) -> HandlerRegistry:
        reg = HandlerRegistry()

        def h(event):
            return Effect(
                op="update_or_create",
                model_label="x.X",
                lookup={"id": event["id"]},
                defaults={"name": event["name"]},
            )

        reg.register("h", "s", h, 0, None)
        return reg

    def test_replay_over_django_store_matches_memory(self):
        events = [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]
        reg = self._register()

        mem = StreamStore()
        mem.create("s")
        for ev in events:
            mem.append("s", json.dumps(ev).encode("utf-8"))
        mem_ex = CollectingExecutor()
        replay(mem, "s", mem_ex, handler_registry=reg)

        dj = DjangoStreamStore()
        dj.create("s")
        for ev in events:
            dj.append("s", json.dumps(ev).encode("utf-8"))
        dj_ex = CollectingExecutor()
        replay(dj, "s", dj_ex, handler_registry=reg)

        assert [(e.lookup, e.defaults) for e in dj_ex.effects] == [
            (e.lookup, e.defaults) for e in mem_ex.effects
        ]


@pytest.mark.django_db
def test_get_store_returns_durable_when_setting_set(settings):
    from django_rakaia.store import get_store

    settings.RAKAIA_STORE = "durable"
    assert isinstance(get_store(), DjangoStreamStore)
