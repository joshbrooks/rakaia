"""Tests for the durable, DB-backed DjangoStreamStore."""

from __future__ import annotations

import json
from unittest.mock import patch

import pytest
from django.db.models.query import QuerySet

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import Stream, StreamEntry
from rakaia import CollectingExecutor
from rakaia.effects import Effect
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay
from rakaia.store import StreamStore


@pytest.mark.django_db
class TestDjangoStreamStore:
    # The shared read/append/create/has surface (create-idempotence, append-
    # requires-stream, ordered round-trip, partial read, envelope round-trip,
    # current-offset) is covered once for every backend by
    # tests/store_contract.py::StoreContract (see test_store_contract.py). Only
    # DjangoStreamStore-specific behaviour lives here: ORM persistence, integer
    # offset format, row locking, durability across instances, delete/list_paths.
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

    def test_append_locks_stream_row_for_offset_allocation(self):
        # Regression guard for the concurrency bug: offset allocation must go
        # through select_for_update() so concurrent appends serialize on the
        # stream row instead of racing to the same offset and failing the
        # unique_together(stream, offset) constraint. SQLite (the CI backend)
        # can't reproduce the race across connections, so assert the lock is
        # acquired rather than trying to trigger the collision.
        store = DjangoStreamStore()
        store.create("s")

        original = QuerySet.select_for_update
        locked = []

        def spy(qs, *args, **kwargs):
            locked.append(True)
            return original(qs, *args, **kwargs)

        with patch.object(QuerySet, "select_for_update", spy):
            store.append("s", b'{"id": 1}')

        assert locked, "append must lock the stream row via select_for_update()"

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
        # Offsets are zero-padded for lexicographic sortability (#34).
        assert store.get_current_offset("s") == "00000000000000000002"

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
