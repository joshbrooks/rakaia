"""Tests for stream-backed persistence of the HandlerRegistry."""

from __future__ import annotations

import json
from dataclasses import FrozenInstanceError

import pytest

from rakaia.registry import (
    HANDLERS_META_STREAM,
    REDUCERS_META_STREAM,
    HandlerRegistry,
    HandlerVersion,
)
from rakaia.store import StreamStore


def _fn(tag: str):
    def handler(event):  # noqa: ARG001
        return tag

    handler.__qualname__ = f"_fn.<{tag}>"
    return handler


@pytest.fixture
def store() -> StreamStore:
    return StreamStore()


class TestPersistence:
    def test_first_registration_creates_meta_stream(self, store: StreamStore):
        HandlerRegistry(store=store)
        assert store.has(HANDLERS_META_STREAM)

    def test_register_appends_event(self, store: StreamStore):
        reg = HandlerRegistry(store=store)
        reg.register("mogrify", "room:*", _fn("v1"), 0, 10)

        messages, _ = store.read(HANDLERS_META_STREAM)
        assert len(messages) == 1
        payload = json.loads(messages[0].data)
        assert payload["name"] == "mogrify"
        assert payload["event_match"] == "room:*"
        assert payload["effective_from"] == 0
        assert payload["effective_to"] == 10
        assert payload["source_hash"]
        assert payload["dotted_path"]

    def test_register_open_ended_persists_null(self, store: StreamStore):
        reg = HandlerRegistry(store=store)
        reg.register("m", "e", _fn("v1"), 10, None)
        messages, _ = store.read(HANDLERS_META_STREAM)
        payload = json.loads(messages[0].data)
        assert payload["effective_to"] is None

    def test_multiple_registrations_append(self, store: StreamStore):
        reg = HandlerRegistry(store=store)
        reg.register("mogrify", "room:*", _fn("v1"), 0, 10)
        reg.register("mogrify", "room:*", _fn("v2"), 10, 20)
        reg.register("zap", "chat:*", _fn("z1"), 0, None)
        messages, _ = store.read(HANDLERS_META_STREAM)
        assert len(messages) == 3

    def test_no_store_no_persistence(self):
        reg = HandlerRegistry()
        reg.register("m", "e", _fn("v"), 0, 10)
        # No store - no exception, no stream created elsewhere
        assert reg.all_versions()

    def test_match_field_persisted_and_rehydrated(self, store: StreamStore):
        reg = HandlerRegistry(store=store)
        reg.register("m", "tf_*", _fn("v1"), 0, None, match_field="form_type")

        messages, _ = store.read(HANDLERS_META_STREAM)
        payload = json.loads(messages[0].data)
        assert payload["match_field"] == "form_type"

        # A fresh registry over the same store dedups the identical
        # registration (match_field included in the identity) — no re-append.
        reg2 = HandlerRegistry(store=store)
        reg2.register("m", "tf_*", _fn("v1"), 0, None, match_field="form_type")
        messages2, _ = store.read(HANDLERS_META_STREAM)
        assert len(messages2) == 1

    def test_missing_match_field_defaults_to_none(self, store: StreamStore):
        reg = HandlerRegistry(store=store)
        reg.register("m", "e", _fn("v1"), 0, None)
        messages, _ = store.read(HANDLERS_META_STREAM)
        payload = json.loads(messages[0].data)
        assert payload["match_field"] is None

    def test_stage_persisted_and_dedups(self, store: StreamStore):
        reg = HandlerRegistry(store=store)
        reg.register("m", "e", _fn("v1"), 0, None, stage=2)
        messages, _ = store.read(HANDLERS_META_STREAM)
        assert json.loads(messages[0].data)["stage"] == 2

        # A fresh registry over the same store dedups the identical
        # registration (stage included in the identity) — no re-append.
        reg2 = HandlerRegistry(store=store)
        reg2.register("m", "e", _fn("v1"), 0, None, stage=2)
        messages2, _ = store.read(HANDLERS_META_STREAM)
        assert len(messages2) == 1

    def test_stage_change_appends_new_event(self, store: StreamStore):
        # Moving a handler to a different stage is a real change -> new event.
        reg = HandlerRegistry(store=store)
        fn = _fn("v1")
        reg.register("m", "e", fn, 0, 10, stage=0)
        reg.register("m", "e", fn, 10, 20, stage=1)  # different range + stage
        messages, _ = store.read(HANDLERS_META_STREAM)
        assert len(messages) == 2
        assert {json.loads(m.data)["stage"] for m in messages} == {0, 1}

    def test_missing_stage_defaults_to_zero(self, store: StreamStore):
        # A payload persisted before `stage` existed rehydrates as stage 0.
        store.create(HANDLERS_META_STREAM)
        payload = {
            "name": "ghost",
            "event_match": "x",
            "effective_from": 0,
            "effective_to": 5,
            "dotted_path": "some.module.ghost",
            "source_hash": "deadbeef" * 8,
            # no "stage" key
        }
        store.append(HANDLERS_META_STREAM, json.dumps(payload).encode("utf-8"))
        reg = HandlerRegistry(store=store)
        ident = next(iter(reg._persisted_ids))  # type: ignore[attr-defined]
        assert ident[-1] == 0  # stage defaulted to 0


class TestIdempotency:
    def test_same_registration_twice_in_one_process_is_noop(self, store: StreamStore):
        reg = HandlerRegistry(store=store)
        fn = _fn("v1")
        v1 = reg.register("m", "e", fn, 0, 10)
        v2 = reg.register("m", "e", fn, 0, 10)
        assert v1 is v2
        messages, _ = store.read(HANDLERS_META_STREAM)
        assert len(messages) == 1
        assert len(reg.all_versions()) == 1

    def test_registration_persists_across_registry_instances(self, store: StreamStore):
        """Simulate a process restart by creating a fresh registry against
        the same store; the same handler re-registering must not append a
        duplicate event."""
        reg1 = HandlerRegistry(store=store)
        fn = _fn("v1")
        reg1.register("m", "e", fn, 0, 10)
        messages_before, _ = store.read(HANDLERS_META_STREAM)
        assert len(messages_before) == 1

        # Simulate restart
        reg2 = HandlerRegistry(store=store)
        reg2.register("m", "e", fn, 0, 10)

        messages_after, _ = store.read(HANDLERS_META_STREAM)
        assert len(messages_after) == 1
        # And the new registry should know about the handler
        assert len(reg2.all_versions()) == 1
        assert reg2.all_versions()[0].name == "m"

    def test_source_change_appends_new_event(self, store: StreamStore):
        """If a handler's source body changes, that's drift; we record a new
        registration event so the audit log captures it."""
        reg = HandlerRegistry(store=store)
        fn_a = _fn("a")
        reg.register("m", "e", fn_a, 0, 10)

        # Different function (different source) — different identity
        def fn_b(event):  # noqa: ARG001
            return "b"

        fn_b.__qualname__ = "_fn.<b-other-source>"
        # Use a different range to avoid the overlap rejection — we're
        # testing the persistence side, not in-memory dedup.
        reg.register("m", "e", fn_b, 10, 20)

        messages, _ = store.read(HANDLERS_META_STREAM)
        assert len(messages) == 2


class TestReducerPersistence:
    def test_reducer_appended_to_reducer_stream(self, store: StreamStore):
        reg = HandlerRegistry(store=store)
        reg.register_reducer("balance", 1, _fn("b"))
        messages, _ = store.read(REDUCERS_META_STREAM)
        assert len(messages) == 1
        payload = json.loads(messages[0].data)
        assert payload["name"] == "balance"
        assert payload["stage"] == 1
        assert payload["dotted_path"]
        assert payload["source_hash"]

    def test_reducer_dedups_across_instances(self, store: StreamStore):
        reg1 = HandlerRegistry(store=store)
        fn = _fn("b")
        reg1.register_reducer("balance", 1, fn)
        assert len(store.read(REDUCERS_META_STREAM)[0]) == 1

        # Simulated restart: same reducer re-registers without a duplicate.
        reg2 = HandlerRegistry(store=store)
        reg2.register_reducer("balance", 1, fn)
        assert len(store.read(REDUCERS_META_STREAM)[0]) == 1
        assert reg2.all_reducers()[0].name == "balance"

    def test_reducer_and_handler_streams_are_separate(self, store: StreamStore):
        reg = HandlerRegistry(store=store)
        reg.register("h", "e", _fn("h"), 0, None)
        reg.register_reducer("agg", 1, _fn("a"))
        assert len(store.read(HANDLERS_META_STREAM)[0]) == 1
        assert len(store.read(REDUCERS_META_STREAM)[0]) == 1


class TestRehydrate:
    def test_rehydrate_on_empty_store_is_noop(self, store: StreamStore):
        reg = HandlerRegistry(store=store)
        reg.rehydrate()
        assert reg.all_versions() == []

    def test_rehydrate_without_store_is_noop(self):
        reg = HandlerRegistry()
        reg.rehydrate()
        assert reg.all_versions() == []

    def test_loaded_persisted_ids_populates_on_init(self, store: StreamStore):
        # Pre-populate the stream manually (no content_type — meta-stream
        # stores one JSON blob per message, not a flattened JSON array).
        store.create(HANDLERS_META_STREAM)
        payload = {
            "name": "ghost",
            "event_match": "x",
            "effective_from": 0,
            "effective_to": 5,
            "dotted_path": "some.module.ghost",
            "source_hash": "deadbeef" * 8,
        }
        store.append(
            HANDLERS_META_STREAM,
            json.dumps(payload).encode("utf-8"),
        )

        # New registry should see the ID as already persisted
        reg = HandlerRegistry(store=store)
        # Sanity: in-memory is empty (module wasn't imported)
        assert reg.all_versions() == []
        # But re-registering ghost (if its source matched) would be a no-op append.
        # We assert that by checking the internal set was loaded.
        assert any(
            ident[0] == "ghost"
            for ident in reg._persisted_ids  # type: ignore[attr-defined]
        )


def test_handler_version_immutable_identity():
    # Sanity: HandlerVersion is frozen, dotted_path + source_hash are part of
    # the identity tuple used for stream dedup.
    fn = _fn("x")
    v = HandlerVersion(
        name="m",
        event_match="e",
        effective_from=0,
        effective_to=None,
        fn=fn,
        dotted_path="x.y.z",
        source_hash="h",
    )
    with pytest.raises(FrozenInstanceError):
        v.name = "other"  # type: ignore[misc]
