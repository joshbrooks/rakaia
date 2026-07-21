"""Shared conformance contract for the framework store surface (`WritableStore`).

Both the in-memory `StreamStore` and the durable `DjangoStreamStore` must satisfy
the create/append/read/has surface that `replay()`, producers, and the
meta-stream registry rely on — so "test on the in-memory store, ship on the
durable store" is safe. Subclass `StoreContract` in each backend's test package
and provide a `store` fixture returning a fresh, empty store. See ADR 0002 / #36.

This module is intentionally not named `test_*`, so pytest does not collect it
directly; only the backend subclasses run it.
"""

from __future__ import annotations

import json

import pytest

from rakaia.types import AppendOptions


class StoreContract:
    """Contract every framework store must uphold.

    Subclasses provide::

        @pytest.fixture
        def store(self):
            return MyStore()
    """

    def test_satisfies_writable_store_protocol(self, store):
        from rakaia import ReadableStore, WritableStore

        assert isinstance(store, WritableStore)
        assert isinstance(store, ReadableStore)

    def test_create_is_idempotent(self, store):
        store.create("s")
        store.create("s")
        assert store.has("s") is True

    def test_has_reflects_existence(self, store):
        assert store.has("s") is False
        store.create("s")
        assert store.has("s") is True

    def test_append_requires_existing_stream(self, store):
        # The durable store raises; the contract requires create-before-append.
        with pytest.raises(KeyError):
            store.append("missing", b"{}")

    def test_read_missing_stream_raises(self, store):
        with pytest.raises(KeyError):
            store.read("nope")

    def test_append_then_read_roundtrips_in_order(self, store):
        store.create("s")
        events = [{"id": 1}, {"id": 2}, {"id": 3}]
        for ev in events:
            store.append("s", json.dumps(ev).encode("utf-8"))
        messages, up_to_date = store.read("s")
        assert up_to_date is True
        assert [json.loads(m.data) for m in messages] == events

    def test_partial_read_from_offset(self, store):
        store.create("s")
        for ev in [{"id": 1}, {"id": 2}, {"id": 3}]:
            store.append("s", json.dumps(ev).encode("utf-8"))
        messages, _ = store.read("s")
        rest, _ = store.read("s", offset=messages[0].offset)
        # each store's own offset is portable within that store (ordering semantics)
        assert [json.loads(m.data) for m in rest] == [{"id": 2}, {"id": 3}]

    def test_envelope_label_and_metadata_roundtrip(self, store):
        store.create("s")
        store.append(
            "s",
            b'{"id": 1}',
            AppendOptions(label="update", metadata={"user": 7, "url": "/x"}),
        )
        messages, _ = store.read("s")
        assert messages[-1].label == "update"
        assert messages[-1].metadata == {"user": 7, "url": "/x"}

    def test_raw_append_has_empty_envelope(self, store):
        # A raw append (no options) reads back label="" / metadata=None on both
        # backends, so handlers see a uniform envelope regardless of store.
        store.create("s")
        store.append("s", b'{"id": 1}')
        messages, _ = store.read("s")
        assert messages[-1].label == ""
        assert messages[-1].metadata is None

    def test_message_timestamp_is_float(self, store):
        store.create("s")
        store.append("s", b'{"id": 1}')
        messages, _ = store.read("s")
        assert isinstance(messages[0].timestamp, float)

    def test_get_current_offset_none_then_advances(self, store):
        assert store.get_current_offset("s") is None
        store.create("s")
        before = store.get_current_offset("s")
        store.append("s", b'{"id": 1}')
        after = store.get_current_offset("s")
        assert after is not None and after != before
