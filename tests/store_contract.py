"""Shared conformance contract for the framework store surface (`WritableStore`).

Both the in-memory `StreamStore` and the durable `DjangoStreamStore` must satisfy
the create/append/read/has surface that `replay()`, producers, and the
meta-stream registry rely on — so "test on the in-memory store, ship on the
durable store" is safe. Subclass `StoreContract` in each backend's test package
and provide a `store` fixture returning a fresh, empty store. See ADR 0002 / #36.

This module is intentionally not named `test_*`, so pytest does not collect it
directly; only the backend subclasses run it.

Scope note. This file used to declare `create()` conflict detection, stream
close, Stream-Seq and producer fencing "genuine, permanent architectural
divergences, not gaps to close", on the grounds that the durable `Stream` model
had no content_type/ttl/closed columns and never would. That is no longer true:
the durable store now backs a protocol server, so it models all of them, and
both stores are held to them by `tests/server_store_contract.py`. They are out
of scope *here* only because they belong to the protocol-server surface
(`StreamServerStore`), not the framework one (`WritableStore`) — a different
contract, not an absent one.

What remains genuinely backend-specific is the offset *format* (compound
`{seq}_{byte}` vs zero-padded int) — and, because of it, each store rejects the
other's offsets with `InvalidOffset` rather than resolving them to some
position of its own.

The offset **format** stays backend-specific (the compound `{seq}_{byte}` vs
zero-padded int divergence above) — the protocol mandates opacity, not one
format (§6). What *is* now contract, asserted here for both stores, is the
offset **behaviour**: opaque, lexicographically sortable, strictly increasing
per append, and usable as a resume token. See #39.
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

    def test_satisfies_store_protocols(self, store):
        from rakaia import CursorStore, ReadableStore, WritableStore

        assert isinstance(store, WritableStore)
        assert isinstance(store, ReadableStore)
        assert isinstance(store, CursorStore)  # get_current_offset (subscribers)

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

    def test_event_ts_defaults_to_append_time_when_unset(self, store):
        # With no producer-set logical ts, the envelope ts is populated with the
        # append time — never None after a store append.
        store.create("s")
        store.append("s", b'{"id": 1}')
        messages, _ = store.read("s")
        assert isinstance(messages[0].event_ts, float)
        assert messages[0].event_ts == pytest.approx(messages[0].timestamp)

    def test_event_ts_roundtrips_producer_logical_time(self, store):
        # A producer-set logical ts (e.g. a backfilled historical time) is
        # preserved verbatim and stays distinct from transport time.
        store.create("s")
        logical = 1_600_000_000.5  # well in the past vs append time
        store.append("s", b'{"id": 1}', AppendOptions(event_ts=logical))
        messages, _ = store.read("s")
        assert messages[-1].event_ts == logical
        assert messages[-1].timestamp != logical

    def test_offsets_strictly_increase_lexicographically(self, store):
        # The offset contract (protocol §6): successive appends yield offsets that
        # are strictly increasing under plain string (lexicographic) comparison —
        # on both stores, without parsing the offset. This is what lets a consumer
        # order/resume portably.
        store.create("s")
        for ev in [{"id": 1}, {"id": 2}, {"id": 3}]:
            store.append("s", json.dumps(ev).encode("utf-8"))
        offsets = [m.offset for m in store.read("s")[0]]
        assert offsets == sorted(offsets)
        assert len(set(offsets)) == len(offsets)  # no duplicates
        assert all(a < b for a, b in zip(offsets, offsets[1:], strict=False))

    def test_offset_is_an_opaque_resume_token(self, store):
        # An offset round-trips as a resume token without any numeric parsing:
        # reading from message[k].offset returns exactly the messages after it.
        store.create("s")
        for ev in [{"id": 1}, {"id": 2}, {"id": 3}, {"id": 4}]:
            store.append("s", json.dumps(ev).encode("utf-8"))
        messages, _ = store.read("s")
        rest, _ = store.read("s", offset=messages[1].offset)
        assert [json.loads(m.data) for m in rest] == [{"id": 3}, {"id": 4}]

    def test_get_current_offset_none_then_advances(self, store):
        assert store.get_current_offset("s") is None
        store.create("s")
        before = store.get_current_offset("s")
        store.append("s", b'{"id": 1}')
        after = store.get_current_offset("s")
        assert after is not None and after != before

    def test_append_many_matches_append_loop(self, store):
        # append_many([...]) produces the same read-back stream state as calling
        # append once per item — same data, order, envelope (label/metadata/
        # event_ts), and strictly-increasing offsets — on every backend. Result
        # object types differ per backend (AppendResult vs StreamEntry), so the
        # contract asserts read-back state, not the return values.
        batch = [
            (b'{"id": 1}', None),
            (b'{"id": 2}', AppendOptions(label="update", metadata={"user": 7})),
            (b'{"id": 3}', AppendOptions(event_ts=1_600_000_000.5)),
        ]

        store.create("loop")
        for data, options in batch:
            store.append("loop", data, options)

        store.create("bulk")
        store.append_many("bulk", batch)

        loop_msgs, _ = store.read("loop")
        bulk_msgs, _ = store.read("bulk")
        assert len(bulk_msgs) == len(loop_msgs) == 3
        for loop_m, bulk_m in zip(loop_msgs, bulk_msgs, strict=True):
            assert json.loads(bulk_m.data) == json.loads(loop_m.data)
            assert bulk_m.label == loop_m.label
            assert bulk_m.metadata == loop_m.metadata
        # A producer-set event_ts round-trips verbatim; an unset one defaults to
        # each store's own append time (so it differs between the two runs by
        # design) — assert it's populated as a float rather than equal.
        assert bulk_msgs[2].event_ts == 1_600_000_000.5
        assert isinstance(bulk_msgs[0].event_ts, float)
        offsets = [m.offset for m in bulk_msgs]
        assert all(a < b for a, b in zip(offsets, offsets[1:], strict=False))

    def test_append_many_empty_is_noop(self, store):
        store.create("s")
        assert store.append_many("s", []) == []
        messages, _ = store.read("s")
        assert messages == []
