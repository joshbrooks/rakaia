"""Shared conformance contract for the framework store surface (`WritableStore`).

Both the in-memory `StreamStore` and the durable `DjangoStreamStore` must satisfy
the create/append/read/has surface that `replay()`, producers, and the
meta-stream registry rely on — so "test on the in-memory store, ship on the
durable store" is safe. Subclass `StoreContract` in each backend's test package
and provide a `store` fixture returning a fresh, empty store. See ADR 0002 / #36.

This module is intentionally not named `test_*`, so pytest does not collect it
directly; only the backend subclasses run it.

Out of contract scope (backend-specific, deliberately not asserted here):
`create()` conflict detection (the in-memory `StreamStore` raises `ValueError`
on re-create with a different content_type/ttl; `DjangoStreamStore` has no such
concept — its `Stream` model has no content_type/ttl/closed columns at all —
and permanently ignores extra kwargs instead of raising), stream close /
Stream-Seq / producer fencing (same story: no `closed` concept on the durable
side), and the exact offset *format* (compound `{seq}_{byte}` vs zero-padded
int). These are genuine, permanent architectural divergences, not gaps to
close — asserting one behaviour for both here would contradict the point of a
shared contract, and a producer that depends on in-memory conflict-rejection
or closed-stream signalling is relying on protocol-server behaviour ADR 0002
explicitly excludes from `WritableStore` (see `src/rakaia/protocols.py`).
Each divergence is pinned in that backend's own suite instead:
- in-memory: `tests/test_rakaia/test_store.py::
  test_create_conflicting_content_type_raises`,
  `test_create_conflicting_ttl_raises`, `test_seq_conflict_raises`,
  `test_append_to_closed_stream_returns_stream_closed`.
- Django: `tests/test_django_rakaia/test_django_store.py::
  test_create_ignores_conflicting_kwargs_instead_of_raising`,
  `test_append_has_no_closed_stream_concept`.
Cross-backend offset/format unification is tracked in #39.
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

    def test_get_current_offset_none_then_advances(self, store):
        assert store.get_current_offset("s") is None
        store.create("s")
        before = store.get_current_offset("s")
        store.append("s", b'{"id": 1}')
        after = store.get_current_offset("s")
        assert after is not None and after != before
