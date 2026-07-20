"""Tests for rakaia.append: no-op-suppressed append."""

from __future__ import annotations

import json

from rakaia.append import append_if_changed, snapshots_equal
from rakaia.store import StreamStore


class TestSnapshotsEqual:
    def test_dict_order_insensitive(self):
        assert snapshots_equal({"a": 1, "b": 2}, {"b": 2, "a": 1})

    def test_list_order_sensitive(self):
        assert not snapshots_equal([1, 2], [2, 1])

    def test_nested_deep_equal(self):
        assert snapshots_equal({"x": {"y": [1, 2]}}, {"x": {"y": [1, 2]}})
        assert not snapshots_equal({"x": {"y": [1, 2]}}, {"x": {"y": [1, 3]}})


def _count(store: StreamStore, path: str) -> int:
    messages, _ = store.read(path)
    return len(messages)


class TestAppendIfChanged:
    def _store(self):
        store = StreamStore()
        store.create("s", content_type="application/octet-stream")
        return store

    def test_new_subject_always_appends(self):
        store = self._store()
        appended = append_if_changed(store, "s", b'{"a": 1}', current=None)
        assert appended is True
        assert _count(store, "s") == 1

    def test_unchanged_payload_is_suppressed(self):
        store = self._store()
        appended = append_if_changed(
            store, "s", b'{"a": 1, "b": 2}', current={"a": 1, "b": 2}
        )
        assert appended is False
        assert _count(store, "s") == 0  # nothing written

    def test_changed_payload_appends(self):
        store = self._store()
        appended = append_if_changed(
            store, "s", b'{"a": 1, "b": 9}', current={"a": 1, "b": 2}
        )
        assert appended is True
        assert _count(store, "s") == 1

    def test_snapshot_of_ignores_volatile_fields(self):
        # The payload's `ts` always changes, but only `fields` is compared, so an
        # otherwise-identical save is still suppressed.
        store = self._store()
        current = {"x": 1}
        appended = append_if_changed(
            store,
            "s",
            b'{"fields": {"x": 1}, "ts": "later"}',
            current=current,
            snapshot_of=lambda ev: ev["fields"],
        )
        assert appended is False
        assert _count(store, "s") == 0

    def test_forwards_options_when_appending(self):
        from rakaia.types import AppendOptions

        store = self._store()
        append_if_changed(
            store,
            "s",
            b'{"a": 2}',
            current={"a": 1},
            options=AppendOptions(label="update", metadata={"user": 5}),
        )
        messages, _ = store.read("s")
        assert messages[-1].label == "update"
        assert messages[-1].metadata == {"user": 5}
        assert json.loads(messages[-1].data) == {"a": 2}
