"""Durable consumer-cursor tests over the Django store.

Exercises `django_rakaia.subscription` (load/commit/poll_consumer) end-to-end
against `DjangoStreamStore`, including resume-across-restart and rewind detection
with the Django store's non-zero-padded integer offsets.
"""

from __future__ import annotations

import json

import pytest

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.subscription import commit_cursor, load_cursor, poll_consumer

CONSUMER = "reporting"


def _append(store: DjangoStreamStore, path: str, n: int) -> None:
    for i in range(n):
        store.append(path, json.dumps({"i": i}).encode())


@pytest.mark.django_db
class TestConsumerCursor:
    def test_commit_then_load_round_trips(self):
        assert load_cursor(CONSUMER, "s") is None
        commit_cursor(CONSUMER, "s", "7")
        assert load_cursor(CONSUMER, "s") == "7"
        commit_cursor(CONSUMER, "s", "9")  # upsert, not duplicate
        assert load_cursor(CONSUMER, "s") == "9"

    def test_fresh_then_caught_up(self):
        store = DjangoStreamStore()
        store.create("s")
        _append(store, "s", 3)

        first = poll_consumer(store, CONSUMER, "s")
        assert first.status == "fresh"
        assert len(first.messages) == 3
        commit_cursor(CONSUMER, "s", first.cursor)

        again = poll_consumer(store, CONSUMER, "s")
        assert again.caught_up
        assert again.messages == []

    def test_incremental_resume_across_restart(self):
        store = DjangoStreamStore()
        store.create("s")
        _append(store, "s", 2)
        first = poll_consumer(store, CONSUMER, "s")
        commit_cursor(CONSUMER, "s", first.cursor)

        _append(store, "s", 2)  # two more events arrive
        # A "restart": a brand-new store handle, cursor read from the DB.
        second = poll_consumer(DjangoStreamStore(), CONSUMER, "s")
        assert second.status == "advanced"
        assert len(second.messages) == 2  # only the delta, no re-delivery

    def test_offsets_are_lexicographically_sortable(self):
        # Protocol conformance (#34): offsets MUST sort byte-wise
        # lexicographically. Unpadded, "10" sorts before "2"; zero-padding fixes
        # it so lexicographic order matches chronological across the 1->12 digit
        # boundary — exactly where the bare-int rendering broke.
        store = DjangoStreamStore()
        store.create("s")
        _append(store, "s", 12)
        offsets = [m.offset for m in store.read("s")[0]]
        assert offsets == sorted(offsets)
        assert len(offsets) == 12

    def test_rewind_after_recreate_shorter(self):
        # Consume 10 events, then rebuild the stream with only 2. The Django
        # store still resets offsets on recreate (its globally-monotonic fix,
        # defect #2, is deferred in #34), so the stale cursor sorts past the new
        # head and the poll correctly reports a rewind.
        store = DjangoStreamStore()
        store.create("s")
        _append(store, "s", 10)
        first = poll_consumer(store, CONSUMER, "s")
        assert int(first.cursor) == 10  # zero-padded now, but numerically 10
        commit_cursor(CONSUMER, "s", first.cursor)

        store.delete("s")
        store.create("s")
        _append(store, "s", 2)

        result = poll_consumer(store, CONSUMER, "s")
        assert result.status == "rewound"
        assert len(result.messages) == 2  # re-read from the start
