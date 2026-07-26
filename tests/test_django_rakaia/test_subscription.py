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

    def test_recreate_delivers_new_content_via_monotonic_offsets(self):
        # #34 Defect #2 fixed: consume 10 events, then rebuild the stream with 2.
        # Durable offsets are now globally monotonic across delete+recreate, so
        # the recreated content is issued offsets strictly greater than the
        # retired ones. The stale cursor therefore sorts BEFORE the new head, and
        # the poll delivers the new events as an ordinary `advanced` — no
        # `caught_up` silent skip, no rewind. (`rewound` remains reachable only
        # for a genuinely truncated log or a cursor from a different stream.)
        store = DjangoStreamStore()
        store.create("s")
        _append(store, "s", 10)
        first = poll_consumer(store, CONSUMER, "s")
        assert int(first.cursor) == 10  # zero-padded, numerically 10
        commit_cursor(CONSUMER, "s", first.cursor)

        store.delete("s")
        store.create("s")
        _append(store, "s", 2)

        result = poll_consumer(store, CONSUMER, "s")
        assert result.status == "advanced"
        assert len(result.messages) == 2  # the new content, delivered not skipped
        # The recreated stream's offsets are strictly greater than the old head.
        assert result.cursor > first.cursor

    def test_empty_recreate_does_not_spuriously_rewind(self):
        # #34 Defect #2, read side: in the window after a recreate but BEFORE the
        # first append, the stream looks empty but its watermark survives. The
        # head must reflect that high-water so a stale cursor reads as `caught_up`
        # (nothing new yet), not `rewound` — which would tell the consumer to
        # wipe its derived state for a stream that will resume above the cursor.
        store = DjangoStreamStore()
        store.create("s")
        _append(store, "s", 10)
        first = poll_consumer(store, CONSUMER, "s")
        commit_cursor(CONSUMER, "s", first.cursor)

        store.delete("s")
        store.create("s")  # recreated, no append yet

        result = poll_consumer(store, CONSUMER, "s")
        assert result.status == "caught_up"
        assert result.messages == []
