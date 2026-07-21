"""Tests for rakaia.subscription: the per-consumer stream cursor / watermark.

A subscriber tracks the last offset it consumed and polls for the delta since.
`poll` derives its status purely from the stored cursor vs the stream head, so
it is store-agnostic (works over any `ReadableStore` that also exposes
`get_current_offset`) and — like everything else in rakaia — deterministic.
"""

from __future__ import annotations

from rakaia.store import StreamStore
from rakaia.subscription import poll


def _store_with(path: str, payloads: list[bytes]) -> StreamStore:
    store = StreamStore()
    store.create(path)
    for p in payloads:
        store.append(path, p)
    return store


class TestPoll:
    def test_fresh_subscriber_gets_everything(self):
        store = _store_with("s", [b"a", b"b", b"c"])
        result = poll(store, "s", cursor=None)
        assert result.status == "fresh"
        assert [m.data for m in result.messages] == [b"a", b"b", b"c"]
        # Cursor advances to the tail so the next poll resumes after it.
        assert result.cursor == store.get_current_offset("s")

    def test_incremental_poll_returns_only_the_delta(self):
        store = _store_with("s", [b"a", b"b"])
        first = poll(store, "s", cursor=None)
        store.append("s", b"c")
        store.append("s", b"d")
        second = poll(store, "s", cursor=first.cursor)
        assert second.status == "advanced"
        assert [m.data for m in second.messages] == [b"c", b"d"]
        assert second.cursor == store.get_current_offset("s")

    def test_caught_up_returns_no_messages(self):
        store = _store_with("s", [b"a"])
        first = poll(store, "s", cursor=None)
        again = poll(store, "s", cursor=first.cursor)
        assert again.status == "caught_up"
        assert again.caught_up
        assert again.messages == []
        assert again.cursor == first.cursor  # unchanged

    def test_no_gaps_or_dupes_across_repeated_polls(self):
        store = _store_with("s", [])
        seen: list[bytes] = []
        cursor = None
        for i in range(5):
            store.append("s", f"e{i}".encode())
            result = poll(store, "s", cursor=cursor)
            seen.extend(m.data for m in result.messages)
            cursor = result.cursor
        assert seen == [b"e0", b"e1", b"e2", b"e3", b"e4"]

    def test_empty_stream_is_fresh_then_caught_up(self):
        store = StreamStore()
        store.create("s")
        first = poll(store, "s", cursor=None)
        assert first.status == "fresh"
        assert first.messages == []
        assert first.cursor == store.get_current_offset("s")
        assert poll(store, "s", cursor=first.cursor).status == "caught_up"

    def test_rewound_when_cursor_is_beyond_head(self):
        # `rewound` is the defensive path: a cursor that sorts past the current
        # head — a genuinely truncated log, or a cursor carried over from a
        # different/longer stream. (Globally-monotonic offsets, #34, mean a
        # normal delete+recreate no longer triggers this — the recreated head
        # sorts *after* the old cursor and reads as `advanced` instead.)
        store = _store_with("s", [b"a", b"b"])
        beyond = "9999999999999999_9999999999999999"  # past any real offset
        result = poll(store, "s", cursor=beyond)
        assert result.status == "rewound"
        assert result.rewound
        assert [m.data for m in result.messages] == [b"a", b"b"]  # re-read from start
        assert result.cursor == store.get_current_offset("s")

    def test_absent_stream_reports_absent(self):
        store = StreamStore()
        result = poll(store, "missing", cursor="0000000000000000_0000000000000009")
        assert result.status == "absent"
        assert result.messages == []
        assert result.cursor is None

    def test_recreate_reusing_offset_is_not_silently_skipped(self):
        # Adversarial-review finding (was a strict-xfail): a stream deleted and
        # recreated so its byte offset lands on the exact position the consumer
        # last committed used to read as `caught_up`, silently dropping the new
        # content. Globally-monotonic store offsets (#34) bump the read_seq
        # generation on recreate, so the recreated head sorts past the stale
        # cursor and the content is delivered as a normal `advanced`.
        store = _store_with("s", [b"aa"])  # gen 0, byte offset "..._...2"
        cursor = poll(store, "s", cursor=None).cursor
        store.delete("s")
        store.create("s")  # gen 1: head "..._1_..._0" > any gen-0 offset
        store.append("s", b"XX")  # same byte length, but a strictly greater offset
        result = poll(store, "s", cursor=cursor)
        assert result.status == "advanced"
        assert [m.data for m in result.messages] == [b"XX"]  # delivered, not skipped
