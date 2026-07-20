"""Tests for rakaia.subscription: the per-consumer stream cursor / watermark.

A subscriber tracks the last offset it consumed and polls for the delta since.
`poll` derives its status purely from the stored cursor vs the stream head, so
it is store-agnostic (works over any `ReadableStore` that also exposes
`get_current_offset`) and — like everything else in rakaia — deterministic.
"""

from __future__ import annotations

import pytest

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
        # Consume a longer stream, then truncate/rebuild it shorter (delete +
        # recreate). The stored cursor now points past the new head, so the poll
        # must report a rewind and re-read from the start.
        store = _store_with("s", [b"a", b"b", b"c"])
        stale = poll(store, "s", cursor=None).cursor
        store.delete("s")
        store.create("s")
        store.append("s", b"a2")
        result = poll(store, "s", cursor=stale)
        assert result.status == "rewound"
        assert result.rewound
        assert [m.data for m in result.messages] == [b"a2"]  # re-read from start
        assert result.cursor == store.get_current_offset("s")

    def test_absent_stream_reports_absent(self):
        store = StreamStore()
        result = poll(store, "missing", cursor="0000000000000000_0000000000000009")
        assert result.status == "absent"
        assert result.messages == []
        assert result.cursor is None

    @pytest.mark.xfail(
        reason="offset-only rewind detection can't see a delete+recreate that "
        "reuses the committed offset; needs a stream-generation token "
        "(issue #11 / PR #33). Append-only streams are unaffected.",
        strict=True,
    )
    def test_recreate_reusing_offset_should_rewind_not_silently_skip(self):
        # Adversarial-review finding: a stream deleted and recreated so that its
        # new head lands on the exact offset the consumer last committed reads as
        # `caught_up`, silently dropping the new content. This pins the DESIRED
        # behavior (rewound + redeliver); it flips green when the generation
        # token lands, and strict xfail then flags the marker for removal.
        store = _store_with("s", [b"aa"])  # byte offset -> "..._0000000000000002"
        cursor = poll(store, "s", cursor=None).cursor
        store.delete("s")
        store.create("s")
        store.append("s", b"XX")  # same length -> same offset, different content
        result = poll(store, "s", cursor=cursor)
        assert result.status == "rewound"
        assert [m.data for m in result.messages] == [b"XX"]
