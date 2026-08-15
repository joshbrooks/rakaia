"""Tests for rakaia.store.StreamStore."""

from __future__ import annotations

import asyncio
from unittest.mock import patch

import pytest

from rakaia.store import StreamStore
from rakaia.types import (
    INITIAL_OFFSET,
    AppendOptions,
    ProducerAccepted,
    ProducerDuplicate,
    ProducerSequenceGap,
    ProducerStaleEpoch,
)


@pytest.fixture
def store() -> StreamStore:
    return StreamStore()


# =============================================================================
# Stream lifecycle
# =============================================================================


class TestStreamLifecycle:
    def test_create_basic(self, store: StreamStore):
        stream = store.create("foo")
        assert stream.path == "foo"
        assert stream.current_offset == INITIAL_OFFSET
        assert stream.closed is False
        assert store.has("foo")

    def test_create_with_content_type(self, store: StreamStore):
        stream = store.create("foo", content_type="application/json")
        assert stream.content_type == "application/json"

    def test_create_idempotent_same_config(self, store: StreamStore):
        s1 = store.create("foo", content_type="application/json")
        s2 = store.create("foo", content_type="application/json")
        assert s1 is s2

    def test_create_idempotent_with_charset(self, store: StreamStore):
        store.create("foo", content_type="application/json")
        # Same content type with charset should be considered matching
        s2 = store.create("foo", content_type="application/json; charset=utf-8")
        assert s2.path == "foo"

    def test_create_conflicting_content_type_raises(self, store: StreamStore):
        store.create("foo", content_type="application/json")
        with pytest.raises(ValueError, match="different configuration"):
            store.create("foo", content_type="text/plain")

    def test_create_conflicting_ttl_raises(self, store: StreamStore):
        store.create("foo", ttl_seconds=60)
        with pytest.raises(ValueError, match="different configuration"):
            store.create("foo", ttl_seconds=120)

    def test_create_with_initial_data(self, store: StreamStore):
        stream = store.create(
            "foo", content_type="application/octet-stream", initial_data=b"hello"
        )
        assert len(stream.messages) == 1
        assert stream.messages[0].data == b"hello"

    def test_create_closed(self, store: StreamStore):
        stream = store.create("foo", closed=True)
        assert stream.closed is True

    def test_get_returns_stream(self, store: StreamStore):
        store.create("foo")
        assert store.get("foo") is not None

    def test_get_missing_returns_none(self, store: StreamStore):
        assert store.get("missing") is None

    def test_has_existing(self, store: StreamStore):
        store.create("foo")
        assert store.has("foo") is True

    def test_has_missing(self, store: StreamStore):
        assert store.has("missing") is False

    def test_delete_existing(self, store: StreamStore):
        store.create("foo")
        assert store.delete("foo") is True
        assert store.has("foo") is False

    def test_delete_missing(self, store: StreamStore):
        assert store.delete("missing") is False

    def test_clear(self, store: StreamStore):
        store.create("a")
        store.create("b")
        store.create("c")
        store.clear()
        assert store.list_paths() == []

    def test_list_paths(self, store: StreamStore):
        store.create("a")
        store.create("b")
        assert sorted(store.list_paths()) == ["a", "b"]


# =============================================================================
# TTL/expiry
# =============================================================================


class TestExpiry:
    def test_ttl_expired_stream_removed(self, store: StreamStore):
        with patch("rakaia.store.time.time") as mock_time:
            mock_time.return_value = 1000.0
            store.create("foo", ttl_seconds=10)

            mock_time.return_value = 1020.0  # 20s later, ttl was 10s
            assert store.get("foo") is None
            assert store.has("foo") is False

    def test_ttl_not_expired(self, store: StreamStore):
        with patch("rakaia.store.time.time") as mock_time:
            mock_time.return_value = 1000.0
            store.create("foo", ttl_seconds=60)

            mock_time.return_value = 1030.0
            assert store.get("foo") is not None

    def test_expires_at_iso_expired(self, store: StreamStore):
        with patch("rakaia.store.time.time") as mock_time:
            mock_time.return_value = 1000.0
            store.create("foo", expires_at="2024-01-01T00:00:00Z")

            # Set time to well after 2024 (timestamp for 2030-01-01)
            mock_time.return_value = 1893456000.0
            assert store.get("foo") is None

    def test_expires_at_malformed_ignored(self, store: StreamStore):
        store.create("foo", expires_at="not-a-date")
        # Should not raise; malformed expires_at is treated as not expired
        assert store.get("foo") is not None

    def test_ttl_slides_on_read(self, store: StreamStore):
        """A read resets the TTL sliding window."""
        with patch("rakaia.store.time.time") as mock_time:
            mock_time.return_value = 1000.0
            store.create("foo", ttl_seconds=10)

            mock_time.return_value = 1005.0  # read within window -> extends
            store.read("foo")

            mock_time.return_value = 1012.0  # 12s after create, 7s after read
            assert store.get("foo") is not None  # would be expired without sliding

    def test_ttl_slides_on_append(self, store: StreamStore):
        """A write resets the TTL sliding window."""
        with patch("rakaia.store.time.time") as mock_time:
            mock_time.return_value = 1000.0
            store.create("foo", content_type="text/plain", ttl_seconds=10)

            mock_time.return_value = 1005.0
            store.append("foo", b"x", AppendOptions(content_type="text/plain"))

            mock_time.return_value = 1012.0
            assert store.get("foo") is not None

    def test_ttl_does_not_slide_on_get(self, store: StreamStore):
        """A metadata lookup (HEAD path) must not extend the TTL window."""
        with patch("rakaia.store.time.time") as mock_time:
            mock_time.return_value = 1000.0
            store.create("foo", ttl_seconds=10)

            mock_time.return_value = 1005.0
            store.get("foo")  # HEAD-style lookup, no extension

            mock_time.return_value = 1012.0
            assert store.get("foo") is None

    def test_expires_at_does_not_slide_on_read(self, store: StreamStore):
        """Absolute Stream-Expires-At is not affected by activity."""
        with patch("rakaia.store.time.time") as mock_time:
            mock_time.return_value = 1000.0
            store.create("foo", expires_at="2024-01-01T00:00:00Z")

            # A read at a still-valid time must not push the absolute deadline out.
            mock_time.return_value = 1000.0
            store.read("foo")

            mock_time.return_value = 1893456000.0  # 2030
            assert store.get("foo") is None


# =============================================================================
# Append
# =============================================================================


class TestAppend:
    def test_basic_append(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        result = store.append("foo", b"hello")
        assert result.message is not None
        assert result.message.data == b"hello"
        assert result.stream_closed is False

    def test_append_advances_offset(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.append("foo", b"hello")
        stream = store.get("foo")
        assert stream is not None
        assert stream.current_offset != INITIAL_OFFSET

    def test_append_to_missing_raises(self, store: StreamStore):
        with pytest.raises(KeyError, match="not found"):
            store.append("missing", b"data")

    def test_append_json_mode(self, store: StreamStore):
        store.create("foo", content_type="application/json")
        result = store.append("foo", b'{"a": 1}')
        assert result.message is not None

    def test_append_invalid_json_raises(self, store: StreamStore):
        store.create("foo", content_type="application/json")
        with pytest.raises(ValueError, match="Invalid JSON"):
            store.append("foo", b"not-json")

    def test_append_carries_envelope(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        result = store.append(
            "foo",
            b"hello",
            AppendOptions(label="update", metadata={"user": 5, "url": "/x"}),
        )
        assert result.message is not None
        assert result.message.label == "update"
        assert result.message.metadata == {"user": 5, "url": "/x"}
        # and it round-trips through read
        messages, _ = store.read("foo")
        assert messages[-1].label == "update"
        assert messages[-1].metadata == {"user": 5, "url": "/x"}

    def test_append_envelope_defaults_empty(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        result = store.append("foo", b"hello")  # no envelope
        assert result.message is not None
        assert result.message.label == ""
        assert result.message.metadata is None

    def test_append_content_type_mismatch_raises(self, store: StreamStore):
        store.create("foo", content_type="application/json")
        with pytest.raises(ValueError, match="Content-type mismatch"):
            store.append(
                "foo",
                b'{"a":1}',
                AppendOptions(content_type="text/plain"),
            )

    def test_append_to_closed_stream_returns_stream_closed(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.close_stream("foo")
        result = store.append("foo", b"data")
        assert result.stream_closed is True
        assert result.message is None

    def test_append_with_close_flag(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        result = store.append("foo", b"final", AppendOptions(close=True))
        assert result.stream_closed is True
        stream = store.get("foo")
        assert stream is not None
        assert stream.closed is True

    def test_seq_conflict_raises(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.append("foo", b"a", AppendOptions(seq="5"))
        with pytest.raises(ValueError, match="Sequence conflict"):
            store.append("foo", b"b", AppendOptions(seq="5"))

    def test_seq_progression(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.append("foo", b"a", AppendOptions(seq="1"))
        store.append("foo", b"b", AppendOptions(seq="2"))
        stream = store.get("foo")
        assert stream is not None
        assert stream.last_seq == "2"


class TestAppendMany:
    def test_append_many_delegates_to_append(self, store: StreamStore):
        # append_many returns one AppendResult per item in order, and delegates
        # to append so per-item semantics (here: producer seq progression) are
        # preserved exactly as a loop of append would be.
        store.create("foo", content_type="application/octet-stream")
        batch = [
            (b"a", AppendOptions(producer_id="p", producer_epoch=0, producer_seq=0)),
            (b"b", AppendOptions(producer_id="p", producer_epoch=0, producer_seq=1)),
            (b"c", AppendOptions(producer_id="p", producer_epoch=0, producer_seq=2)),
        ]
        results = store.append_many("foo", batch)
        assert len(results) == 3
        assert [r.message.data for r in results] == [b"a", b"b", b"c"]
        assert all(r.producer_result.status == "accepted" for r in results)

    def test_append_many_empty_returns_empty(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        assert store.append_many("foo", []) == []


# =============================================================================
# Producer validation
# =============================================================================


class TestProducerValidation:
    def test_new_producer_accepted_at_seq_zero(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        result = store.append(
            "foo",
            b"data",
            AppendOptions(producer_id="p1", producer_epoch=1, producer_seq=0),
        )
        assert isinstance(result.producer_result, ProducerAccepted)
        assert result.message is not None

    def test_new_producer_with_nonzero_seq_returns_gap(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        result = store.append(
            "foo",
            b"data",
            AppendOptions(producer_id="p1", producer_epoch=1, producer_seq=5),
        )
        assert isinstance(result.producer_result, ProducerSequenceGap)

    def test_duplicate_seq_returns_duplicate(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.append(
            "foo",
            b"a",
            AppendOptions(producer_id="p1", producer_epoch=1, producer_seq=0),
        )
        store.append(
            "foo",
            b"b",
            AppendOptions(producer_id="p1", producer_epoch=1, producer_seq=1),
        )
        # Re-send seq 1
        result = store.append(
            "foo",
            b"b-retry",
            AppendOptions(producer_id="p1", producer_epoch=1, producer_seq=1),
        )
        assert isinstance(result.producer_result, ProducerDuplicate)
        # No message appended
        assert result.message is None

    def test_stale_epoch_rejected(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.append(
            "foo",
            b"a",
            AppendOptions(producer_id="p1", producer_epoch=2, producer_seq=0),
        )
        # Try with older epoch
        result = store.append(
            "foo",
            b"b",
            AppendOptions(producer_id="p1", producer_epoch=1, producer_seq=0),
        )
        assert isinstance(result.producer_result, ProducerStaleEpoch)

    def test_epoch_advance_resets_seq(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.append(
            "foo",
            b"a",
            AppendOptions(producer_id="p1", producer_epoch=1, producer_seq=0),
        )
        # New epoch, seq=0 → accepted
        result = store.append(
            "foo",
            b"b",
            AppendOptions(producer_id="p1", producer_epoch=2, producer_seq=0),
        )
        assert isinstance(result.producer_result, ProducerAccepted)

    def test_sequence_gap_detected(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.append(
            "foo",
            b"a",
            AppendOptions(producer_id="p1", producer_epoch=1, producer_seq=0),
        )
        # Skip seq 1, try seq 5
        result = store.append(
            "foo",
            b"jump",
            AppendOptions(producer_id="p1", producer_epoch=1, producer_seq=5),
        )
        assert isinstance(result.producer_result, ProducerSequenceGap)


# =============================================================================
# Read
# =============================================================================


class TestRead:
    def test_read_empty_stream(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        messages, up_to_date = store.read("foo")
        assert messages == []
        assert up_to_date is True

    def test_read_all(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.append("foo", b"a")
        store.append("foo", b"b")
        messages, _ = store.read("foo")
        assert len(messages) == 2

    def test_read_from_offset_negative_one_returns_all(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.append("foo", b"a")
        messages, _ = store.read("foo", offset="-1")
        assert len(messages) == 1

    def test_read_from_specific_offset(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.append("foo", b"a")
        m2 = store.append("foo", b"b")
        # Read from after the first message
        first = store.get("foo").messages[0]  # type: ignore[union-attr]
        messages, _ = store.read("foo", offset=first.offset)
        assert len(messages) == 1
        assert messages[0].offset == m2.message.offset  # type: ignore[union-attr]

    def test_read_missing_raises(self, store: StreamStore):
        with pytest.raises(KeyError):
            store.read("missing")

    def test_format_response_binary(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.append("foo", b"hello")
        store.append("foo", b"world")
        messages, _ = store.read("foo")
        assert store.format_response("foo", messages) == b"helloworld"

    def test_format_response_json(self, store: StreamStore):
        store.create("foo", content_type="application/json")
        store.append("foo", b'{"a":1}')
        store.append("foo", b'{"b":2}')
        messages, _ = store.read("foo")
        result = store.format_response("foo", messages)
        assert result.startswith(b"[")
        assert result.endswith(b"]")
        assert b'"a"' in result
        assert b'"b"' in result


# =============================================================================
# Long-poll
# =============================================================================


class TestLongPoll:
    async def test_returns_immediately_if_messages(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.append("foo", b"data")
        messages, timed_out, closed = await store.wait_for_messages(
            "foo", offset=INITIAL_OFFSET, timeout_seconds=5.0
        )
        assert len(messages) == 1
        assert timed_out is False
        assert closed is False

    async def test_times_out_when_no_messages(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        stream = store.get("foo")
        assert stream is not None
        messages, timed_out, _ = await store.wait_for_messages(
            "foo", offset=stream.current_offset, timeout_seconds=0.1
        )
        assert messages == []
        assert timed_out is True

    async def test_returns_on_notification(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        stream = store.get("foo")
        assert stream is not None
        start_offset = stream.current_offset

        async def push():
            await asyncio.sleep(0.05)
            store.append("foo", b"new")

        push_task = asyncio.create_task(push())
        messages, timed_out, _ = await store.wait_for_messages(
            "foo", offset=start_offset, timeout_seconds=2.0
        )
        await push_task
        assert len(messages) == 1
        assert timed_out is False

    async def test_closed_at_tail_returns_closed(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.close_stream("foo")
        stream = store.get("foo")
        assert stream is not None
        messages, timed_out, closed = await store.wait_for_messages(
            "foo", offset=stream.current_offset, timeout_seconds=0.1
        )
        assert closed is True

    async def test_missing_stream_raises(self, store: StreamStore):
        with pytest.raises(KeyError):
            await store.wait_for_messages(
                "missing", offset=INITIAL_OFFSET, timeout_seconds=0.1
            )


# =============================================================================
# Close
# =============================================================================


class TestClose:
    def test_close_returns_result(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        result = store.close_stream("foo")
        assert result is not None
        assert result.already_closed is False

    def test_close_already_closed(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        store.close_stream("foo")
        result = store.close_stream("foo")
        assert result is not None
        assert result.already_closed is True

    def test_close_missing_returns_none(self, store: StreamStore):
        assert store.close_stream("missing") is None

    async def test_close_with_producer_accepts_new(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        result = await store.close_stream_with_producer("foo", "p1", 1, 0)
        assert result is not None
        assert isinstance(result.producer_result, ProducerAccepted)

    async def test_close_with_producer_duplicate_returns_duplicate(
        self, store: StreamStore
    ):
        store.create("foo", content_type="application/octet-stream")
        await store.close_stream_with_producer("foo", "p1", 1, 0)
        result = await store.close_stream_with_producer("foo", "p1", 1, 0)
        assert result is not None
        assert result.already_closed is True
        assert isinstance(result.producer_result, ProducerDuplicate)

    async def test_close_with_producer_missing(self, store: StreamStore):
        result = await store.close_stream_with_producer("missing", "p1", 1, 0)
        assert result is None


# =============================================================================
# Concurrent producer
# =============================================================================


class TestConcurrentProducer:
    async def test_append_with_producer_serializes(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")

        # Two parallel appends with same producer
        results = await asyncio.gather(
            store.append_with_producer(
                "foo",
                b"a",
                AppendOptions(producer_id="p1", producer_epoch=1, producer_seq=0),
            ),
            store.append_with_producer(
                "foo",
                b"b",
                AppendOptions(producer_id="p1", producer_epoch=1, producer_seq=1),
            ),
        )
        # Both should succeed (one accepted at seq 0, then seq 1)
        accepted = [
            r for r in results if isinstance(r.producer_result, ProducerAccepted)
        ]
        assert len(accepted) == 2

    async def test_append_with_producer_no_id_falls_through(self, store: StreamStore):
        store.create("foo", content_type="application/octet-stream")
        result = await store.append_with_producer("foo", b"data", AppendOptions())
        assert result.message is not None


class TestGloballyMonotonicOffsets:
    """Recreating a stream at the same path must issue offsets strictly greater
    than any it issued before (#34), so a subscriber's stale cursor can never
    collide with a recreated stream's head. The offset's leading `read_seq`
    component carries the per-path generation; appends keep the byte component
    zero-padded, so offsets stay lexicographically sortable per the protocol."""

    def test_fresh_path_keeps_canonical_initial_offset(self):
        store = StreamStore()
        store.create("s")
        assert store.get_current_offset("s") == INITIAL_OFFSET

    def test_recreate_bumps_generation_above_prior_tail(self):
        store = StreamStore()
        store.create("s")
        store.append("s", b"a")
        old_tail = store.get_current_offset("s")

        store.delete("s")
        store.create("s")
        new_head = store.get_current_offset("s")

        # The recreated stream's head sorts strictly after the deleted stream's
        # tail — lexicographically, as the protocol requires.
        assert new_head > old_tail
        store.append("s", b"b")
        assert store.get_current_offset("s") > old_tail

    def test_generation_climbs_across_repeated_recreates(self):
        store = StreamStore()
        tails = []
        for _ in range(4):
            store.create("s")
            store.append("s", b"x")
            tails.append(store.get_current_offset("s"))
            store.delete("s")
        # Every recreation's tail is strictly greater than the last.
        assert tails == sorted(tails)
        assert len(set(tails)) == 4


class TestReadEdgeCases:
    """Boundary behaviour of read(): empty streams, sentinel offsets, and the
    exclusive-offset contract (read returns messages strictly after `offset`)."""

    def test_read_empty_stream(self, store: StreamStore):
        store.create("s")
        messages, up_to_date = store.read("s")
        assert messages == []
        assert up_to_date is True

    def test_read_empty_stream_with_sentinel_offset(self, store: StreamStore):
        store.create("s")
        messages, up_to_date = store.read("s", "-1")
        assert messages == []
        assert up_to_date is True

    def test_read_missing_stream_raises(self, store: StreamStore):
        with pytest.raises(KeyError):
            store.read("nope")

    def test_read_from_offset_is_exclusive(self, store: StreamStore):
        store.create("s")
        offsets = [
            store.append("s", b"a").message.offset,
            store.append("s", b"b").message.offset,
            store.append("s", b"c").message.offset,
        ]
        # Offsets are strictly increasing and lexicographically sortable.
        assert offsets == sorted(offsets)
        assert len(set(offsets)) == 3

        # No offset (or "-1") returns the whole stream in order.
        allmsgs, up = store.read("s")
        assert [m.offset for m in allmsgs] == offsets
        assert up is True

        # Reading from an offset is exclusive: it returns messages after it.
        after_first, _ = store.read("s", offsets[0])
        assert [m.offset for m in after_first] == offsets[1:]

        # Reading from the tail offset yields nothing — caught up.
        after_last, up = store.read("s", offsets[-1])
        assert after_last == []
        assert up is True

    def test_read_beyond_tail_returns_empty(self, store: StreamStore):
        store.create("s")
        store.append("s", b"a")
        msgs, up = store.read("s", "9999999999999999_9999999999999999")
        assert msgs == []
        assert up is True


class TestClosedStreamAppend:
    """Appending to a closed stream is rejected without mutating the log, even
    when a Stream-Seq is supplied."""

    def test_append_to_closed_stream_with_seq_rejected(self, store: StreamStore):
        store.create("s")
        store.append("s", b"a")
        store.close_stream("s")

        result = store.append("s", b"b", AppendOptions(seq="5"))
        assert result.stream_closed is True
        assert result.message is None

        # The rejected append left the log untouched.
        msgs, _ = store.read("s")
        assert [m.data for m in msgs] == [b"a"]
