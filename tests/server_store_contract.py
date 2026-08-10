"""Shared conformance contract for the protocol-server store surface.

`tests/store_contract.py` covers `WritableStore` — what `replay()` and
projections need. This covers `StreamServerStore`: the protocol lifecycle a
Durable Streams server needs from whatever is under it — producer fencing,
close, TTL, long-poll, response formatting.

Both the in-memory `StreamStore` and the durable `DjangoStreamStore` must pass
it. That is the point: one protocol server implementation runs on either, so
the Django integration does not need a protocol implementation of its own.

Not named `test_*`, so pytest collects it only via the backend subclasses.

**This file supersedes the "permanent architectural divergence" note that used
to open `store_contract.py`.** Close, TTL, Stream-Seq and producer fencing were
described there as concerns the durable store would never model. They are
modelled now, and asserted here for both. What remains genuinely
backend-specific is only the offset *format* (compound `{seq}_{byte}` vs
zero-padded int) — the protocol mandates opacity, not one format (§6).
"""

from __future__ import annotations

import pytest

from rakaia.types import (
    AppendOptions,
    ContentTypeMismatch,
    ProducerAccepted,
    ProducerDuplicate,
    ProducerInvalidEpochSeq,
    ProducerSequenceGap,
    ProducerStaleEpoch,
    SequenceConflict,
    StreamConfigConflict,
    StreamNotFound,
)


class ServerStoreContract:
    """Contract every protocol-server store must uphold.

    Subclasses provide::

        @pytest.fixture
        def store(self):
            return MyStore()
    """

    # =========================================================================
    # Shape
    # =========================================================================

    def test_satisfies_the_server_store_protocol(self, store):
        from rakaia import StreamServerStore

        assert isinstance(store, StreamServerStore)

    def test_get_exposes_what_a_server_reads(self, store):
        """A server reads exactly these six off a stream."""
        store.create("s", content_type="text/plain", ttl_seconds=60)
        stream = store.get("s")
        for attr in (
            "current_offset",
            "closed",
            "content_type",
            "ttl_seconds",
            "expires_at",
            "last_seq",
        ):
            assert hasattr(stream, attr), f"stream is missing {attr}"

    def test_get_is_none_for_a_missing_stream(self, store):
        assert store.get("nope") is None

    # =========================================================================
    # Create
    # =========================================================================

    def test_create_is_idempotent_for_matching_config(self, store):
        store.create("s", content_type="application/json")
        store.create("s", content_type="application/json")
        assert store.has("s")

    def test_create_with_different_config_conflicts(self, store):
        store.create("s", content_type="text/plain")
        with pytest.raises(StreamConfigConflict):
            store.create("s", content_type="application/json")

    # =========================================================================
    # Append
    # =========================================================================

    def test_append_returns_the_message(self, store):
        store.create("s")
        result = store.append("s", b'{"id": 1}')
        assert result.message is not None
        assert result.message.offset

    def test_append_to_a_missing_stream_raises(self, store):
        with pytest.raises(StreamNotFound):
            store.append("nope", b'{"id": 1}')

    def test_append_to_a_closed_stream_reports_closed(self, store):
        store.create("s")
        store.close_stream("s")
        result = store.append("s", b'{"id": 1}')
        assert result.stream_closed is True
        assert result.message is None

    def test_content_type_mismatch_raises(self, store):
        store.create("s", content_type="text/plain")
        with pytest.raises(ContentTypeMismatch):
            store.append("s", b'{"id": 1}', AppendOptions(content_type="text/csv"))

    def test_seq_conflict_raises(self, store):
        store.create("s")
        store.append("s", b'{"id": 1}', AppendOptions(seq=5))
        with pytest.raises(SequenceConflict):
            store.append("s", b'{"id": 2}', AppendOptions(seq=5))

    def test_seq_advances_numerically(self, store):
        """10 follows 9. Compared as text it would not."""
        store.create("s")
        store.append("s", b'{"id": 1}', AppendOptions(seq=9))
        result = store.append("s", b'{"id": 2}', AppendOptions(seq=10))
        assert result.message is not None

    # =========================================================================
    # Close
    # =========================================================================

    def test_close_reports_the_final_offset(self, store):
        store.create("s")
        store.append("s", b'{"id": 1}')
        result = store.close_stream("s")
        assert result is not None
        assert result.final_offset
        assert result.already_closed is False

    def test_close_is_idempotent(self, store):
        store.create("s")
        first = store.close_stream("s")
        second = store.close_stream("s")
        assert second.already_closed is True
        assert second.final_offset == first.final_offset

    def test_close_of_a_missing_stream_is_none(self, store):
        assert store.close_stream("nope") is None

    def test_closed_shows_on_the_stream(self, store):
        store.create("s")
        assert store.get("s").closed is False
        store.close_stream("s")
        assert store.get("s").closed is True

    # =========================================================================
    # Producer fencing
    # =========================================================================

    # Async cases need their sync setup routed through the backend's own
    # sync/async bridge: a store on a database cannot be called directly from
    # an async test. The default just calls through, for stores that don't care.
    @staticmethod
    async def _sync(fn, *args, **kwargs):
        return fn(*args, **kwargs)

    @staticmethod
    def _producer(pid: str, epoch: int, seq: int) -> AppendOptions:
        return AppendOptions(producer_id=pid, producer_epoch=epoch, producer_seq=seq)

    async def test_a_new_producer_must_open_at_seq_zero(self, store):
        await self._sync(store.create, "s")
        result = await store.append_with_producer(
            "s", b'{"id": 1}', self._producer("p", 0, 3)
        )
        assert isinstance(result.producer_result, ProducerSequenceGap)
        assert result.message is None

    async def test_a_producer_opening_at_zero_is_accepted(self, store):
        await self._sync(store.create, "s")
        result = await store.append_with_producer(
            "s", b'{"id": 1}', self._producer("p", 0, 0)
        )
        assert isinstance(result.producer_result, ProducerAccepted)
        assert result.message is not None

    async def test_a_replayed_seq_is_a_duplicate_not_a_write(self, store):
        await self._sync(store.create, "s")
        await store.append_with_producer("s", b'{"id": 1}', self._producer("p", 0, 0))
        again = await store.append_with_producer(
            "s", b'{"id": 1}', self._producer("p", 0, 0)
        )
        assert isinstance(again.producer_result, ProducerDuplicate)
        assert again.message is None
        messages, _ = await self._sync(store.read, "s")
        assert len(messages) == 1, "a duplicate must not write a second time"

    async def test_a_stale_epoch_is_refused(self, store):
        await self._sync(store.create, "s")
        await store.append_with_producer("s", b'{"id": 1}', self._producer("p", 5, 0))
        result = await store.append_with_producer(
            "s", b'{"id": 2}', self._producer("p", 4, 0)
        )
        assert isinstance(result.producer_result, ProducerStaleEpoch)

    async def test_a_new_epoch_must_also_open_at_zero(self, store):
        await self._sync(store.create, "s")
        await store.append_with_producer("s", b'{"id": 1}', self._producer("p", 0, 0))
        result = await store.append_with_producer(
            "s", b'{"id": 2}', self._producer("p", 1, 7)
        )
        assert isinstance(result.producer_result, ProducerInvalidEpochSeq)

    async def test_a_gap_is_refused_with_the_expected_seq(self, store):
        await self._sync(store.create, "s")
        await store.append_with_producer("s", b'{"id": 1}', self._producer("p", 0, 0))
        result = await store.append_with_producer(
            "s", b'{"id": 2}', self._producer("p", 0, 5)
        )
        assert isinstance(result.producer_result, ProducerSequenceGap)
        assert result.producer_result.expected_seq == 1

    async def test_a_refused_write_does_not_advance_the_producer(self, store):
        """A rejected append must leave the sequence where it was."""
        await self._sync(store.create, "s")
        await store.append_with_producer("s", b'{"id": 1}', self._producer("p", 0, 0))
        await store.append_with_producer("s", b'{"id": 2}', self._producer("p", 0, 9))
        accepted = await store.append_with_producer(
            "s", b'{"id": 2}', self._producer("p", 0, 1)
        )
        assert isinstance(accepted.producer_result, ProducerAccepted)

    # =========================================================================
    # Long-poll
    # =========================================================================

    async def test_wait_returns_immediately_when_messages_exist(self, store):
        await self._sync(store.create, "s")
        await self._sync(store.append, "s", b'{"id": 1}')
        messages, _, _ = await store.wait_for_messages("s", "-1", 5.0)
        assert len(messages) == 1

    async def test_wait_times_out_empty_when_caught_up(self, store):
        await self._sync(store.create, "s")
        await self._sync(store.append, "s", b'{"id": 1}')
        head = await self._sync(store.get_current_offset, "s")
        messages, timed_out, _ = await store.wait_for_messages("s", head, 0.15)
        assert messages == []
        assert timed_out is True

    async def test_wait_returns_on_a_closed_stream(self, store):
        await self._sync(store.create, "s")
        await self._sync(store.append, "s", b'{"id": 1}')
        head = await self._sync(store.get_current_offset, "s")
        await self._sync(store.close_stream, "s")
        messages, _, closed = await store.wait_for_messages("s", head, 5.0)
        assert messages == []
        assert closed is True

    # =========================================================================
    # TTL
    # =========================================================================

    def test_an_expired_stream_reads_as_absent(self, store):
        store.create("s", ttl_seconds=0)
        # ttl_seconds=0 expires as soon as any time has passed.
        import time

        time.sleep(0.01)
        assert store.get("s") is None
        assert store.has("s") is False

    def test_touch_is_a_no_op_for_a_missing_stream(self, store):
        store.touch("nope")  # must not raise

    # =========================================================================
    # Response formatting
    # =========================================================================

    def test_json_mode_formats_one_array(self, store):
        store.create("s", content_type="application/json")
        store.append("s", b'{"id": 1}')
        store.append("s", b'{"id": 2}')
        messages, _ = store.read("s")
        body = store.format_response("s", messages)
        import json

        assert json.loads(body) == [{"id": 1}, {"id": 2}]
