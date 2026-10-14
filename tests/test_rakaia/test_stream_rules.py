"""A stream's lifecycle rules, as a table — no store (#322).

The stores used to carry a copy each and the copies disagreed; the contract
suites check that every store asks these, and this file checks the answers.
"""

from __future__ import annotations

import logging

import pytest

from rakaia.stream_rules import (
    StreamConfig,
    check_delete,
    check_expiry_allowed,
    check_protocol_delete,
    decide_close,
    is_expired,
    reported_expiry,
    same_config,
)
from rakaia.types import (
    ClosedBy,
    DeleteNotAllowed,
    ExpiryNotAllowed,
    ProducerAccepted,
    ProducerDuplicate,
    ProducerSequenceGap,
    ProducerState,
    ProducerStreamClosed,
)

NOW = 2_000_000_000.0


class TestExpiry:
    def test_a_stream_with_no_expiry_never_expires(self):
        assert is_expired(None, None, 0.0, now=NOW) is False

    def test_the_ttl_slides_from_the_last_activity(self):
        assert is_expired(10, None, NOW - 9, now=NOW) is False
        assert is_expired(10, None, NOW - 11, now=NOW) is True

    def test_an_absolute_expiry_is_a_deadline(self):
        # NOW is 2033-05-18T03:33:20Z.
        assert is_expired(None, "2033-05-18T03:33:19Z", NOW, now=NOW) is True
        assert is_expired(None, "2033-05-18T03:33:21Z", NOW, now=NOW) is False

    def test_a_naive_expiry_is_read_as_utc_and_an_offset_is_kept(self):
        assert is_expired(None, "2033-05-18T03:33:19", NOW, now=NOW) is True
        assert is_expired(None, "2033-05-18T03:33:21", NOW, now=NOW) is False
        # 05:33:21 at +02:00 is 03:33:21Z, a second ahead; dropping the offset
        # would read it as two hours behind.
        assert is_expired(None, "2033-05-18T05:33:21+02:00", NOW, now=NOW) is False

    @pytest.mark.parametrize("bad", ["not a date", ""])
    def test_a_malformed_expiry_never_expires_and_is_logged(self, bad, caplog):
        with caplog.at_level(logging.DEBUG, logger="rakaia.stream_rules"):
            assert is_expired(None, bad, 0.0, now=NOW) is False
        assert "malformed" in caplog.text

    def test_a_permanent_store_expires_nothing(self):
        assert (
            is_expired(1, "2000-01-01T00:00:00Z", 0.0, now=NOW, permanent=True) is False
        )


class TestCreate:
    def test_a_permanent_store_refuses_a_ttl_or_an_expiry(self):
        with pytest.raises(ExpiryNotAllowed):
            check_expiry_allowed("s", 60, None, permanent=True)
        with pytest.raises(ExpiryNotAllowed):
            check_expiry_allowed("s", None, "2099-01-01T00:00:00Z", permanent=True)
        check_expiry_allowed("s", None, None, permanent=True)
        check_expiry_allowed("s", 60, None)

    def test_the_same_config_is_idempotent(self):
        a = StreamConfig("application/json", 60, None, False)
        assert same_config(a, StreamConfig("application/json; charset=utf-8", 60))

    @pytest.mark.parametrize(
        "requested",
        [
            StreamConfig("text/plain", 60),
            StreamConfig("application/json", 30),
            StreamConfig("application/json", 60, "2099-01-01T00:00:00Z"),
            StreamConfig("application/json", 60, None, True),
        ],
    )
    def test_any_difference_is_a_conflict(self, requested):
        assert not same_config(StreamConfig("application/json", 60), requested)

    def test_a_permanent_store_compares_an_old_expiry_as_none(self):
        existing = StreamConfig(None, 60, "2099-01-01T00:00:00Z")
        assert same_config(existing, StreamConfig(), permanent=True)
        assert not same_config(existing, StreamConfig())

    def test_a_permanent_store_reports_no_expiry(self):
        assert reported_expiry(60, "x", permanent=True) == (None, None)
        assert reported_expiry(60, "x") == (60, "x")


class TestDelete:
    def test_a_permanent_store_refuses_a_client_delete(self):
        with pytest.raises(DeleteNotAllowed):
            check_protocol_delete("s", permanent=True)
        check_protocol_delete("s")

    def test_a_permanent_store_refuses_an_unforced_delete(self):
        with pytest.raises(DeleteNotAllowed):
            check_delete("s", permanent=True)
        check_delete("s", permanent=True, force=True)
        check_delete("s")
        check_delete("s", force=True)


TUPLE = ("p", 1, 0)


class TestClose:
    def test_a_plain_close_closes_and_touches(self):
        verdict = decide_close(closed=False, closed_by=None, now=NOW)
        assert (verdict.close, verdict.already_closed, verdict.touch) == (
            True,
            False,
            True,
        )
        assert verdict.producer_result is None

    def test_a_plain_close_of_a_closed_stream_still_touches(self):
        verdict = decide_close(closed=True, closed_by=None, now=NOW)
        assert (verdict.close, verdict.already_closed, verdict.touch) == (
            False,
            True,
            True,
        )

    def test_a_fenced_close_lands_and_records_its_tuple(self):
        verdict = decide_close(closed=False, closed_by=None, producer=TUPLE, now=NOW)
        assert verdict.close is True and verdict.touch is True
        assert isinstance(verdict.producer_result, ProducerAccepted)
        assert verdict.closed_by == ClosedBy("p", epoch=1, seq=0)

    def test_a_retry_of_the_closing_tuple_is_a_duplicate(self):
        verdict = decide_close(
            closed=True,
            closed_by=ClosedBy("p", epoch=1, seq=0),
            producer=TUPLE,
            now=NOW,
        )
        assert verdict.already_closed is True and verdict.close is False
        assert isinstance(verdict.producer_result, ProducerDuplicate)
        assert verdict.touch is False

    @pytest.mark.parametrize("other", [("p", 2, 0), ("p", 1, 1)])
    def test_the_same_producer_with_another_epoch_or_seq_is_told_closed(self, other):
        verdict = decide_close(
            closed=True,
            closed_by=ClosedBy("p", epoch=1, seq=0),
            producer=other,
            now=NOW,
        )
        assert isinstance(verdict.producer_result, ProducerStreamClosed)

    def test_a_duplicate_reports_the_closing_seq(self):
        verdict = decide_close(
            closed=True,
            closed_by=ClosedBy("p", epoch=1, seq=7),
            producer=("p", 1, 7),
            now=NOW,
        )
        assert verdict.producer_result == ProducerDuplicate(last_seq=7)

    def test_another_tuple_on_a_closed_stream_is_told_it_is_closed(self):
        verdict = decide_close(
            closed=True,
            closed_by=ClosedBy("q", epoch=0, seq=0),
            producer=TUPLE,
            now=NOW,
        )
        assert isinstance(verdict.producer_result, ProducerStreamClosed)
        assert verdict.closed_by is None

    def test_closed_is_reported_before_the_producer_is_fenced(self):
        """A producer whose sequence is also wrong learns the stream is closed,
        not about a gap in a close that can never land."""
        verdict = decide_close(
            closed=True,
            closed_by=None,
            producer=("p", 0, 9),
            producer_state=ProducerState(epoch=0, last_seq=0, last_updated=NOW),
            now=NOW,
        )
        assert isinstance(verdict.producer_result, ProducerStreamClosed)

    def test_a_fenced_out_close_does_nothing(self):
        verdict = decide_close(
            closed=False, closed_by=None, producer=("p", 0, 5), now=NOW
        )
        assert isinstance(verdict.producer_result, ProducerSequenceGap)
        assert (verdict.close, verdict.already_closed, verdict.touch) == (
            False,
            False,
            False,
        )
