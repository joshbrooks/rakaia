"""The append admission rules, as a table — no store, no database, no async.

This is the payoff of lifting the decision out of the two adapters. Asserting
"a sequence gap is refused and reports the expected seq" used to need a live
store; here it is a function call on a frozen dataclass.

The ordering cases at the bottom are the ones worth having. Each was previously
implicit in the *sequence of if-statements* inside two separate methods, where a
reordering during an unrelated edit would change behaviour with nothing to catch
it.
"""

from __future__ import annotations

import pytest

from rakaia.append_decision import StreamFacts, decide_append
from rakaia.types import (
    AppendOptions,
    ClosedBy,
    ContentTypeMismatch,
    ProducerAccepted,
    ProducerDuplicate,
    ProducerSequenceGap,
    ProducerStaleEpoch,
    ProducerState,
    ProducerStreamClosed,
    SequenceConflict,
)

NOW = 1_000.0


def _decide(facts=None, opts=None, *, producer_state=None):
    return decide_append(
        facts or StreamFacts(),
        opts or AppendOptions(),
        producer_state=producer_state,
        now=NOW,
    )


class TestTheOpenHappyPath:
    def test_a_plain_append_is_allowed(self):
        verdict = _decide()
        assert verdict.write is True
        assert verdict.producer_result is None

    def test_a_first_producer_write_is_accepted(self):
        verdict = _decide(
            opts=AppendOptions(producer_id="p", producer_epoch=0, producer_seq=0)
        )
        assert verdict.write is True
        assert isinstance(verdict.producer_result, ProducerAccepted)


class TestClosed:
    def test_a_closed_stream_refuses(self):
        verdict = _decide(StreamFacts(closed=True))
        assert verdict.write is False
        assert verdict.stream_closed is True
        assert isinstance(verdict.producer_result, ProducerStreamClosed)

    def test_the_producer_that_closed_it_gets_a_duplicate(self):
        """A retry of the closing append must be distinguishable from someone
        else's stream being closed underneath you."""
        facts = StreamFacts(closed=True, closed_by=ClosedBy("p", epoch=2, seq=7))
        verdict = _decide(
            facts,
            AppendOptions(producer_id="p", producer_epoch=2, producer_seq=7),
        )
        assert verdict.stream_closed is True
        assert isinstance(verdict.producer_result, ProducerDuplicate)

    @pytest.mark.parametrize(
        "producer_id,epoch,seq",
        [
            ("other", 2, 7),  # different producer
            ("p", 3, 7),  # same producer, later epoch
            ("p", 2, 8),  # same producer, different seq
        ],
    )
    def test_any_other_tuple_is_told_the_stream_is_closed(
        self, producer_id, epoch, seq
    ):
        facts = StreamFacts(closed=True, closed_by=ClosedBy("p", epoch=2, seq=7))
        verdict = _decide(
            facts,
            AppendOptions(
                producer_id=producer_id, producer_epoch=epoch, producer_seq=seq
            ),
        )
        assert verdict.stream_closed is True
        assert isinstance(verdict.producer_result, ProducerStreamClosed)


class TestContentType:
    def test_a_mismatch_raises(self):
        with pytest.raises(ContentTypeMismatch):
            _decide(
                StreamFacts(content_type="application/json"),
                AppendOptions(content_type="text/plain"),
            )

    def test_a_match_is_allowed_despite_parameters(self):
        verdict = _decide(
            StreamFacts(content_type="application/json"),
            AppendOptions(content_type="application/json; charset=utf-8"),
        )
        assert verdict.write is True

    def test_no_declared_type_on_either_side_is_allowed(self):
        assert _decide(opts=AppendOptions(content_type="text/csv")).write is True


class TestStreamSeq:
    def test_a_replayed_seq_conflicts(self):
        with pytest.raises(SequenceConflict):
            _decide(StreamFacts(last_seq=5), AppendOptions(seq=5))

    def test_a_lower_seq_conflicts(self):
        with pytest.raises(SequenceConflict):
            _decide(StreamFacts(last_seq=5), AppendOptions(seq=4))

    def test_a_higher_seq_is_allowed(self):
        assert _decide(StreamFacts(last_seq=5), AppendOptions(seq=6)).write is True

    def test_seq_is_compared_as_a_number(self):
        """`"10" < "9"` lexicographically — the bug that broke every producer on
        reaching double digits."""
        assert _decide(StreamFacts(last_seq=9), AppendOptions(seq=10)).write is True


class TestProducerFencing:
    def test_a_stale_epoch_is_refused(self):
        state = ProducerState(epoch=5, last_seq=0, last_updated=NOW)
        verdict = _decide(
            opts=AppendOptions(producer_id="p", producer_epoch=4, producer_seq=1),
            producer_state=state,
        )
        assert verdict.write is False
        assert isinstance(verdict.producer_result, ProducerStaleEpoch)

    def test_a_gap_reports_the_expected_seq(self):
        state = ProducerState(epoch=0, last_seq=0, last_updated=NOW)
        verdict = _decide(
            opts=AppendOptions(producer_id="p", producer_epoch=0, producer_seq=5),
            producer_state=state,
        )
        assert isinstance(verdict.producer_result, ProducerSequenceGap)
        assert verdict.producer_result.expected_seq == 1

    def test_a_partial_producer_tuple_is_not_fenced(self):
        """All three fields or none — a `Producer-Id` with no epoch/seq is not
        enough to fence on, and must not be silently treated as one."""
        verdict = _decide(opts=AppendOptions(producer_id="p"))
        assert verdict.write is True
        assert verdict.producer_result is None


class TestOrdering:
    """Each case pins one *relative* order. These are the rules that were
    previously encoded only as the order of if-statements in two methods."""

    def test_closed_is_checked_before_content_type(self):
        """A closed stream refuses rather than raising, even with a bad type —
        otherwise a client retrying against a closed stream gets a confusing
        400 instead of the close it needs to see."""
        verdict = _decide(
            StreamFacts(closed=True, content_type="application/json"),
            AppendOptions(content_type="text/plain"),
        )
        assert verdict.write is False
        assert verdict.stream_closed is True

    def test_closed_is_checked_before_producer_fencing(self):
        """A closed stream is the answer even when the fencing tuple is also
        wrong — reporting the gap instead would tell a producer to fix its
        sequence and retry a write that can never land, and it is what the
        durable store did before this sequence was shared."""
        state = ProducerState(epoch=0, last_seq=0, last_updated=NOW)
        verdict = _decide(
            StreamFacts(closed=True, closed_by=ClosedBy("p", epoch=0, seq=0)),
            AppendOptions(producer_id="p", producer_epoch=0, producer_seq=9),
            producer_state=state,
        )
        assert verdict.write is False
        assert verdict.stream_closed is True
        assert isinstance(verdict.producer_result, ProducerStreamClosed)

    def test_fencing_is_checked_before_stream_seq(self):
        """The load-bearing one. A retried append carries the same Stream-Seq it
        did the first time, so checking Stream-Seq first would raise
        SequenceConflict on exactly the retry that fencing exists to absorb."""
        state = ProducerState(epoch=0, last_seq=3, last_updated=NOW)
        verdict = _decide(
            StreamFacts(last_seq=9),
            AppendOptions(producer_id="p", producer_epoch=0, producer_seq=3, seq=9),
            producer_state=state,
        )
        # Reported as a duplicate, not raised as a sequence conflict.
        assert verdict.write is False
        assert isinstance(verdict.producer_result, ProducerDuplicate)

    def test_content_type_is_checked_before_fencing(self):
        state = ProducerState(epoch=5, last_seq=0, last_updated=NOW)
        with pytest.raises(ContentTypeMismatch):
            _decide(
                StreamFacts(content_type="application/json"),
                AppendOptions(
                    content_type="text/plain",
                    producer_id="p",
                    producer_epoch=4,
                    producer_seq=1,
                ),
                producer_state=state,
            )
