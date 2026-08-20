"""The producer-fencing outcome table, pinned directly.

`rakaia.producer` is the whole of the fencing rule: from a producer's last
committed state and the `(id, epoch, seq)` tuple it presents, exactly one of six
outcomes follows. Both stores call it — the in-memory one keeps producer state in
a dict, the durable one in a table — so that they cannot decide differently.

Until now nothing named `validate_producer` or `is_producer_state_expired`. The
callers do cover it, and more thoroughly than "untested" would suggest — stubbing
out the stale-epoch branch alone fails seven tests across five files
(`test_append_decision.py`, both `test_server_store_contract.py`, `test_store.py`,
`test_protocol_server.py`). What they cover is the *outcomes*, each reached
through a live append or a `decide_append` call.

What that shape does not reach is the **edges and the payloads**. A caller
appending for real picks inputs comfortably inside a branch and looks at which
outcome came back, not at what it carried. Measured: of nine mutations to
`producer.py`, seven leave the entire suite outside this file green —

- expiry `>` → `>=`, and `abs()` around the age (clock skew reads as expired);
- an unknown producer's gap reporting `expected_seq=seq` instead of `0`;
- a new epoch carrying the old `last_seq` forward instead of restarting at 0;
- the proposal stamping the old `last_updated` rather than the `now` it was given;
- `validate_producer` mutating the state it was handed;
- `is_new` set on a same-epoch continuation.

Only two of the nine — the duplicate boundary and the gap's `expected_seq` — are
caught elsewhere. #154's defect was a write door that skipped this table
altogether, so the branches being individually load-bearing is not hypothetical.

So this file is the table itself: one class per outcome, every branch reached,
and the boundaries asserted rather than assumed.

**One member of the result union is deliberately absent.**
`ProducerStreamClosed` is never returned here: whether a stream is closed is not
a fencing question, and `rakaia.append_decision.decide_append` checks closure
*before* consulting this table (asserted behaviourally in
`test_append_decision.py`). Said here rather than as a test, because no mutation
of `validate_producer` can fail such a test uniquely — nothing in the function
constructs the type, so the only version that reds is one asserting on the
module's source text, which a passing mention in a comment would satisfy. A
branch for it appearing here would be a second, weaker copy of a decision that
lives upstream; this paragraph is where someone editing the table will read that.
"""

from __future__ import annotations

import pytest

from rakaia.producer import is_producer_state_expired, validate_producer
from rakaia.types import (
    PRODUCER_STATE_TTL_SECONDS,
    ProducerAccepted,
    ProducerDuplicate,
    ProducerInvalidEpochSeq,
    ProducerSequenceGap,
    ProducerStaleEpoch,
    ProducerState,
)

NOW = 1_000_000.0
PID = "writer-1"


def _state(epoch: int, last_seq: int, *, last_updated: float = NOW) -> ProducerState:
    return ProducerState(epoch=epoch, last_seq=last_seq, last_updated=last_updated)


def _stale_state(epoch: int = 1, last_seq: int = 5) -> ProducerState:
    """A state old enough that `is_producer_state_expired` treats it as absent."""
    return _state(epoch, last_seq, last_updated=NOW - PRODUCER_STATE_TTL_SECONDS - 1)


# =============================================================================
# Expiry — the predicate the rest of the table depends on
# =============================================================================


class TestExpiry:
    def test_the_ttl_is_seven_days(self):
        # Every other expiry test is expressed *relative* to the constant, so
        # they all stay green if its value changes — which means the value itself
        # was asserted nowhere in the repo and a TTL change would land silently.
        # Restating a constant normally tests nothing, but this one is how long
        # producer fencing survives an idle writer (see TestExpiryOutranksFencing),
        # so it is a decision rather than an implementation detail. Same reasoning
        # as `test_public_api.py` writing out `__all__` by hand: when this fails,
        # that is the test working — change the number here deliberately.
        assert PRODUCER_STATE_TTL_SECONDS == 7 * 24 * 60 * 60

    def test_fresh_state_is_not_expired(self):
        assert not is_producer_state_expired(_state(1, 0), NOW)

    def test_state_much_older_than_the_ttl_is_expired(self):
        ancient = _state(1, 0, last_updated=NOW - PRODUCER_STATE_TTL_SECONDS * 10)
        assert is_producer_state_expired(ancient, NOW)

    def test_exactly_at_the_ttl_is_not_yet_expired(self):
        # The boundary is `>`, not `>=`: a state whose age equals the TTL to the
        # second is still live. Pinned because flipping the comparison is a
        # one-character change that no caller-level test would notice.
        edge = _state(1, 0, last_updated=NOW - PRODUCER_STATE_TTL_SECONDS)
        assert not is_producer_state_expired(edge, NOW)

    def test_one_second_past_the_ttl_is_expired(self):
        past = _state(1, 0, last_updated=NOW - PRODUCER_STATE_TTL_SECONDS - 1)
        assert is_producer_state_expired(past, NOW)

    def test_a_clock_skewed_far_into_the_future_is_not_expired(self):
        # Clock skew between writers gives a *negative* age, and the rule must
        # read that as fresh. The skew has to exceed the TTL to test anything: a
        # stamp an hour ahead is still well inside 7 days, so `abs()` around the
        # age would return not-expired anyway and the case would pass vacuously.
        # At TTL + 1 ahead, `abs()` reads it as expired and this goes red.
        future = _state(1, 0, last_updated=NOW + PRODUCER_STATE_TTL_SECONDS + 1)
        assert not is_producer_state_expired(future, NOW)


# =============================================================================
# Unknown producer — no state, or state that has aged out
# =============================================================================


class TestUnknownProducer:
    def test_opening_at_seq_zero_is_accepted_as_new(self):
        result = validate_producer(None, PID, epoch=0, seq=0, now=NOW)
        assert result == ProducerAccepted(
            is_new=True,
            producer_id=PID,
            proposed_state=_state(0, 0),
        )

    def test_opening_above_seq_zero_is_a_gap_against_zero(self):
        result = validate_producer(None, PID, epoch=0, seq=3, now=NOW)
        assert result == ProducerSequenceGap(expected_seq=0, received_seq=3)

    def test_the_gap_reports_the_seq_that_was_offered(self):
        # Both numbers matter to the caller: the protocol echoes them back in
        # Producer-Expected-Seq / Producer-Received-Seq.
        result = validate_producer(None, PID, epoch=4, seq=9, now=NOW)
        assert isinstance(result, ProducerSequenceGap)
        assert (result.expected_seq, result.received_seq) == (0, 9)

    def test_an_expired_state_is_treated_as_unknown(self):
        result = validate_producer(_stale_state(), PID, epoch=1, seq=0, now=NOW)
        assert isinstance(result, ProducerAccepted)
        assert result.is_new is True

    def test_an_expired_state_still_requires_seq_zero(self):
        result = validate_producer(_stale_state(), PID, epoch=1, seq=6, now=NOW)
        assert result == ProducerSequenceGap(expected_seq=0, received_seq=6)

    def test_the_accepted_epoch_is_the_one_offered_not_zero(self):
        result = validate_producer(None, PID, epoch=7, seq=0, now=NOW)
        assert isinstance(result, ProducerAccepted)
        assert result.proposed_state is not None
        assert result.proposed_state.epoch == 7


class TestExpiryOutranksFencing:
    """An expired state is treated as absent **before** the epoch is looked at.

    So a producer that was fenced out by a higher epoch is admitted again once
    the old state ages past the TTL — even presenting the lower epoch it was
    fenced on. That is the documented intent (expiry exists so an abandoned id
    starts fresh rather than pinning its sequence forever), but it means fencing
    is not permanent: it lasts as long as the winner keeps its state warm.

    Pinned as its own class because it is the one row of the table where two
    rules could each plausibly win, and nothing else says which does.
    """

    def test_a_lower_epoch_wins_once_the_state_has_expired(self):
        fenced_out = _stale_state(epoch=9, last_seq=100)
        result = validate_producer(fenced_out, PID, epoch=2, seq=0, now=NOW)
        assert isinstance(result, ProducerAccepted)
        assert result.proposed_state is not None
        assert result.proposed_state.epoch == 2

    def test_the_same_tuple_is_fenced_while_the_state_is_fresh(self):
        # The contrast that gives the previous case its meaning: identical
        # inputs, a fresh state, opposite outcome.
        fresh = _state(9, 100)
        result = validate_producer(fresh, PID, epoch=2, seq=0, now=NOW)
        assert result == ProducerStaleEpoch(current_epoch=9)


# =============================================================================
# Epoch below the known one — fenced out
# =============================================================================


class TestStaleEpoch:
    def test_a_lower_epoch_is_stale(self):
        result = validate_producer(_state(5, 3), PID, epoch=4, seq=4, now=NOW)
        assert result == ProducerStaleEpoch(current_epoch=5)

    def test_staleness_is_decided_before_the_sequence(self):
        # A stale epoch is stale whatever seq it brings — including a seq that
        # would have been a clean accept at the current epoch.
        result = validate_producer(_state(5, 3), PID, epoch=1, seq=0, now=NOW)
        assert isinstance(result, ProducerStaleEpoch)

    def test_it_reports_the_epoch_that_won(self):
        result = validate_producer(_state(12, 0), PID, epoch=11, seq=0, now=NOW)
        assert isinstance(result, ProducerStaleEpoch)
        assert result.current_epoch == 12


# =============================================================================
# Epoch above the known one — a new epoch must restart the sequence
# =============================================================================


class TestNewEpoch:
    def test_a_higher_epoch_opening_at_zero_is_accepted(self):
        result = validate_producer(_state(1, 40), PID, epoch=2, seq=0, now=NOW)
        assert result == ProducerAccepted(
            is_new=True,
            producer_id=PID,
            proposed_state=_state(2, 0),
        )

    def test_a_higher_epoch_not_opening_at_zero_is_rejected(self):
        result = validate_producer(_state(1, 40), PID, epoch=2, seq=41, now=NOW)
        assert result == ProducerInvalidEpochSeq()

    def test_continuing_the_old_sequence_under_a_new_epoch_is_rejected(self):
        # The realistic mistake: a restarted writer bumps its epoch but carries
        # on counting. Distinguished from a gap on purpose — the remedy differs
        # (restart at 0, rather than resend the missing seq).
        result = validate_producer(_state(3, 7), PID, epoch=4, seq=8, now=NOW)
        assert isinstance(result, ProducerInvalidEpochSeq)
        assert not isinstance(result, ProducerSequenceGap)

    def test_a_new_epoch_discards_the_old_sequence_high_mark(self):
        result = validate_producer(_state(1, 999), PID, epoch=2, seq=0, now=NOW)
        assert isinstance(result, ProducerAccepted)
        assert result.proposed_state is not None
        assert result.proposed_state.last_seq == 0

    def test_an_epoch_may_jump_by_more_than_one(self):
        # Epochs are a fencing token, not a sequence: a writer that restarts
        # several times while offline presents whatever it reached.
        result = validate_producer(_state(1, 0), PID, epoch=50, seq=0, now=NOW)
        assert isinstance(result, ProducerAccepted)
        assert result.proposed_state is not None
        assert result.proposed_state.epoch == 50


# =============================================================================
# Same epoch — duplicate, accept, or gap
# =============================================================================


class TestSameEpoch:
    def test_exactly_one_above_the_last_is_accepted(self):
        result = validate_producer(_state(2, 5), PID, epoch=2, seq=6, now=NOW)
        assert result == ProducerAccepted(
            is_new=False,
            producer_id=PID,
            proposed_state=_state(2, 6),
        )

    def test_an_accepted_continuation_is_not_marked_new(self):
        result = validate_producer(_state(2, 5), PID, epoch=2, seq=6, now=NOW)
        assert isinstance(result, ProducerAccepted)
        assert result.is_new is False

    def test_repeating_the_last_seq_is_a_duplicate(self):
        result = validate_producer(_state(2, 5), PID, epoch=2, seq=5, now=NOW)
        assert result == ProducerDuplicate(last_seq=5)

    def test_a_seq_below_the_last_is_also_a_duplicate(self):
        # A retry of an older request, not an error: the append already landed,
        # so the caller is told the same thing either way.
        result = validate_producer(_state(2, 5), PID, epoch=2, seq=1, now=NOW)
        assert result == ProducerDuplicate(last_seq=5)

    def test_two_above_the_last_is_a_gap(self):
        result = validate_producer(_state(2, 5), PID, epoch=2, seq=7, now=NOW)
        assert result == ProducerSequenceGap(expected_seq=6, received_seq=7)

    def test_the_gap_names_the_seq_that_was_expected(self):
        result = validate_producer(_state(2, 5), PID, epoch=2, seq=99, now=NOW)
        assert isinstance(result, ProducerSequenceGap)
        assert (result.expected_seq, result.received_seq) == (6, 99)

    def test_seq_zero_against_a_known_sequence_is_a_duplicate_not_a_restart(self):
        # Same epoch, so seq 0 is a replay of the opening write — a new epoch is
        # the only way to legitimately restart a sequence.
        result = validate_producer(_state(2, 5), PID, epoch=2, seq=0, now=NOW)
        assert result == ProducerDuplicate(last_seq=5)

    def test_a_first_write_after_opening_at_zero_is_accepted(self):
        # The full opening handshake: seq 0 opens (is_new), seq 1 continues.
        opened = validate_producer(None, PID, epoch=0, seq=0, now=NOW)
        assert isinstance(opened, ProducerAccepted)
        assert opened.proposed_state is not None

        second = validate_producer(opened.proposed_state, PID, epoch=0, seq=1, now=NOW)
        assert second == ProducerAccepted(
            is_new=False,
            producer_id=PID,
            proposed_state=_state(0, 1),
        )


# =============================================================================
# What the function promises about itself
# =============================================================================


class TestTheFunctionIsPure:
    """The module docstring makes three promises. Each is asserted here.

    They matter because the caller commits `proposed_state` only *after* the
    append succeeds — so a function that mutated the state it was handed, or that
    advanced a sequence itself, would let a failed write move a producer's
    sequence forward. That is the failure this design exists to prevent, and
    nothing was checking the design held.
    """

    def test_the_state_passed_in_is_not_mutated(self):
        # `now` is deliberately *not* NOW. The state's `last_updated` is NOW, so
        # calling with `now=NOW` makes a `state.last_updated = now` mutation a
        # no-op and this assertion cannot see it — which is exactly what happened
        # the first time this test was written.
        state = _state(2, 5, last_updated=NOW)
        before = ProducerState(
            epoch=state.epoch, last_seq=state.last_seq, last_updated=state.last_updated
        )
        validate_producer(state, PID, epoch=2, seq=6, now=NOW + 500.0)
        assert state == before

    @pytest.mark.parametrize(
        ("epoch", "seq"),
        [
            # same epoch: duplicate, accept, gap
            (2, 0),
            (2, 5),
            (2, 6),
            (2, 8),
            # lower epoch: stale. Reached only by varying `epoch` — an earlier
            # version of this test held it at 2 and so never entered this branch
            # or the next, leaving a `state.last_updated = now` inserted before
            # either `return` alive against the whole suite.
            (1, 0),
            (1, 6),
            # higher epoch: accepted restart, and invalid non-zero start
            (3, 0),
            (3, 6),
        ],
    )
    def test_no_outcome_mutates_the_state(self, epoch, seq):
        # Every branch, not just the accept: `ProducerState` is a plain dataclass,
        # so a rejection that advanced `last_updated` would keep a dead producer's
        # state alive forever, and one that advanced `last_seq` would move a
        # sequence on a write that never happened.
        state = _state(2, 5, last_updated=NOW)
        before = ProducerState(
            epoch=state.epoch, last_seq=state.last_seq, last_updated=state.last_updated
        )
        validate_producer(state, PID, epoch=epoch, seq=seq, now=NOW + 500.0)
        assert state == before

    def test_the_proposed_state_is_a_different_object(self):
        # Returning the same object would let a caller's later mutation of the
        # proposal reach back into the committed state.
        state = _state(2, 5)
        result = validate_producer(state, PID, epoch=2, seq=6, now=NOW)
        assert isinstance(result, ProducerAccepted)
        assert result.proposed_state is not state

    def test_calling_twice_gives_the_same_answer(self):
        state = _state(2, 5)
        args = (state, PID)
        first = validate_producer(*args, epoch=2, seq=6, now=NOW)
        second = validate_producer(*args, epoch=2, seq=6, now=NOW)
        assert first == second

    def test_the_proposal_stamps_the_now_it_was_given(self):
        # Not `time.time()`: the caller supplies the clock so a replay or a test
        # is deterministic.
        result = validate_producer(_state(2, 5), PID, epoch=2, seq=6, now=12345.0)
        assert isinstance(result, ProducerAccepted)
        assert result.proposed_state is not None
        assert result.proposed_state.last_updated == 12345.0

    def test_a_rejection_proposes_no_state(self):
        # Only an accept carries a proposal; the other outcomes have nothing to
        # commit, and a caller that committed one anyway would advance a
        # sequence on a rejected write.
        for result in (
            validate_producer(_state(5, 3), PID, epoch=4, seq=4, now=NOW),
            validate_producer(_state(1, 4), PID, epoch=2, seq=9, now=NOW),
            validate_producer(_state(2, 5), PID, epoch=2, seq=5, now=NOW),
            validate_producer(_state(2, 5), PID, epoch=2, seq=8, now=NOW),
        ):
            assert not isinstance(result, ProducerAccepted)
            assert not hasattr(result, "proposed_state")
