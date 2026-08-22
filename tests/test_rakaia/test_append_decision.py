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

from rakaia.append_decision import (
    StreamFacts,
    check_payload,
    decide_append,
    decide_append_batch,
)
from rakaia.types import (
    AppendOptions,
    ClosedBy,
    ContentTypeMismatch,
    EmptyJsonArray,
    InvalidJson,
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
        ("producer_id", "epoch", "seq"),
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
            _decide(StreamFacts(last_seq="5"), AppendOptions(seq="5"))

    def test_a_lower_seq_conflicts(self):
        with pytest.raises(SequenceConflict):
            _decide(StreamFacts(last_seq="5"), AppendOptions(seq="4"))

    def test_a_higher_seq_is_allowed(self):
        assert _decide(StreamFacts(last_seq="5"), AppendOptions(seq="6")).write is True

    def test_seq_is_compared_lexicographically(self):
        """`Stream-Seq` is an opaque string compared byte-wise, so `"10"` after
        `"9"` is a conflict — `"10" < "9"`. That is the protocol's rule, not a
        bug: a writer that needs its values to order pads them to a fixed width
        or uses a ULID. Rakaia's own offsets zero-pad for exactly this reason
        (`src/rakaia/store.py`), so `"09"` then `"10"` is accepted below."""
        with pytest.raises(SequenceConflict):
            _decide(StreamFacts(last_seq="9"), AppendOptions(seq="10"))
        assert (
            _decide(StreamFacts(last_seq="09"), AppendOptions(seq="10")).write is True
        )

    def test_a_non_numeric_seq_is_a_valid_opaque_string(self):
        """ULIDs are the idiomatic conforming value; nothing may reject them."""
        assert (
            _decide(
                StreamFacts(last_seq="01ARZ3NDEKTSV4RRFFQ69G5FAV"),
                AppendOptions(seq="01ARZ3NDEKTSV4RRFFQ69G5FAW"),
            ).write
            is True
        )


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
            StreamFacts(last_seq="9"),
            AppendOptions(producer_id="p", producer_epoch=0, producer_seq=3, seq="9"),
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


def _decide_batch(facts=None, items=(), *, payloads=None, producer_states=None):
    """`payloads` defaults to a valid JSON body per item, so a case about
    admission does not have to restate one."""
    items = list(items)
    return decide_append_batch(
        facts or StreamFacts(),
        items,
        payloads=[b"{}"] * len(items) if payloads is None else list(payloads),
        producer_states=producer_states,
        now=NOW,
    )


class TestBatchAdmissibility:
    """The batch rule, same treatment: no store, no database.

    Both stores hand-rolled this and the two disagreed (#181). Each case below
    is one of the ways they disagreed, or one of the rules that stops them
    disagreeing again.
    """

    def test_one_verdict_per_item_in_order(self):
        batch = _decide_batch(items=[AppendOptions(), None, AppendOptions()])
        assert len(batch.verdicts) == 3
        assert all(v.write for v in batch.verdicts)
        assert batch.writes_anything is True

    def test_an_empty_batch_decides_nothing(self):
        batch = _decide_batch()
        assert batch.verdicts == []
        assert batch.writes_anything is False

    def test_stream_seq_advances_across_the_batch(self):
        batch = _decide_batch(items=[AppendOptions(seq="1"), AppendOptions(seq="2")])
        assert all(v.write for v in batch.verdicts)
        assert batch.last_seq == "2"

    def test_a_conflict_inside_the_batch_propagates(self):
        """All-or-nothing: the caller has decided nothing when it sees this, so
        it writes nothing. A plain loop of `append` would leave a prefix."""
        with pytest.raises(SequenceConflict):
            _decide_batch(items=[AppendOptions(seq="2"), AppendOptions(seq="1")])

    def test_a_refused_item_does_not_advance_stream_seq(self):
        """The in-memory store's own scan advanced it here, so a batch whose
        first item was fenced out raised a conflict against a sequence nothing
        had ever written."""
        state = ProducerState(epoch=5, last_seq=0, last_updated=NOW)
        batch = _decide_batch(
            items=[
                AppendOptions(
                    producer_id="p", producer_epoch=1, producer_seq=0, seq="9"
                ),
                AppendOptions(seq="3"),
            ],
            producer_states={"p": state},
        )
        assert isinstance(batch.verdicts[0].producer_result, ProducerStaleEpoch)
        assert batch.verdicts[0].write is False
        assert batch.verdicts[1].write is True
        assert batch.last_seq == "3"

    def test_a_producer_is_fenced_against_its_own_earlier_item(self):
        opts = AppendOptions(producer_id="p", producer_epoch=0, producer_seq=0)
        batch = _decide_batch(items=[opts, opts])
        assert isinstance(batch.verdicts[0].producer_result, ProducerAccepted)
        assert isinstance(batch.verdicts[1].producer_result, ProducerDuplicate)
        assert batch.verdicts[1].write is False

    def test_a_refused_item_does_not_advance_its_producer(self):
        """A gap is refused and takes no sequence, so the next item from the
        same producer is still judged against the pre-batch state."""
        batch = _decide_batch(
            items=[
                AppendOptions(producer_id="p", producer_epoch=0, producer_seq=4),
                AppendOptions(producer_id="p", producer_epoch=0, producer_seq=0),
            ]
        )
        assert isinstance(batch.verdicts[0].producer_result, ProducerSequenceGap)
        assert isinstance(batch.verdicts[1].producer_result, ProducerAccepted)
        assert batch.verdicts[1].write is True

    def test_a_close_refuses_the_items_after_it(self):
        closing = AppendOptions(close=True)
        batch = _decide_batch(items=[AppendOptions(), closing, AppendOptions()])
        assert [v.write for v in batch.verdicts] == [True, True, False]
        assert batch.verdicts[2].stream_closed is True
        assert batch.closing_opts is closing

    def test_a_close_records_which_tuple_closed_the_stream(self):
        """So the items after it can recognise a re-send of that same closing
        append as a duplicate — the answer a loop of `append` gives, and the
        one the durable store lost by short-circuiting on `closed`."""
        closing = AppendOptions(
            producer_id="p", producer_epoch=0, producer_seq=0, close=True
        )
        batch = _decide_batch(items=[closing, closing])
        assert batch.verdicts[0].write is True
        assert batch.verdicts[1].write is False
        assert batch.verdicts[1].stream_closed is True
        assert isinstance(batch.verdicts[1].producer_result, ProducerDuplicate)
        assert batch.closing_opts is closing

    def test_an_already_closed_stream_still_gets_a_verdict_per_item(self):
        batch = _decide_batch(
            StreamFacts(closed=True, closed_by=ClosedBy("p", epoch=0, seq=0)),
            items=[
                AppendOptions(producer_id="p", producer_epoch=0, producer_seq=0),
                AppendOptions(),
            ],
        )
        assert batch.writes_anything is False
        assert isinstance(batch.verdicts[0].producer_result, ProducerDuplicate)
        assert isinstance(batch.verdicts[1].producer_result, ProducerStreamClosed)

    def test_the_batch_commits_one_state_per_producer(self):
        """The last accepted outcome per producer, not one per accepted item.

        A store persists exactly these, so the count of writes it owes does not
        grow with the batch. Three accepted items from one producer leave one
        commit, carrying the third item's sequence.
        """
        batch = _decide_batch(
            items=[
                AppendOptions(producer_id="p", producer_epoch=0, producer_seq=i)
                for i in range(3)
            ]
        )
        assert all(v.write for v in batch.verdicts)
        assert list(batch.producer_commits) == ["p"]
        assert batch.producer_commits["p"].proposed_state.last_seq == 2

    def test_each_producer_in_a_batch_gets_its_own_commit(self):
        batch = _decide_batch(
            items=[
                AppendOptions(producer_id="a", producer_epoch=0, producer_seq=0),
                AppendOptions(producer_id="b", producer_epoch=0, producer_seq=0),
            ]
        )
        assert sorted(batch.producer_commits) == ["a", "b"]

    def test_a_refused_producer_leaves_no_commit(self):
        """Nothing was written, so there is nothing to persist — a commit here
        would advance a producer whose write never landed."""
        batch = _decide_batch(
            items=[AppendOptions(producer_id="p", producer_epoch=0, producer_seq=4)]
        )
        assert isinstance(batch.verdicts[0].producer_result, ProducerSequenceGap)
        assert batch.producer_commits == {}

    def test_the_callers_producer_states_are_not_mutated(self):
        """The rule is pure: a store must be free to abandon the whole batch on
        a conflict without having had its own state advanced under it."""
        states = {"p": None}
        _decide_batch(
            items=[AppendOptions(producer_id="p", producer_epoch=0, producer_seq=0)],
            producer_states=states,
        )
        assert states == {"p": None}


JSON = StreamFacts(content_type="application/json")


class TestPayloadValidity:
    """The body, as opposed to the options (#214).

    A batch was all-or-nothing on a content-type or `Stream-Seq` conflict and
    not on a body the stream could not hold, so the in-memory store persisted a
    prefix behind an `InvalidJson` and the durable store took the bad body.
    """

    def test_a_valid_json_body_is_admitted(self):
        assert check_payload("application/json", b'{"a": 1}') is None

    def test_an_unparseable_body_is_refused(self):
        with pytest.raises(InvalidJson):
            check_payload("application/json", b"not json")

    def test_an_empty_array_is_refused(self):
        with pytest.raises(EmptyJsonArray):
            check_payload("application/json", b"[]")

    @pytest.mark.parametrize("content_type", ["text/plain", "application/octet-stream"])
    def test_a_declared_non_json_content_type_takes_any_bytes(self, content_type):
        assert check_payload(content_type, b"not json") is None

    def test_no_declared_content_type_takes_any_bytes(self):
        """The event-sourcing shape: the store parses if it can and keeps the
        raw bytes if it cannot, so there is nothing here to refuse."""
        assert check_payload(None, b"not json") is None

    def test_a_charset_parameter_still_means_json(self):
        """`application/json; charset=utf-8` is a JSON stream, so its bodies are
        constrained — the check must normalise rather than compare literally."""
        with pytest.raises(InvalidJson):
            check_payload("application/json; charset=utf-8", b"not json")


class TestBatchPayloadValidity:
    """Where the payload check sits in the batch loop, which is the subtle part.

    Every case here is about *ordering*: a pass over the payloads before the
    admission scan, or after it, gets a different answer from a loop of
    `append`.
    """

    def test_a_bad_body_refuses_the_whole_batch(self):
        with pytest.raises(InvalidJson):
            _decide_batch(
                facts=JSON,
                items=[AppendOptions(), AppendOptions()],
                payloads=[b'{"a": 1}', b"not json"],
            )

    def test_the_payloads_must_match_the_items(self):
        """A store that dropped an item while building one list and not the
        other would otherwise check the wrong body against the wrong item."""
        with pytest.raises(ValueError):
            _decide_batch(
                facts=JSON, items=[AppendOptions(), AppendOptions()], payloads=[b"{}"]
            )

    def test_a_closed_stream_is_reported_before_the_body_is_read(self):
        """Closed outranks the body: a producer told "invalid JSON" would fix a
        body whose write can never land."""
        batch = _decide_batch(
            facts=StreamFacts(closed=True, content_type="application/json"),
            items=[AppendOptions()],
            payloads=[b"not json"],
        )
        assert isinstance(batch.verdicts[0].producer_result, ProducerStreamClosed)

    def test_a_body_after_a_close_inside_the_batch_is_not_read(self):
        """The close takes effect for the items after it, so their bodies are
        never parsed — the same rule, reached through the loop rather than
        through the incoming facts."""
        batch = _decide_batch(
            facts=JSON,
            items=[AppendOptions(close=True), AppendOptions()],
            payloads=[b'{"a": 1}', b"not json"],
        )
        assert batch.verdicts[0].write is True
        assert batch.verdicts[1].write is False

    def test_a_body_the_fence_refuses_is_not_read(self):
        """An item the fence rejects takes no write, so it takes no parse."""
        batch = _decide_batch(
            facts=JSON,
            items=[AppendOptions(producer_id="p", producer_epoch=0, producer_seq=4)],
            payloads=[b"not json"],
        )
        assert isinstance(batch.verdicts[0].producer_result, ProducerSequenceGap)

    def test_a_content_type_conflict_is_raised_before_a_bad_body(self):
        """Both are caller errors that raise; the options are decided first, so
        the answer is the mismatch the item declared rather than a complaint
        about a body that was never going to be stored."""
        with pytest.raises(ContentTypeMismatch):
            _decide_batch(
                facts=JSON,
                items=[AppendOptions(content_type="text/plain")],
                payloads=[b"not json"],
            )

    def test_a_bad_body_is_raised_before_a_later_seq_conflict(self):
        """And in the other direction across items: the loop reaches item one's
        body before item two's options, exactly as a loop of `append` would."""
        with pytest.raises(InvalidJson):
            _decide_batch(
                facts=StreamFacts(content_type="application/json", last_seq="5"),
                items=[AppendOptions(), AppendOptions(seq="1")],
                payloads=[b"not json", b"{}"],
            )
