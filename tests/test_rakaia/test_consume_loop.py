"""The consume loop: poll, apply, record any outcome, commit the cursor.

ADR 0007 Decision 2. Three properties are the whole reason the loop exists, and
each of them is a defect somewhere in the tree that this closes:

* the outcome is written **outside** whatever transaction the apply used, so a
  failing batch cannot discard the record of its own failure (that one is proved
  against a real transaction in
  `tests/test_django_rakaia/test_consume_rollback.py` — nothing here has a
  transaction to roll back);
* the cursor is committed **last**, per message, so an event that was not applied
  is still pending rather than silently below the watermark;
* `on_error` is an explicit parameter, and the two modes do opposite things.

The tests are named for those properties rather than for the functions they call,
because what each one is defending is the property and not the call.
"""

from __future__ import annotations

import pytest

from rakaia.outcomes import InMemoryOutcomeStore, Outcome
from rakaia.store import StreamStore
from rakaia.subscription import consume
from rakaia.types import StreamMessage


def _store_with(path: str, payloads: list[bytes]) -> StreamStore:
    store = StreamStore()
    store.create(path)
    for payload in payloads:
        store.append(path, payload)
    return store


class _Recorder:
    """A commit sink that remembers the order it was called in.

    The order is the assertion in most of this file: "the cursor is committed
    last" is not observable from the final value, only from when it moved.
    """

    def __init__(self) -> None:
        self.committed: list[str] = []
        self.applied: list[bytes] = []

    def commit(self, offset: str) -> None:
        self.committed.append(offset)


class TestTheCursorIsCommittedLast:
    def test_the_cursor_does_not_advance_past_an_event_that_did_not_apply(self) -> None:
        """The gate for constraint 2, stated as the loss it prevents.

        Committing before applying is at-most-once, and under Decision 3 it is
        worse than losing the event: success writes no record, so an unapplied
        event below the cursor reads back as one that worked. The watermark must
        therefore stop *below* the message that raised.
        """
        store = _store_with("s", [b"a", b"b", b"c"])
        offsets = [m.offset for m in store.read("s")[0]]
        sink = _Recorder()
        outcomes = InMemoryOutcomeStore()

        def apply(message: StreamMessage) -> None:
            if message.data == b"b":
                raise RuntimeError("handler said no")
            sink.applied.append(message.data)

        result = consume(
            store,
            "s",
            apply,
            consumer="c",
            on_error="halt",
            commit=sink.commit,
            outcomes=outcomes,
        )

        # Only the first message was applied, and only its offset was committed.
        assert sink.applied == [b"a"]
        assert sink.committed == [offsets[0]]
        assert result.cursor == offsets[0]
        # The failing message and everything behind it are still pending, which
        # is what makes redelivery — not silent loss — the outcome of a crash.
        assert result.cursor != offsets[1]
        assert result.cursor != offsets[2]

    def test_a_pending_event_is_delivered_again_on_the_next_pass(self) -> None:
        """The other half of the same property: still pending means re-read."""
        store = _store_with("s", [b"a", b"b"])
        sink = _Recorder()
        failing = {b"b"}

        def apply(message: StreamMessage) -> None:
            if message.data in failing:
                raise RuntimeError("not yet")
            sink.applied.append(message.data)

        first = consume(
            store, "s", apply, consumer="c", on_error="halt", commit=sink.commit
        )
        failing.clear()
        second = consume(
            store,
            "s",
            apply,
            consumer="c",
            on_error="halt",
            cursor=first.cursor,
            commit=sink.commit,
        )

        assert sink.applied == [b"a", b"b"]
        assert second.applied == 1
        assert second.cursor == store.get_current_offset("s")

    def test_nothing_is_committed_when_the_first_message_fails(self) -> None:
        store = _store_with("s", [b"a"])
        sink = _Recorder()

        result = consume(
            store,
            "s",
            _always_raises,
            consumer="c",
            on_error="halt",
            commit=sink.commit,
        )

        assert sink.committed == []
        assert result.cursor is None


class TestOnErrorIsExplicit:
    def test_halt_stops_where_skip_continues(self) -> None:
        """The gate for constraint 3.

        One loop serves a live consumer and a rebuild, and they want opposite
        things from a poisoned event: a live stream must not stall behind one,
        and a rebuild that skips one has produced a projection that is not
        derived from every event while still reporting success. Same stream,
        same handler, one parameter apart.
        """
        payloads = [b"a", b"boom", b"c"]

        halted_sink = _Recorder()
        halted = consume(
            _store_with("s", payloads),
            "s",
            _fails_on_boom(halted_sink),
            consumer="c",
            on_error="halt",
            commit=halted_sink.commit,
        )

        skipped_sink = _Recorder()
        skipped = consume(
            _store_with("s", payloads),
            "s",
            _fails_on_boom(skipped_sink),
            consumer="c",
            on_error="skip",
            commit=skipped_sink.commit,
        )

        assert halted.halted is True
        assert halted_sink.applied == [b"a"]
        assert len(halted_sink.committed) == 1

        assert skipped.halted is False
        assert skipped_sink.applied == [b"a", b"c"]
        assert len(skipped_sink.committed) == 3

        # Both recorded the failure. The difference is what they did next, not
        # whether they noticed — a mode that stops quietly is no better than one
        # that continues quietly.
        assert len(halted.outcomes) == len(skipped.outcomes) == 1

    def test_skip_advances_past_the_event_it_recorded(self) -> None:
        """A skipped event is below the watermark, which is only safe because
        the outcome says what happened to it."""
        store = _store_with("s", [b"boom", b"c"])
        offsets = [m.offset for m in store.read("s")[0]]
        outcomes = InMemoryOutcomeStore()

        result = consume(
            store,
            "s",
            _fails_on_boom(_Recorder()),
            consumer="c",
            on_error="skip",
            outcomes=outcomes,
        )

        assert result.cursor == offsets[1]
        [recorded] = outcomes.latest("c", "s")
        assert recorded.offset == offsets[0]
        assert recorded.status == "failed"
        # In the log and not applied, so replay is the recovery — ADR 0007's
        # first table row.
        assert recorded.stage == "project"

    def test_on_error_has_no_default(self) -> None:
        """The parameter must be passed, not inferred.

        `on_drift` has no default for the same reason, and this is the assertion
        that keeps one from being added later as a convenience.
        """
        with pytest.raises(TypeError):
            consume(  # type: ignore[call-arg]
                _store_with("s", [b"a"]),
                "s",
                _always_raises,
                consumer="c",
            )


class TestAnApplyMayEmitOutcomesOfItsOwn:
    def test_outcomes_the_apply_returns_are_recorded(self) -> None:
        """A reducer computing a value from a population with a hole in it says
        so, and the loop is where that gets written.

        This is the loop's half of that decision and all of it: rakaia records
        what it is handed. Which construct decides a population has a hole, and
        how, is not this function's business.
        """
        store = _store_with("s", [b"a"])
        outcomes = InMemoryOutcomeStore()

        derived = Outcome(
            consumer="c",
            stream_path="s",
            subject="balance/suku-a",
            offset=None,
            sequence_key="balance/suku-a",
            stage="append",
            status="skipped",
            reasons=("incomplete_population",),
            params={"missing": "1"},
        )

        result = consume(
            store,
            "s",
            lambda _message: [derived],
            consumer="c",
            on_error="skip",
            outcomes=outcomes,
        )

        # Emitting an outcome is not failing: the message applied and the cursor
        # moved. A reducer reporting a hole has still done its work.
        assert result.applied == 1
        assert result.halted is False
        assert result.cursor == store.get_current_offset("s")
        assert result.outcomes == (derived,)
        assert outcomes.latest("c", "s") == [derived]


class TestTheLoopIsStillAPoll:
    def test_a_caught_up_pass_applies_and_commits_nothing(self) -> None:
        store = _store_with("s", [b"a"])
        sink = _Recorder()
        first = consume(
            store,
            "s",
            lambda m: sink.applied.append(m.data),
            consumer="c",
            on_error="skip",
            commit=sink.commit,
        )
        again = consume(
            store,
            "s",
            lambda m: sink.applied.append(m.data),
            consumer="c",
            on_error="skip",
            cursor=first.cursor,
            commit=sink.commit,
        )

        assert again.status == "caught_up"
        assert again.applied == 0
        assert sink.committed == [first.cursor]
        assert again.cursor == first.cursor

    def test_an_absent_stream_does_not_forget_the_watermark(self) -> None:
        """`poll` reports `cursor=None` for a stream that is not there. That is
        a fact about the poll, not an instruction to reset the consumer."""
        result = consume(
            StreamStore(),
            "gone",
            _always_raises,
            consumer="c",
            on_error="skip",
            cursor="000042",
        )

        assert result.status == "absent"
        assert result.cursor == "000042"

    def test_a_rewound_pass_is_reported_rather_than_absorbed(self) -> None:
        """The loop does not reset derived state for the caller — it cannot know
        what the derived state is — so `rewound` still has to reach them."""
        store = _store_with("s", [b"a", b"b"])
        result = consume(
            store,
            "s",
            lambda _m: None,
            consumer="c",
            on_error="skip",
            cursor="zzzzzzzz",
        )

        assert result.status == "rewound"
        assert result.applied == 2


def _always_raises(_message: StreamMessage) -> None:
    raise RuntimeError("boom")


def _fails_on_boom(sink: _Recorder):
    def apply(message: StreamMessage) -> None:
        if message.data == b"boom":
            raise RuntimeError("boom")
        sink.applied.append(message.data)

    return apply
