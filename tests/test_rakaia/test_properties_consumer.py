"""Stateful properties of a consumer over a stream that is rebuilt beneath it (GitHub #339).

Events are appended, the stream is rebuilt shorter, deleted and recreated, and a
consumer runs between them with a handler that sometimes fails. Whatever the
order, an event is applied once, or the consumer stopped and said the stream is
not the one it was reading; and a temporary error never moves the cursor.
"""

from __future__ import annotations

from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from rakaia.consumer import Consumer, InMemoryConsumerCursorStore
from rakaia.offsets import after
from rakaia.outcomes import InMemoryOutcomeStore
from rakaia.store import StreamStore

PATH = "s"
NAME = "c"
STOPPED = ("rewound", "replaced")


class _Blip(Exception):
    """A temporary failure, like a dropped connection."""


class _Bad(Exception):
    """A permanent failure: this event can never be applied."""


class ConsumerMachine(RuleBasedStateMachine):
    def __init__(self) -> None:
        super().__init__()
        self.store = StreamStore()
        self.store.create(PATH)
        self.present = True
        self.log: list[str] = []  # ids of the events now in the stream, in order
        self.counter = 0
        self.epoch = 0  # bumps whenever the stream a consumer read is replaced
        self._reset_consumer()

    def _reset_consumer(self) -> None:
        self.cursors = InMemoryConsumerCursorStore()
        self.outcomes = InMemoryOutcomeStore()
        self.applied: list[str] = []
        self.failed: set[str] = set()  # what the outcome store says failed
        self.bad: set[str] = set()  # what the handler really refused for good
        self.consumer_epoch = self.epoch
        self.stopped = False
        # A rebuild that kept the stream's stamp and left the head below the
        # consumer's cursor: the one way to reach `rewound`.
        self.rewound = False

    def _new_id(self) -> str:
        self.counter += 1
        return f"e{self.counter}"

    @rule()
    def append(self) -> None:
        # Past the cursor again the rewind could no longer be told from progress.
        if self.present and not self.rewound:
            event_id = self._new_id()
            self.store.append(PATH, event_id.encode())
            self.log.append(event_id)

    @rule(keep=st.integers(0, 2))
    def rebuild_shorter(self, keep: int) -> None:
        """The log is rebuilt from scratch and comes back with fewer events.

        A rebuild that leaves the head below the consumer's cursor keeps the old
        stamp, which is how the stream looks when it is truncated rather than
        recreated: that is `rewound`. Any other rebuild is a new stream.
        """
        old_stamp = self.store.get_generation(PATH) if self.present else None
        self.store = StreamStore()
        self.store.create(PATH)
        self.present = True
        self.log = []
        for _ in range(keep):
            event_id = self._new_id()
            self.store.append(PATH, event_id.encode())
            self.log.append(event_id)
        cursor = self.cursors.load(NAME, PATH)
        head = self.store.get_current_offset(PATH)
        if (
            old_stamp is not None
            and cursor is not None
            and head is not None
            and self.epoch == self.consumer_epoch
            and after(cursor, head)
        ):
            self.store._streams[PATH].generation = old_stamp
            self.rewound = True
        else:
            self.epoch += 1
            self.rewound = False

    @rule()
    def delete(self) -> None:
        """The stream is deleted, or expires and is reaped: the same to a reader."""
        if self.present:
            self.store.delete(PATH)
            self.present = False
            self.rewound = False
            self.epoch += 1
            self.log = []

    @rule()
    def create(self) -> None:
        if not self.present:
            self.store.create(PATH)
            self.present = True

    @rule()
    def rebuild_consumer(self) -> None:
        """The operator's answer to a stop: throw the consumer's state away."""
        self._reset_consumer()

    @rule(plan=st.lists(st.sampled_from(["ok", "ok", "blip", "bad"]), max_size=6))
    def consume(self, plan: list[str]) -> None:
        script = list(plan)

        def apply(message):
            step = script.pop(0) if script else "ok"
            if step == "blip":
                raise _Blip("connection dropped")
            if step == "bad":
                self.bad.add(message.data.decode())
                raise _Bad("cannot apply")
            self.applied.append(message.data.decode())

        before = self.cursors.load(NAME, PATH)
        consumer = Consumer(
            store=self.store,
            path=PATH,
            name=NAME,
            cursors=self.cursors,
            outcomes=self.outcomes,
            transient=(_Blip,),
        )
        result = consumer.run(apply, on_error="skip")

        for outcome in result.outcomes:
            if outcome.status == "failed":
                self.failed.add(
                    next(
                        m.data.decode()
                        for m in self._read()
                        if m.offset == outcome.offset
                    )
                )

        if result.status == "absent":
            return

        stream_replaced = before is not None and self.epoch != self.consumer_epoch
        # Same stamp, head below the cursor: the log was cut back, not replaced.
        stream_rewound = self.rewound and before is not None and not stream_replaced
        if result.status in STOPPED:
            assert stream_replaced or stream_rewound, (
                "told the stream was replaced when it was not"
            )
            assert result.status == ("rewound" if stream_rewound else "replaced")
            assert result.applied == 0, (
                "applied events from a stream it was told is not its own"
            )
            assert self.cursors.load(NAME, PATH) == before, (
                "moved its cursor when it stopped"
            )
            self.stopped = True
            return
        assert not (stream_replaced or stream_rewound), (
            f"carried on ({result.status}) over a replaced stream; the log is {self.log}"
        )
        self.consumer_epoch = self.epoch

        cursor = self.cursors.load(NAME, PATH)
        done = set(self.applied) | self.failed
        messages = self._read()
        if cursor is not None:
            upto = [m.offset for m in messages].index(cursor)
            skipped = [
                m.data.decode()
                for m in messages[: upto + 1]
                if m.data.decode() not in done
            ]
            assert not skipped, f"cursor passed events never applied: {skipped}"
        if not getattr(result, "transient", False):
            missing = [e for e in self.log if e not in done]
            assert not missing, f"a finished pass left events unapplied: {missing}"

    def _read(self):
        return self.store.read(PATH)[0] if self.present else []

    @invariant()
    def nothing_is_applied_twice(self) -> None:
        assert len(self.applied) == len(set(self.applied)), self.applied

    @invariant()
    def a_failed_event_was_not_also_applied(self) -> None:
        assert not (self.failed & set(self.applied))

    @invariant()
    def only_a_permanent_error_is_recorded_as_a_failure(self) -> None:
        assert self.failed <= self.bad, (
            f"a temporary error was written down as a failure: {self.failed - self.bad}"
        )


ConsumerMachine.TestCase.settings = settings(
    max_examples=200,
    stateful_step_count=30,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

TestConsumer = ConsumerMachine.TestCase


def test_a_log_cut_back_under_the_same_stamp_is_rewound_not_re_applied() -> None:
    """The property above reaches this by chance; this reaches it every time."""
    machine = ConsumerMachine()
    for _ in range(3):
        machine.append()
    machine.consume([])
    machine.rebuild_shorter(1)
    assert machine.rewound, "the rebuild should have left the head below the cursor"
    machine.consume([])
    assert machine.stopped
    assert machine.applied == ["e1", "e2", "e3"]
