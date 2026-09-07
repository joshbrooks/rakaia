"""A consumer is a thing you hold, not five arguments you repeat.

`consume` takes the store, the path, the consumer name, a cursor loader, a commit
sink and an outcome store on every call, and two of those are optional in a way
that makes the wrong wiring look right: omit ``outcomes`` and the loop runs, the
cursor advances and nothing is ever recorded. ADR 0007's whole claim is that
absence of a record means success, so a consumer that silently records nothing is
a consumer that reports a clean stream it never checked.

`Consumer` holds the five together and asks for the outcome store by
construction. The tests below are named for the two properties that buys:
recording cannot be forgotten, and the name and path are said once.
"""

from __future__ import annotations

import pytest

from rakaia.consumer import Consumer, InMemoryCursorLedger
from rakaia.outcomes import InMemoryOutcomeStore
from rakaia.store import StreamStore
from rakaia.types import StreamMessage


def _store_with(path: str, payloads: list[bytes]) -> StreamStore:
    store = StreamStore()
    store.create(path)
    for payload in payloads:
        store.append(path, payload)
    return store


class TestRecordingIsOnByConstruction:
    def test_a_consumer_without_an_outcome_store_cannot_be_built(self) -> None:
        """The defect this module exists for: omitting the store is currently a
        working-looking consumer that records nothing."""
        with pytest.raises(TypeError):
            Consumer(  # type: ignore[call-arg]
                store=_store_with("s", [b'{"a": 1}']),
                path="s",
                name="reporting",
                cursors=InMemoryCursorLedger(),
            )

    def test_a_failed_apply_is_recorded_without_asking_for_it(self) -> None:
        outcomes = InMemoryOutcomeStore()
        consumer = Consumer(
            store=_store_with("s", [b'{"a": 1}']),
            path="s",
            name="reporting",
            cursors=InMemoryCursorLedger(),
            outcomes=outcomes,
        )

        def apply(_message: StreamMessage) -> None:
            raise ValueError("no")

        consumer.run(apply, on_error="skip")

        assert [o.status for o in outcomes.latest("reporting", "s")] == ["failed"]


class TestTheNameAndPathAreSaidOnce:
    def test_they_reach_the_cursor_ledger_and_the_outcome_alike(self) -> None:
        ledger = InMemoryCursorLedger()
        outcomes = InMemoryOutcomeStore()
        consumer = Consumer(
            store=_store_with("submissions", [b'{"a": 1}']),
            path="submissions",
            name="reporting",
            cursors=ledger,
            outcomes=outcomes,
        )

        def apply(_message: StreamMessage) -> None:
            raise ValueError("no")

        consumer.run(apply, on_error="skip")

        recorded = outcomes.latest("reporting", "submissions")
        assert [(o.consumer, o.stream_path) for o in recorded] == [
            ("reporting", "submissions")
        ]
        assert ledger.load("reporting", "submissions") is not None

    def test_a_second_run_resumes_from_the_committed_cursor(self) -> None:
        """The ledger is loaded as well as committed, or a consumer re-applies
        everything it already applied on every run."""
        store = _store_with("submissions", [b'{"a": 1}', b'{"a": 2}'])
        seen: list[bytes] = []
        consumer = Consumer(
            store=store,
            path="submissions",
            name="reporting",
            cursors=InMemoryCursorLedger(),
            outcomes=InMemoryOutcomeStore(),
        )
        consumer.run(lambda message: seen.append(message.data), on_error="skip")
        consumer.run(lambda message: seen.append(message.data), on_error="skip")

        assert len(seen) == 2
