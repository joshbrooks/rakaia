"""The outcome survives the executor's rollback.

This is the test that would have caught the original design error, and it is why
ADR 0007 exists at all. The version of this feature that looks right first —
thread the event's identity through `Effect` and let the executor write the
record — writes that record inside `DjangoExecutor.apply`'s
``transaction.atomic``. A batch that raises therefore discards the record of its
own failure, and the stream reads back as clean.

Nothing in the core test file can prove that, because nothing there has a
transaction to roll back: an `InMemoryOutcomeStore` survives anything. So the
outcome store here writes an actual row through the ORM, in the same connection
and the same test transaction as the projection the executor is trying to write.
Both tests below use it. They differ in one thing — *where* the record is
written — and that is the whole finding.
"""

from __future__ import annotations

from decimal import Decimal
from uuid import uuid4

import pytest
from django.db import transaction

from django_rakaia.effect_executor import DjangoExecutor
from rakaia.effects import Upsert
from rakaia.outcomes import Outcome, decode, encode
from rakaia.store import StreamStore
from rakaia.subscription import consume
from rakaia.types import StreamMessage

from .models import Alert, Measure

pytestmark = pytest.mark.django_db


class DatabaseOutcomeStore:
    """An `OutcomeStore` that writes a real row, so a rollback can take it away.

    Deliberately not a durable backend anyone should use — ADR 0007 leaves the
    Django one unbuilt — and deliberately a table this suite already has, so the
    test needs no migration of its own. What it has to be is *transactional*,
    which is the one property the two reference stores do not have.
    """

    def __init__(self) -> None:
        self._n = 0

    def record(self, outcome: Outcome) -> None:
        self._n += 1
        Alert.objects.create(
            stream_key="outcome",
            alert_type=f"outcome-{self._n}",
            message=encode(outcome),
        )

    def latest(self, consumer: str, stream_path: str) -> list[Outcome]:
        found = []
        for row in Alert.objects.filter(stream_key="outcome"):
            outcome = decode(row.message)
            if (
                outcome is not None
                and outcome.consumer == consumer
                and outcome.stream_path == stream_path
            ):
                found.append(outcome)
        return found


def _one_message_stream() -> StreamStore:
    store = StreamStore()
    store.create("s")
    store.append("s", b"the batch that fails")
    return store


def _a_batch_that_writes_a_row_and_then_raises(executor: DjangoExecutor):
    """A handler whose batch is half-good: the first effect writes, the second
    is unwritable, and `transaction.atomic` discards them together."""

    def apply(_message: StreamMessage) -> None:
        executor.apply(
            [
                Upsert(
                    "test_django_rakaia.Measure",
                    lookup={"ref": uuid4()},
                    values={"amount": Decimal("1.00")},
                ),
                Upsert(
                    "test_django_rakaia.Measure",
                    lookup={"ref": uuid4()},
                    values={"no_such_column": 1},
                ),
            ]
        )

    return apply


def test_the_outcome_survives_the_batch_it_records() -> None:
    """The gate. The batch is discarded; the record of its failure is not.

    Assert both halves in one test, on purpose. Either alone passes for the
    wrong reason — a record that survives because the batch also survived says
    nothing, and a discarded batch with no record is the defect.
    """
    store = _one_message_stream()
    outcomes = DatabaseOutcomeStore()
    committed: list[str] = []

    result = consume(
        store,
        "s",
        _a_batch_that_writes_a_row_and_then_raises(DjangoExecutor()),
        consumer="c",
        on_error="skip",
        commit=committed.append,
        outcomes=outcomes,
    )

    # The batch is gone — including the effect that had already succeeded.
    assert Measure.objects.count() == 0
    # The record is not.
    [recorded] = outcomes.latest("c", "s")
    assert recorded.status == "failed"
    assert recorded.stage == "project"
    assert recorded.offset == store.get_current_offset("s")
    assert result.outcomes == (recorded,)


def test_recording_inside_the_executor_loses_the_record() -> None:
    """Why the loop owns the write, shown rather than asserted.

    Identical to the test above but for one line: the record is written from
    inside the executor's transaction, which is what threading event identity
    through `Effect` would have made unavoidable. The batch and its record go
    together, and the stream is left looking clean.
    """
    store = _one_message_stream()
    outcomes = DatabaseOutcomeStore()
    executor = DjangoExecutor()

    def apply(message: StreamMessage) -> None:
        try:
            with transaction.atomic():
                outcomes.record(
                    Outcome(
                        consumer="c",
                        stream_path="s",
                        subject=message.offset,
                        offset=message.offset,
                        sequence_key=message.offset,
                        stage="project",
                        status="failed",
                    )
                )
                _a_batch_that_writes_a_row_and_then_raises(executor)(message)
        except Exception:
            pass

    consume(
        store,
        "s",
        apply,
        consumer="c",
        on_error="skip",
        outcomes=None,
    )

    assert Measure.objects.count() == 0
    assert outcomes.latest("c", "s") == []
