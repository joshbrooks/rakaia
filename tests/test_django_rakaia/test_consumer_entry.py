"""The Django consumer refuses to start inside the caller's transaction.

ADR 0007 keeps the outcome out of the executor's transaction, and then says what
it cannot do: one frame further out, a caller that wraps the whole run in
``atomic()`` and rolls back takes the record with it. Measured on the database
store at 0 of 1 outcomes surviving, and the core loop cannot see it — `rakaia` is
stdlib-only and knows nothing about an atomic block.

The Django entry point can see it, so it checks. Both tests here need
``transaction=True``: under a plain ``django_db`` marker pytest-django has
already opened the transaction the guard exists to detect, so the refusal fires
on every test and the green case cannot be written at all — the #180 failure mode
in `CLAUDE.md`, arriving from the other side.
"""

from __future__ import annotations

import pytest
from django.db import transaction

from django_rakaia.consumer import CallerTransactionOpen, django_consumer
from django_rakaia.models import ConsumerCursor, ConsumerOutcome
from rakaia.store import StreamStore
from rakaia.types import StreamMessage


def _store_with(path: str, payloads: list[bytes]) -> StreamStore:
    store = StreamStore()
    store.create(path)
    for payload in payloads:
        store.append(path, payload)
    return store


@pytest.mark.django_db(transaction=True)
class TestTheCallersTransactionIsRefused:
    def test_running_inside_an_open_transaction_raises(self) -> None:
        consumer = django_consumer(
            _store_with("submissions", [b'{"a": 1}']),
            "submissions",
            "reporting",
        )

        def apply(_message: StreamMessage) -> None:
            raise ValueError("no")

        with transaction.atomic(), pytest.raises(CallerTransactionOpen):
            consumer.run(apply, on_error="skip")

        assert ConsumerOutcome.objects.count() == 0

    def test_running_outside_one_records_and_commits(self) -> None:
        """The other half of the guard: the ordinary path still works, and it
        writes both rows without either being asked for."""
        consumer = django_consumer(
            _store_with("submissions", [b'{"a": 1}']),
            "submissions",
            "reporting",
        )

        def apply(_message: StreamMessage) -> None:
            raise ValueError("no")

        consumer.run(apply, on_error="skip")

        assert ConsumerOutcome.objects.count() == 1
        assert ConsumerCursor.objects.filter(
            consumer_id="reporting", stream_path="submissions"
        ).exists()
