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

from django_rakaia.consumer import (
    CallerTransactionOpen,
    DjangoConsumer,
    DjangoConsumerCursorStore,
    django_consumer,
)
from django_rakaia.models import ConsumerCursor, ConsumerOutcome
from django_rakaia.outcomes import DjangoOutcomeStore
from rakaia.consumer import InMemoryConsumerCursorStore
from rakaia.outcomes import InMemoryOutcomeStore
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


@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
class TestTheAliasIsAskedOfTheStores:
    """The guard checks the connection the rows are on, not a named default.

    Both tests here need ``databases`` to list ``overlay`` as well: without it
    pytest-django never sets that connection up, and a test that means to write
    there errors instead of failing on what it is about.
    """

    def test_an_alias_sends_both_rows_to_that_database(self) -> None:
        """The `using=` seam, which nothing exercised: strip either `.using()`
        from the cursor helpers and this fails, because the row lands on
        ``default`` where it is not looked for."""
        consumer = django_consumer(
            _store_with("submissions", [b'{"a": 1}']),
            "submissions",
            "reporting",
            using="overlay",
        )

        def apply(_message: StreamMessage) -> None:
            raise ValueError("no")

        consumer.run(apply, on_error="skip")

        assert ConsumerCursor.objects.using("overlay").count() == 1
        assert ConsumerOutcome.objects.using("overlay").count() == 1
        assert ConsumerCursor.objects.using("default").count() == 0
        assert ConsumerOutcome.objects.using("default").count() == 0

    def test_a_transaction_on_the_stores_alias_is_refused(self) -> None:
        """The case a named alias field got wrong: a consumer whose rows are on
        ``overlay``, run inside a transaction on ``overlay``. Guarding a
        remembered ``default`` here passes the run and loses both rows."""
        consumer = django_consumer(
            _store_with("submissions", [b'{"a": 1}']),
            "submissions",
            "reporting",
            using="overlay",
        )

        def apply(_message: StreamMessage) -> None:
            raise ValueError("no")

        with (
            transaction.atomic(using="overlay"),
            pytest.raises(CallerTransactionOpen),
        ):
            consumer.run(apply, on_error="skip")

    def test_a_transaction_elsewhere_is_not_this_consumers_problem(self) -> None:
        """The other side of it: a transaction open on a connection none of this
        consumer's rows are written on cannot roll them back, so refusing would
        be a false alarm."""
        consumer = django_consumer(
            _store_with("submissions", [b'{"a": 1}']),
            "submissions",
            "reporting",
            using="overlay",
        )

        def apply(_message: StreamMessage) -> None:
            raise ValueError("no")

        with transaction.atomic(using="default"):
            consumer.run(apply, on_error="skip")

        assert ConsumerOutcome.objects.using("overlay").count() == 1

    def test_stores_built_by_hand_are_still_covered(self) -> None:
        """`DjangoConsumer` is exported, so it can be built without the factory.
        The alias is derived from the stores it was given, so there is no fourth
        place to name a database and nothing to keep in step."""
        consumer = DjangoConsumer(
            store=_store_with("submissions", [b'{"a": 1}']),
            path="submissions",
            name="reporting",
            cursors=DjangoConsumerCursorStore(using="overlay"),
            outcomes=DjangoOutcomeStore(using="overlay"),
        )

        def apply(_message: StreamMessage) -> None:
            raise ValueError("no")

        with (
            transaction.atomic(using="overlay"),
            pytest.raises(CallerTransactionOpen),
        ):
            consumer.run(apply, on_error="skip")

    def test_a_store_that_names_no_database_is_not_guarded(self) -> None:
        """A store with no alias is not database-backed, so no transaction of the
        caller's can roll back what it keeps. Treating "names no alias" as "the
        default alias" would refuse a run that was never at risk."""
        consumer = DjangoConsumer(
            store=_store_with("submissions", [b'{"a": 1}']),
            path="submissions",
            name="reporting",
            cursors=InMemoryConsumerCursorStore(),
            outcomes=InMemoryOutcomeStore(),
        )

        def apply(_message: StreamMessage) -> None:
            raise ValueError("no")

        with transaction.atomic():
            result = consumer.run(apply, on_error="skip")

        assert len(result.outcomes) == 1
