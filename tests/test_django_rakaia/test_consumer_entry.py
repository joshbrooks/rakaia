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

import json

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


@pytest.mark.django_db(transaction=True)
class TestNamingWhatTheLoopRecords:
    """A caller can say what a record is about, and what it is ordered within.

    The loop has always accepted both; the entry point did not pass them on, so a
    record written *for* a consumer named the event's position while a record
    written *by* it named the row. That is two kinds of thing in one column, on
    the screen where the comparison is the whole point (#272).
    """

    @staticmethod
    def _payloads() -> list[bytes]:
        return [b'{"row": "Fatuberliu/WATER", "form": "prog-2026-01"}']

    def test_a_record_is_named_by_the_caller(self) -> None:
        consumer = django_consumer(
            _store_with("submissions", self._payloads()),
            "submissions",
            "reporting",
            subject_of=lambda message: json.loads(message.data)["row"],
        )

        def apply(_message: StreamMessage) -> None:
            raise ValueError("no")

        consumer.run(apply, on_error="skip")

        (record,) = DjangoOutcomeStore().latest("reporting", "submissions")
        assert record.subject == "Fatuberliu/WATER"
        # The position is still recorded — it is how a replay finds the event.
        assert record.offset is not None

    def test_the_sequence_a_record_belongs_to_is_the_callers_too(self) -> None:
        consumer = django_consumer(
            _store_with("submissions", self._payloads()),
            "submissions",
            "reporting",
            subject_of=lambda message: json.loads(message.data)["row"],
            sequence_of=lambda message: json.loads(message.data)["form"],
        )

        def apply(_message: StreamMessage) -> None:
            raise ValueError("no")

        consumer.run(apply, on_error="skip")

        (record,) = DjangoOutcomeStore().latest("reporting", "submissions")
        assert record.sequence_key == "prog-2026-01"

    def test_the_sequence_defaults_to_the_subject(self) -> None:
        """The other half of saying nothing, and it was asserted by nothing.

        Review mutated `sequence_of = sequence_of or subject_of` in the loop to a
        constant and the whole suite stayed green: the docstring promised the
        default and no test read it. Passing only `subject_of` is the case that
        can see it.
        """
        consumer = django_consumer(
            _store_with("submissions", self._payloads()),
            "submissions",
            "reporting",
            subject_of=lambda message: json.loads(message.data)["row"],
        )

        def apply(_message: StreamMessage) -> None:
            raise ValueError("no")

        consumer.run(apply, on_error="skip")

        (record,) = DjangoOutcomeStore().latest("reporting", "submissions")
        assert record.sequence_key == record.subject == "Fatuberliu/WATER"

    def test_saying_nothing_still_names_the_position(self) -> None:
        """The default is unchanged, and honest: every event this loop sees is
        already in the log, so its position is a name it always has."""
        consumer = django_consumer(
            _store_with("submissions", self._payloads()),
            "submissions",
            "reporting",
        )

        def apply(_message: StreamMessage) -> None:
            raise ValueError("no")

        consumer.run(apply, on_error="skip")

        (record,) = DjangoOutcomeStore().latest("reporting", "submissions")
        assert record.subject == record.offset
