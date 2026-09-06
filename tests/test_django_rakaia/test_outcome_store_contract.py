"""The database-backed outcome store against the shared contract.

The third run of the same suite, and the first one where the storage has
constraints of its own. ADR 0007 records why that matters: the codec settles what
a value *is* and says nothing about what a column will accept, so a bounded
column is the one divergence the shared rendering cannot reach. Run this under
``RAKAIA_TEST_DB=postgres`` (`just test-pg`) as well as on the default SQLite —
SQLite enforces almost nothing about column contents, so a run there cannot tell
a store that keeps a hostile name from one the database would have refused.
"""

from __future__ import annotations

import pytest

from django_rakaia.outcomes import DjangoOutcomeStore
from rakaia.outcomes import Outcome
from tests.outcome_store_contract import OutcomeStoreContract, project


def an_outcome(**kw) -> Outcome:
    """A projection-stage outcome with every name overridable."""
    base = {
        "consumer": "c",
        "stream_path": "s",
        "subject": "row-1",
        "offset": "0000000001",
        "sequence_key": "seq",
        "stage": "project",
        "status": "failed",
    }
    return Outcome(**{**base, **kw})


@pytest.mark.django_db
class TestDjangoOutcomeStoreContract(OutcomeStoreContract):
    @pytest.fixture
    def outcomes(self) -> DjangoOutcomeStore:
        return DjangoOutcomeStore()


@pytest.mark.django_db
class TestDjangoOutcomeStoreDurability:
    """What the shared contract cannot ask for, because the in-memory reference
    cannot satisfy it."""

    def test_outcomes_survive_a_new_store_over_the_same_database(self):
        DjangoOutcomeStore().record(project("0000000001", reasons=("bad_total",)))
        [got] = DjangoOutcomeStore().latest("c", "s")
        assert got.reasons == ("bad_total",)

    def test_the_row_holds_the_bytes_encode_produced(self):
        """Decision 6b, pinned as byte-identity rather than round-trippability.

        This used to assert `decode(row.payload) == outcome`, which is a weaker
        claim than it reads as: a hand-rolled `json.dumps` of the same ten fields
        decodes to the same outcome, so replacing `encode(outcome)` with one left
        the whole suite green. Decision 6b is not "the store can rebuild an
        equal outcome", it is "every store keeps the *same text*" — and only an
        equality on the text says so.
        """
        from django_rakaia.models import ConsumerOutcome
        from rakaia.outcomes import encode

        outcome = project("0000000001", reasons=("bad_total",), params={"row": "3"})
        DjangoOutcomeStore().record(outcome)

        [row] = ConsumerOutcome.objects.all()
        assert row.payload == encode(outcome)

    def test_a_second_attempt_is_a_new_row_not_an_update(self):
        """Append-only (Decision 6a), pinned below `latest`.

        `latest` collapses attempts, so an in-place overwrite is invisible through
        it — the shared contract says so itself. Counting rows is the only place
        the property is observable, and this is the backend where a well-meaning
        `update_or_create` would be the obvious thing to write.
        """
        from django_rakaia.models import ConsumerOutcome

        store = DjangoOutcomeStore()
        store.record(project("0000000001", reasons=("first",), attempt=1))
        store.record(project("0000000001", reasons=("second",), attempt=2))

        assert ConsumerOutcome.objects.count() == 2


@pytest.mark.django_db
class TestTheKeyColumnsAreADerivedIndex:
    """The columns are `quote(value)` cut to the width, not the value.

    Every case here fails on Postgres, and several pass on SQLite, if the columns
    ever go back to holding raw values — which is exactly why they are here rather
    than left to a review round.
    """

    def test_a_key_column_holds_the_quoted_form_not_the_value(self):
        """The whole design in one assertion.

        Without this, reverting to raw columns leaves every behavioural test green
        on SQLite: the values round-trip through the payload either way, and the
        difference only shows up on a database that objects to a byte.
        """
        from django_rakaia.models import ConsumerOutcome

        DjangoOutcomeStore().record(
            an_outcome(consumer="a/b", stream_path="submission/tf611")
        )
        [row] = ConsumerOutcome.objects.all()
        assert (row.consumer_key, row.stream_path_key) == (
            "a%2Fb",
            "submission%2Ftf611",
        )

    @pytest.mark.parametrize("field", ["consumer", "stream_path", "subject", "offset"])
    def test_a_nul_byte_in_any_name_round_trips(self, field: str):
        """Postgres refuses NUL in a text column outright; SQLite keeps it.

        The divergence that reopened one character wide after the length check
        closed it 256 characters long. `quote` maps NUL to `%00`, so neither
        database ever sees the byte and both backends agree — the same reason the
        file-backed store is safe.
        """
        hostile = "row\x001"
        outcome = an_outcome(**{field: hostile})
        store = DjangoOutcomeStore()
        store.record(outcome)

        [got] = store.latest(outcome.consumer, outcome.stream_path)
        assert getattr(got, field) == hostile

    def test_an_attempt_too_large_for_an_integer_column_round_trips(self):
        """`attempt` was a `PositiveIntegerField`, which is `int4`.

        `2**31` was kept by SQLite and refused by Postgres with "integer out of
        range" — the same class as NUL, arriving through a different property.
        It is not a column any more; `latest` reads it from the payload, so
        there is no range for it to leave.
        """
        store = DjangoOutcomeStore()
        store.record(an_outcome(attempt=2**31))
        [got] = store.latest("c", "s")
        assert got.attempt == 2**31

    @pytest.mark.parametrize(
        ("field", "limit"),
        [("consumer", 128), ("stream_path", 255), ("subject", 255), ("offset", 64)],
    )
    def test_a_name_far_longer_than_its_column_round_trips(
        self, field: str, limit: int
    ):
        """No length is refused any more, and this is the case that says so.

        The column is a prefix of an index, so a name ten times its width is cut
        for indexing and kept whole in the payload. The previous design raised a
        `ValueError` here.
        """
        long_name = "x" * (limit * 10)
        outcome = an_outcome(**{field: long_name})
        store = DjangoOutcomeStore()
        store.record(outcome)

        [got] = store.latest(outcome.consumer, outcome.stream_path)
        assert getattr(got, field) == long_name

    def test_two_names_sharing_a_cut_key_do_not_leak_into_each_other(self):
        """What makes cutting safe, stated rather than assumed.

        Both consumers encode to the same 128-character key, so the query returns
        both rows and only the payload comparison tells them apart. Mutation:
        drop that comparison from `latest`; each consumer sees the other's
        outcome.
        """
        shared = "x" * 200
        store = DjangoOutcomeStore()
        store.record(an_outcome(consumer=shared + "-one", reasons=("mine",)))
        store.record(an_outcome(consumer=shared + "-two", reasons=("theirs",)))

        assert [o.reasons for o in store.latest(shared + "-one", "s")] == [("mine",)]
        assert [o.reasons for o in store.latest(shared + "-two", "s")] == [("theirs",)]

    def test_record_refuses_nothing_a_caller_can_construct(self):
        """Totality, as one case, because `consume` depends on it.

        The consume loop being built alongside this (#248) records from inside
        its own `except` handler, so a store that raises there converts one
        poisoned event into a stopped stream — the opposite of what skipping a
        bad event promises. Every other `OutcomeStore` is total; this pins that
        this one is too, rather than leaving that loop to grow a guard for the
        odd backend out.
        """
        hostile = "\x00" + "\U0001f600" * 500 + "%2F../\r\n\t" + "\x7f"
        store = DjangoOutcomeStore()
        store.record(
            Outcome(
                consumer=hostile,
                stream_path=hostile,
                subject=hostile,
                offset=None,
                sequence_key=hostile,
                stage="append",
                status="refused",
                reasons=(hostile,),
                params={hostile: hostile},
                attempt=2**40,
            )
        )
        [got] = store.latest(hostile, hostile)
        assert got.subject == hostile and got.attempt == 2**40


@pytest.mark.django_db
class TestTheKeysNarrowAndThePayloadDecides:
    """Both directions of the disagreement, because neither is symmetrical.

    An earlier comment here said "the payload wins", which was only true one way
    round: a row the keys admit can still be rejected by its payload, but a row
    the keys exclude is never fetched to be judged. The rule is that the keys
    locate candidates and the payload decides among them, and a row whose keys do
    not match is simply not a candidate.
    """

    def test_a_row_whose_payload_names_another_scope_is_not_reported(self):
        """The direction the payload decides. Mutation: drop the payload
        comparison from `latest`; `row-other` appears."""
        from django_rakaia.models import ConsumerOutcome
        from rakaia.outcomes import encode

        DjangoOutcomeStore().record(project("0000000001", reasons=("mine",)))
        ConsumerOutcome.objects.create(
            consumer_key="c",
            stream_path_key="s",
            subject_key="row-other",
            offset_key="0000000002",
            payload=encode(project("0000000002", consumer="other")),
        )

        assert [o.subject for o in DjangoOutcomeStore().latest("c", "s")] == [
            "0000000001"
        ]

    def test_a_row_whose_keys_name_another_scope_is_never_fetched(self):
        """The direction the keys decide, and the one nothing pinned.

        A row filed under the wrong key is unreachable however good its payload
        is. That is a property of an index, not a bug — but it is the reason the
        keys must be derived by `record` and never written by hand, so it is
        stated here rather than left as a surprise.
        """
        from django_rakaia.models import ConsumerOutcome
        from rakaia.outcomes import encode

        ConsumerOutcome.objects.create(
            consumer_key="somewhere-else",
            stream_path_key="s",
            subject_key="row-1",
            offset_key="0000000001",
            payload=encode(project("0000000001", reasons=("orphaned",))),
        )

        assert DjangoOutcomeStore().latest("c", "s") == []


@pytest.mark.django_db(databases=["default", "overlay"], transaction=True)
class TestWhatSurvivesTheCallersOwnTransaction:
    """The hole ADR 0007 closes one level in, reopening one level out.

    The decision keeps an outcome out of the *executor's* transaction, so a batch
    that fails cannot discard the record of its own failure. It says nothing about
    the caller. A consumer that wraps its whole consume in `transaction.atomic()`
    and then rolls back takes the outcome with it, because by default the write
    joins the transaction already open — and the core consume loop cannot see
    this, since it is stdlib-only and has no idea Django is underneath.

    Both numbers are here rather than one, because the number a reader wants is
    the difference: on the ambient connection nothing survives, and on an alias
    the caller's transaction does not cover, the record does.
    """

    def test_an_outcome_on_the_ambient_connection_rolls_back_with_the_caller(self):
        from django.db import transaction

        from django_rakaia.models import ConsumerOutcome

        store = DjangoOutcomeStore()
        with transaction.atomic():
            store.record(project("0000000001", reasons=("bad_total",)))
            assert ConsumerOutcome.objects.count() == 1, "written inside the block"
            transaction.set_rollback(True)

        assert ConsumerOutcome.objects.count() == 0
        assert store.latest("c", "s") == []

    def test_an_outcome_on_another_alias_survives_the_callers_rollback(self):
        """`using=` is the escape hatch, and this is what it buys.

        The alias is a connection the caller's `atomic()` does not cover, so the
        record commits on its own and is still there afterwards. Mutation: drop
        `.using(self._using)` from `record`; the write lands on `default`, goes
        back with the rollback, and this reads zero.
        """
        from django.db import transaction

        from django_rakaia.models import ConsumerOutcome

        store = DjangoOutcomeStore(using="overlay")
        try:
            with transaction.atomic():
                store.record(project("0000000001", reasons=("bad_total",)))
                transaction.set_rollback(True)

            assert ConsumerOutcome.objects.using("default").count() == 0
            assert [o.reasons for o in store.latest("c", "s")] == [("bad_total",)]
        finally:
            # `transaction=True` truncates the aliases this test declares, but a
            # row committed on `overlay` outside the harness's control is exactly
            # the kind that outlives its test. Clean it up here rather than leave
            # the next test to discover it.
            ConsumerOutcome.objects.using("overlay").all().delete()
