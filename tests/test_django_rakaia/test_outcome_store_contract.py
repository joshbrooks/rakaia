"""The database-backed outcome store against the shared contract.

The third run of the same suite, and the first one where the storage has
constraints of its own. ADR 0007 records why that matters: the codec settles what
a value *is* and says nothing about how long it may be, so a bounded column is
the one divergence the shared rendering cannot reach. Run this under
``RAKAIA_TEST_DB=postgres`` (`just test-pg`) as well as on the default SQLite —
SQLite does not enforce ``max_length``, so a run there cannot tell a store that
refuses an over-long name from one that quietly keeps it.
"""

from __future__ import annotations

import pytest

from django_rakaia.outcomes import DjangoOutcomeStore
from rakaia.outcomes import Outcome
from tests.outcome_store_contract import OutcomeStoreContract, project, refused


def an_outcome(**kw) -> Outcome:
    """A projection-stage outcome with every bounded name overridable."""
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

    def test_the_row_keeps_the_encoded_text_not_a_column_per_field(self):
        """Decision 6b, pinned where it can actually be undone.

        The columns are an index over the record, not the record. A future change
        that spread `reasons` and `params` across columns of their own would give
        this backend a second opinion about what a stored outcome looks like —
        which is the disagreement the shared codec exists to prevent — and every
        contract case would stay green, because they all read back through
        `latest`.
        """
        from django_rakaia.models import ConsumerOutcome
        from rakaia.outcomes import decode

        outcome = project("0000000001", reasons=("bad_total",), params={"row": "3"})
        DjangoOutcomeStore().record(outcome)

        [row] = ConsumerOutcome.objects.all()
        assert isinstance(row.payload, str)
        assert decode(row.payload) == outcome

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

    def test_a_row_whose_columns_disagree_with_its_payload_is_not_reported(self):
        """The payload is the record; the columns are an index over it.

        The file-backed store holds the same rule for the same reason — there,
        two consumers can share a file on a case-folding filesystem. Here the
        columns can be written by anything with SQL access, and a row filed under
        the wrong consumer must not be handed to one it is not about.
        """
        from django_rakaia.models import ConsumerOutcome
        from rakaia.outcomes import encode

        DjangoOutcomeStore().record(project("0000000001", reasons=("mine",)))
        ConsumerOutcome.objects.create(
            consumer="c",
            stream_path="s",
            subject="row-other",
            offset="0000000002",
            attempt=1,
            payload=encode(project("0000000002", consumer="other")),
        )

        assert [o.subject for o in DjangoOutcomeStore().latest("c", "s")] == [
            "0000000001"
        ]


@pytest.mark.django_db
class TestBoundedColumnsRefuseTheSameValueOnEveryDatabase:
    """The divergence ADR 0007 forecast, closed before the database sees it.

    The columns are bounded — `ConsumerCursor`'s widths — and the two databases
    the suite runs on disagree about what that means: Postgres raises on an
    over-long value, SQLite keeps it whole. A store that behaves differently
    depending on which one is underneath is the reference-is-more-permissive
    defect again, so the length check runs in Python and both databases refuse
    identically. **These cases pass on SQLite only because of that check**;
    without it the SQLite run is green and the Postgres run raises `DataError`.
    """

    @pytest.mark.parametrize(
        ("field", "limit"),
        [("consumer", 128), ("stream_path", 255), ("subject", 255), ("offset", 64)],
    )
    def test_a_name_longer_than_its_column_is_refused(self, field: str, limit: int):
        over = "x" * (limit + 1) if field != "offset" else "0" * (limit + 1)
        with pytest.raises(ValueError, match=f"{field} is {limit + 1} characters"):
            DjangoOutcomeStore().record(an_outcome(**{field: over}))

    @pytest.mark.parametrize(
        ("field", "limit"),
        [("consumer", 128), ("stream_path", 255), ("subject", 255), ("offset", 64)],
    )
    def test_a_name_exactly_the_width_of_its_column_is_kept(
        self, field: str, limit: int
    ):
        """The other half, and the half that catches an off-by-one.

        A check written with `>=` would refuse a name the column holds perfectly
        well, and no refusal test can see that.
        """
        exact = "x" * limit if field != "offset" else "0" * limit
        outcome = an_outcome(**{field: exact})
        store = DjangoOutcomeStore()
        store.record(outcome)

        [got] = store.latest(outcome.consumer, outcome.stream_path)
        assert getattr(got, field) == exact

    def test_a_refused_outcome_leaves_no_row_behind(self):
        """Refusing means nothing was recorded, not that half of it was."""
        from django_rakaia.models import ConsumerOutcome

        with pytest.raises(ValueError):
            DjangoOutcomeStore().record(refused("x" * 256))
        assert ConsumerOutcome.objects.count() == 0

    def test_an_unbounded_field_is_not_length_checked(self):
        """`reasons`, `params` and `sequence_key` live in the payload only, so
        this store holds them at whatever length the other two do. Pinned because
        the cheap fix for the case above — bounding every field — would silently
        make this backend the strict one."""
        long_key = "k" * 5000
        store = DjangoOutcomeStore()
        store.record(
            project(
                "0000000001",
                sequence_key=long_key,
                reasons=(long_key,),
                params={"note": long_key},
            )
        )
        [got] = store.latest("c", "s")
        assert got.sequence_key == long_key and got.reasons == (long_key,)


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
