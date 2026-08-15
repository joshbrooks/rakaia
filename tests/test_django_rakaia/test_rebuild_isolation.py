"""The write-side half of the rebuild gate: a rebuild must not mutate live.

`deny_database_access` (ADR 0003) proves a from-scratch rebuild does not **read**
the production database. This is its mirror: proof it does not **write** it
either. `hermeticity.py`'s own docstring has named `assert_no_live_writes` as the
write-side guard since ADR 0003 landed, while the implementation lived only in
the first consumer's tree. These tests bring it upstream, where the pairing is
documented.

**Why both halves are needed, and why row counts.** The read-side guard is a
statement wrapper, so it can only be armed around a region where *no* legitimate
query to the alias happens. A rebuild frequently cannot meet that bar: the event
log itself may live on `default`, and the read guard would trip on the store's
own reads (the constraint `deny_database_access`'s docstring already records).
The write guard has no such limit — it compares row counts across the block, so
it tolerates arbitrary reads and still catches a mutation. It is the guard you
can arm around the *whole* rebuild rather than a hand-picked region.

The specific leak it exists to catch: `session_replication_role = replica`
disables Postgres triggers but **not** Django `post_save`/`pre_save` signals, so
a signal receiver that writes without a `using=` lands on `default` — silently
mutating production during a rebuild that reports itself green.
"""

from __future__ import annotations

import pytest

from django_rakaia.hermeticity import LiveWriteLeaked, assert_no_live_writes

from .models import FinanceLine

pytestmark = pytest.mark.django_db(databases=["default", "overlay"])


def _line(submission_id: str, *, using: str = "default") -> FinanceLine:
    return FinanceLine.objects.using(using).create(
        submission_id=submission_id, suku="s"
    )


class TestAWriteToTheLiveDatabaseIsCaught:
    """The RED core."""

    def test_a_create_on_the_default_alias_raises(self):
        with pytest.raises(LiveWriteLeaked), assert_no_live_writes(FinanceLine):
            _line("leaked")

    def test_a_delete_on_the_default_alias_raises(self):
        """Row counts move in both directions — a rebuild that *removes* live
        rows is just as much a live mutation as one that adds them."""
        _line("pre-existing")
        with pytest.raises(LiveWriteLeaked), assert_no_live_writes(FinanceLine):
            FinanceLine.objects.using("default").all().delete()

    def test_the_error_names_the_model_and_the_counts(self):
        with pytest.raises(LiveWriteLeaked) as exc, assert_no_live_writes(FinanceLine):
            _line("leaked")
        message = str(exc.value)
        assert "FinanceLine" in message
        assert "0" in message and "1" in message


class TestALegitimateRebuildPasses:
    def test_writing_only_to_the_disposable_alias_is_fine(self):
        """The whole point: a rebuild replays into `overlay` and `default` is
        untouched."""
        with assert_no_live_writes(FinanceLine):
            _line("rebuilt", using="overlay")
        assert FinanceLine.objects.using("overlay").count() == 1
        assert FinanceLine.objects.using("default").count() == 0

    def test_reads_of_the_live_alias_are_tolerated(self):
        """Unlike the read-side guard, this one permits reads — which is what
        lets it wrap a whole rebuild whose event log lives on `default`."""
        _line("pre-existing")
        with assert_no_live_writes(FinanceLine):
            assert FinanceLine.objects.using("default").count() == 1

    def test_no_models_is_a_no_op(self):
        with assert_no_live_writes():
            _line("unguarded")


class TestGuardingAnExplicitAlias:
    def test_using_names_the_alias_to_protect(self):
        """Symmetric with `deny_database_access(*aliases)` — you say which
        database is the live one."""
        with (
            pytest.raises(LiveWriteLeaked),
            assert_no_live_writes(FinanceLine, using="overlay"),
        ):
            _line("leaked", using="overlay")

    def test_a_write_to_a_different_alias_is_ignored(self):
        with assert_no_live_writes(FinanceLine, using="overlay"):
            _line("fine", using="default")


class TestTheGuardChecksEvenWhenTheBlockFails:
    """A rebuild that raised half-way still must not have mutated live — and the
    original failure must survive, since it is the more informative one."""

    def test_the_original_exception_propagates_when_nothing_leaked(self):
        with (
            pytest.raises(ValueError, match="boom"),
            assert_no_live_writes(FinanceLine),
        ):
            raise ValueError("boom")

    def test_a_leak_is_reported_even_if_the_block_raised(self):
        with pytest.raises(LiveWriteLeaked), assert_no_live_writes(FinanceLine):
            _line("leaked")
            raise ValueError("boom")


class TestPairingWithTheReadSideGuard:
    def test_both_guards_compose(self):
        """The documented shape of a full gate: deny reads of the live alias,
        and assert nothing was written to it."""
        from django_rakaia.hermeticity import deny_database_access

        with assert_no_live_writes(FinanceLine), deny_database_access("default"):
            _line("rebuilt", using="overlay")

        assert FinanceLine.objects.using("default").count() == 0

    def test_live_write_leaked_is_a_runtime_error(self):
        """Matches `AmbientDatabaseAccess` — a rebuild that mutates live is a
        defect, not a warning."""
        assert issubclass(LiveWriteLeaked, RuntimeError)
