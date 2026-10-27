"""Tests for the `manage.py prune_outcomes` management command.

The command deletes `ConsumerOutcome` rows older than an age the operator has to
name. The tests that matter most are the ones about *not* deleting: an omitted
age changes nothing, and the row sitting exactly on the cutoff survives.
"""

from __future__ import annotations

import io
from datetime import timedelta

import pytest
from django.core.management import call_command
from django.core.management.base import CommandError
from django.db import connection
from django.test.utils import CaptureQueriesContext
from django.utils import timezone
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from django_rakaia.management.commands import prune_outcomes
from django_rakaia.models import ConsumerOutcome
from django_rakaia.outcomes import DjangoOutcomeStore
from rakaia.outcomes import Outcome


class _FrozenClock:
    """Just enough of `django.utils.timezone` for the command to read the clock.

    The boundary case cannot be pinned against a moving clock: a row stamped
    ``now() - 30d`` is already microseconds past a cutoff the command computes an
    instant later, so "exactly on the cutoff" is unreachable unless both sides
    agree on one instant.
    """

    def __init__(self, at):
        self._at = at

    def now(self):
        return self._at


def _at(when) -> int:
    """Create one outcome row stamped exactly `when`, and return its pk."""
    row = ConsumerOutcome.objects.create(
        consumer_key="c", stream_path_key="s", payload="{}"
    )
    ConsumerOutcome.objects.filter(pk=row.pk).update(recorded_at=when)
    return row.pk


def _outcome(*, age: timedelta, consumer: str = "c", stream: str = "s") -> int:
    """Create one outcome row aged `age` into the past, and return its pk.

    ``recorded_at`` is ``auto_now_add``, so it cannot be set on create — the row
    is stamped now and then backdated with an ``update()``, which auto_now_add
    does not touch.
    """
    row = ConsumerOutcome.objects.create(
        consumer_key=consumer, stream_path_key=stream, payload="{}"
    )
    ConsumerOutcome.objects.filter(pk=row.pk).update(recorded_at=timezone.now() - age)
    return row.pk


@pytest.mark.django_db
class TestPruneOutcomesCommand:
    def test_no_age_refuses_and_deletes_nothing(self):
        """No default age: without one the command fails and the table is untouched."""
        _outcome(age=timedelta(days=3650))

        with pytest.raises(CommandError) as excinfo:
            call_command("prune_outcomes", stdout=io.StringIO())

        # The message has to say *why* there is no default, or the next operator
        # will read the refusal as a missing flag and guess a number.
        assert "--older-than-days" in str(excinfo.value)
        assert ConsumerOutcome.objects.count() == 1

    def test_deletes_only_rows_older_than_the_age(self):
        old = _outcome(age=timedelta(days=400))
        recent = _outcome(age=timedelta(days=10))

        out = io.StringIO()
        call_command("prune_outcomes", "--older-than-days", "365", stdout=out)

        assert list(ConsumerOutcome.objects.values_list("pk", flat=True)) == [recent]
        assert not ConsumerOutcome.objects.filter(pk=old).exists()
        assert "deleted=1" in out.getvalue()

    def test_row_on_the_cutoff_is_kept(self, monkeypatch):
        """ "Older than" is strict: the row stamped exactly on the cutoff survives."""
        frozen = timezone.now()
        monkeypatch.setattr(prune_outcomes, "timezone", _FrozenClock(frozen))
        cutoff = frozen - timedelta(days=30)

        older = _at(cutoff - timedelta(microseconds=1))
        on_cutoff = _at(cutoff)
        newer = _at(cutoff + timedelta(microseconds=1))

        call_command("prune_outcomes", "--older-than-days", "30", stdout=io.StringIO())

        survivors = set(ConsumerOutcome.objects.values_list("pk", flat=True))
        assert survivors == {on_cutoff, newer}
        assert older not in survivors

    def test_zero_days_is_an_age_and_is_honoured(self):
        """0 is a real answer — delete everything recorded before now — not a missing one."""
        _outcome(age=timedelta(days=1))

        call_command("prune_outcomes", "--older-than-days", "0", stdout=io.StringIO())

        assert ConsumerOutcome.objects.count() == 0

    def test_negative_age_is_refused(self):
        _outcome(age=timedelta(days=1))

        with pytest.raises(CommandError) as excinfo:
            call_command(
                "prune_outcomes", "--older-than-days", "-1", stdout=io.StringIO()
            )

        assert "--older-than-days" in str(excinfo.value)

        assert ConsumerOutcome.objects.count() == 1

    def test_dry_run_reports_the_count_and_deletes_nothing(self):
        _outcome(age=timedelta(days=400))
        _outcome(age=timedelta(days=10))

        out = io.StringIO()
        call_command(
            "prune_outcomes",
            "--older-than-days",
            "365",
            "--dry-run",
            stdout=out,
        )

        assert ConsumerOutcome.objects.count() == 2
        text = out.getvalue()
        assert "DRY RUN" in text
        assert "deleted=1" in text

    def test_deletes_across_more_than_one_batch(self):
        for _ in range(5):
            _outcome(age=timedelta(days=400))
        kept = _outcome(age=timedelta(days=1))

        out = io.StringIO()
        with CaptureQueriesContext(connection) as queries:
            call_command(
                "prune_outcomes",
                "--older-than-days",
                "365",
                "--batch-size",
                "2",
                stdout=out,
            )

        assert list(ConsumerOutcome.objects.values_list("pk", flat=True)) == [kept]
        assert "deleted=5" in out.getvalue()
        # Five stale rows at two per pass is three statements, not one. Counting
        # them is what makes this a test of the batching rather than of the
        # filter — one big DELETE leaves the same rows behind.
        deletes = [q for q in queries.captured_queries if "DELETE" in q["sql"].upper()]
        assert len(deletes) == 3

    def test_reports_the_cutoff_it_used(self):
        out = io.StringIO()
        call_command("prune_outcomes", "--older-than-days", "7", stdout=out)

        # An operator reading a cron log needs to see the date, not just the age.
        cutoff = timezone.now() - timedelta(days=7)
        assert cutoff.date().isoformat() in out.getvalue()


@pytest.mark.django_db
class TestRecordedAtIndex:
    def test_recorded_at_is_indexed(self):
        """The command's filter is the first real query on the timestamp."""
        indexed = {tuple(index.fields) for index in ConsumerOutcome._meta.indexes}
        assert ("recorded_at",) in indexed


@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
class TestTheDatabaseFlag:
    """`--database` decides which database is pruned, and nothing else is touched.

    ``transaction=True`` because this is alias-aware: under a plain marker
    pytest-django has already opened a transaction on every declared alias, which
    is what `CLAUDE.md` records as hiding a missing ``using=`` (#180). There is no
    ``atomic()`` in this command for that to mask today, and the marker is what
    keeps that true if one ever appears.
    """

    def _stale_on(self, alias: str) -> int:
        row = ConsumerOutcome.objects.using(alias).create(
            consumer_key="c", stream_path_key="s", payload="{}"
        )
        ConsumerOutcome.objects.using(alias).filter(pk=row.pk).update(
            recorded_at=timezone.now() - timedelta(days=400)
        )
        return row.pk

    def test_only_the_named_database_is_pruned(self):
        self._stale_on("default")
        self._stale_on("overlay")

        call_command(
            "prune_outcomes", "--older-than-days", "365", "--database", "overlay"
        )

        assert ConsumerOutcome.objects.using("overlay").count() == 0
        # The row the operator did not name. Drop `.using(alias)` from either
        # queryset in the command and this is the row that goes instead.
        assert ConsumerOutcome.objects.using("default").count() == 1

    def test_a_dry_run_counts_the_named_database(self):
        self._stale_on("default")
        self._stale_on("overlay")
        self._stale_on("overlay")

        out = io.StringIO()
        call_command(
            "prune_outcomes",
            "--older-than-days",
            "365",
            "--database",
            "overlay",
            "--dry-run",
            stdout=out,
        )

        assert "deleted=2" in out.getvalue()
        assert ConsumerOutcome.objects.using("overlay").count() == 2


@pytest.mark.django_db
class TestTheBatchSize:
    """A batch size below one is refused rather than quietly doing nothing.

    ``--batch-size 0`` slices ``[:0]``, finds nothing, breaks on the first pass
    and reports ``deleted=0`` — a cron entry that looks like it ran and pruned an
    empty table. ``-1`` reaches Django and raises about negative indexing.
    """

    def test_zero_is_refused_and_deletes_nothing(self):
        _outcome(age=timedelta(days=400))

        with pytest.raises(CommandError, match="--batch-size"):
            call_command(
                "prune_outcomes", "--older-than-days", "365", "--batch-size", "0"
            )

        assert ConsumerOutcome.objects.count() == 1

    def test_negative_is_refused_and_deletes_nothing(self):
        _outcome(age=timedelta(days=400))

        with pytest.raises(CommandError, match="--batch-size"):
            call_command(
                "prune_outcomes", "--older-than-days", "365", "--batch-size", "-1"
            )

        assert ConsumerOutcome.objects.count() == 1


def _record(
    *, subject: str, attempt: int, age: timedelta, consumer: str = "c", stream="s"
) -> int:
    """Record a real outcome through the store, then backdate it."""
    DjangoOutcomeStore().record(
        Outcome(
            consumer=consumer,
            stream_path=stream,
            subject=subject,
            offset="1",
            sequence_key=subject,
            stage="project",
            status="failed",
            reasons=("unhandled",),
            attempt=attempt,
        )
    )
    row = ConsumerOutcome.objects.order_by("-pk").first()
    assert row is not None
    ConsumerOutcome.objects.filter(pk=row.pk).update(recorded_at=timezone.now() - age)
    return row.pk


@pytest.mark.django_db
class TestPruningKeepsTheLatestRecordPerSubject:
    """Success writes no record, so a subject with no record reads as one that
    worked. Pruning a skipped event's only failure record would therefore turn it
    into a success, whatever its age. What the store reports as the latest record
    for each subject is kept."""

    def test_an_old_only_record_survives_so_the_event_does_not_read_as_succeeded(
        self,
    ):
        only = _record(subject="row-1", attempt=1, age=timedelta(days=900))

        call_command("prune_outcomes", "--older-than-days", "365", stdout=io.StringIO())

        assert list(ConsumerOutcome.objects.values_list("pk", flat=True)) == [only]
        assert [o.subject for o in DjangoOutcomeStore().latest("c", "s")] == ["row-1"]

    def test_older_attempts_of_the_same_subject_are_pruned(self):
        first = _record(subject="row-1", attempt=1, age=timedelta(days=900))
        second = _record(subject="row-1", attempt=2, age=timedelta(days=800))

        out = io.StringIO()
        call_command("prune_outcomes", "--older-than-days", "365", stdout=out)

        survivors = set(ConsumerOutcome.objects.values_list("pk", flat=True))
        assert survivors == {second}
        assert first not in survivors
        assert "deleted=1" in out.getvalue()

    def test_latest_is_unchanged_by_pruning(self):
        _record(subject="a", attempt=1, age=timedelta(days=900))
        _record(subject="a", attempt=2, age=timedelta(days=700))
        _record(subject="b", attempt=1, age=timedelta(days=600))
        _record(subject="c", attempt=1, age=timedelta(days=5))
        before = DjangoOutcomeStore().latest("c", "s")

        call_command("prune_outcomes", "--older-than-days", "365", stdout=io.StringIO())

        assert DjangoOutcomeStore().latest("c", "s") == before

    def test_the_same_subject_under_two_consumers_keeps_one_record_each(self):
        mine = _record(subject="x", attempt=1, age=timedelta(days=900), consumer="a")
        theirs = _record(subject="x", attempt=1, age=timedelta(days=900), consumer="b")
        other_stream = _record(
            subject="x", attempt=1, age=timedelta(days=900), consumer="a", stream="t"
        )

        call_command("prune_outcomes", "--older-than-days", "365", stdout=io.StringIO())

        survivors = set(ConsumerOutcome.objects.values_list("pk", flat=True))
        assert survivors == {mine, theirs, other_stream}

    def test_a_dry_run_counts_what_a_real_run_would_delete(self):
        _record(subject="a", attempt=1, age=timedelta(days=900))
        _record(subject="a", attempt=2, age=timedelta(days=800))
        _record(subject="b", attempt=1, age=timedelta(days=800))

        out = io.StringIO()
        call_command(
            "prune_outcomes",
            "--older-than-days",
            "365",
            "--dry-run",
            stdout=out,
        )

        assert "deleted=1" in out.getvalue()
        assert ConsumerOutcome.objects.count() == 3

    def test_a_repeat_of_the_same_attempt_keeps_the_later_row(self):
        """`latest` lets the last write win on a tie; so must the keeper."""
        first = _record(subject="a", attempt=1, age=timedelta(days=900))
        second = _record(subject="a", attempt=1, age=timedelta(days=800))

        call_command("prune_outcomes", "--older-than-days", "365", stdout=io.StringIO())

        survivors = set(ConsumerOutcome.objects.values_list("pk", flat=True))
        assert survivors == {second}
        assert first not in survivors

    def test_a_higher_attempt_beats_a_later_written_lower_one(self):
        """Attempt, not write order, decides -- as it does in `latest`."""
        higher = _record(subject="a", attempt=2, age=timedelta(days=900))
        _record(subject="a", attempt=1, age=timedelta(days=800))

        call_command("prune_outcomes", "--older-than-days", "365", stdout=io.StringIO())

        assert list(ConsumerOutcome.objects.values_list("pk", flat=True)) == [higher]


@pytest.mark.django_db
class TestPruningNeverChangesWhatLatestReports:
    """Property: whatever the ages and attempts, pruning leaves `latest` as it was."""

    @settings(
        max_examples=40,
        deadline=None,
        suppress_health_check=[HealthCheck.function_scoped_fixture],
    )
    @given(
        rows=st.lists(
            st.tuples(
                st.sampled_from(["a", "b", "c"]),
                st.integers(min_value=1, max_value=3),
                st.integers(min_value=0, max_value=900),
            ),
            max_size=12,
        ),
        days=st.integers(min_value=0, max_value=900),
    )
    def test_every_subjects_latest_record_survives(self, rows, days):
        ConsumerOutcome.objects.all().delete()
        for subject, attempt, age in rows:
            _record(subject=subject, attempt=attempt, age=timedelta(days=age))
        before = DjangoOutcomeStore().latest("c", "s")

        call_command(
            "prune_outcomes", "--older-than-days", str(days), stdout=io.StringIO()
        )

        assert DjangoOutcomeStore().latest("c", "s") == before
