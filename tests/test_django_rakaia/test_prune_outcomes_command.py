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

from django_rakaia.management.commands import prune_outcomes
from django_rakaia.models import ConsumerOutcome


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
