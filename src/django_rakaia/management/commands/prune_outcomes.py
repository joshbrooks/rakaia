"""
`manage.py prune_outcomes --older-than-days N` — delete `ConsumerOutcome` rows
recorded more than N days ago.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import DEFAULT_DB_ALIAS
from django.utils import timezone

from django_rakaia.models import ConsumerOutcome


class Command(BaseCommand):
    """Retention for the outcome table, with the age supplied by the operator.

    ``--older-than-days`` has no default and the command refuses to run without
    it. Every plausible default is wrong somewhere: a row here is the record of
    an event a consumer could not apply, and the installation that keeps two
    weeks of them and the one that keeps seven years are both right about their
    own obligations. A default would be applied by whoever ran the command
    without reading it, and what it destroys is the evidence someone is still
    working from.

    ``--dry-run`` counts the same rows without deleting them, so the age can be
    checked against a real table before it is trusted in a cron entry. Deletion
    is batched because the table's size is unbounded in exactly the case this
    command exists for — an installation that has never pruned — and one
    statement over a year of rows is a long lock on a table the consume loop
    writes to from inside its own error handler.
    """

    help = "Delete consumer outcome records older than a given age."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--older-than-days",
            dest="older_than_days",
            type=int,
            default=None,
            help=(
                "Delete outcomes recorded more than this many days ago. "
                "Required — there is deliberately no default."
            ),
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report how many rows would be deleted, and delete nothing.",
        )
        parser.add_argument(
            "--batch-size",
            dest="batch_size",
            type=int,
            default=1000,
            help="Rows to delete per statement. Default: 1000.",
        )
        parser.add_argument(
            "--database",
            dest="database",
            default=DEFAULT_DB_ALIAS,
            help=f"Database alias to prune. Default: {DEFAULT_DB_ALIAS!r}.",
        )

    def handle(self, *args: Any, **options: Any) -> None:  # noqa: ARG002
        days = options["older_than_days"]
        if days is None:
            raise CommandError(
                "--older-than-days is required and has no default. The right "
                "retention period differs by installation, and a guess deletes "
                "the record of a failure someone may still be investigating. "
                "Pass the age you mean, e.g. --older-than-days 365."
            )
        if days < 0:
            raise CommandError(
                f"--older-than-days must not be negative (got {days}). An age "
                "in the future selects no rows and is more likely a typo than a "
                "request."
            )
        batch_size = options["batch_size"]
        if batch_size < 1:
            raise CommandError(f"--batch-size must be at least 1 (got {batch_size}).")

        alias = options["database"]
        dry_run = options["dry_run"]
        cutoff = timezone.now() - timedelta(days=days)
        # Strictly older: a row stamped exactly on the cutoff is not older than
        # the age given, and keeping it is the reading that never deletes more
        # than the operator asked for.
        stale = ConsumerOutcome.objects.using(alias).filter(recorded_at__lt=cutoff)

        if dry_run:
            deleted = stale.count()
        else:
            deleted = 0
            while True:
                # Re-selecting the primary keys each pass rather than deleting
                # the sliced queryset directly: a queryset with a LIMIT cannot be
                # deleted, and the filter is on a timestamp that only ever moves
                # rows *into* the selection, so a fresh pass cannot loop forever.
                pks = list(stale.values_list("pk", flat=True)[:batch_size])
                if not pks:
                    break
                ConsumerOutcome.objects.using(alias).filter(pk__in=pks).delete()
                deleted += len(pks)

        mode = "DRY RUN" if dry_run else "DELETED"
        self.stdout.write(
            self.style.SUCCESS(
                f"[{mode}] older-than-days={days} "
                f"cutoff={cutoff.isoformat()} "
                f"database={alias!r} "
                f"deleted={deleted}"
            )
        )
