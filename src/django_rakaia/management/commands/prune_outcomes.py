"""
`manage.py prune_outcomes --older-than-days N` — delete `ConsumerOutcome` rows
recorded more than N days ago, except the latest record for each subject.
"""

from __future__ import annotations

from datetime import timedelta
from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import DEFAULT_DB_ALIAS
from django.utils import timezone

from django_rakaia.models import ConsumerOutcome
from rakaia.outcomes import decode_outcome


class Command(BaseCommand):
    """Retention for the outcome table, with the age supplied by the operator.

    ``--older-than-days`` has no default and the command refuses to run without
    it. Every plausible default is wrong somewhere: a row here is the record of
    an event a consumer could not apply, and the installation that keeps two
    weeks of them and the one that keeps seven years are both right about their
    own obligations. A default would be applied by whoever ran the command
    without reading it, and what it destroys is the evidence someone is still
    working from.

    **The latest record for each subject is never pruned, however old.** Success
    writes no record, so an event with no record reads as one that worked
    (ADR 0007, Decision 3). Deleting the only failure record of an event that was
    skipped would turn it into a success without anyone having decided that. So
    the row `DjangoOutcomeStore.latest` reports for each consumer, stream and
    subject stays; older attempts at the same subject, and records no longer
    readable by this version, go once they are past the age.

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
        doomed = _older_and_not_latest(alias, cutoff)

        if dry_run:
            deleted = len(doomed)
        else:
            deleted = 0
            for start in range(0, len(doomed), batch_size):
                pks = doomed[start : start + batch_size]
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


def _older_and_not_latest(alias: str, cutoff: Any) -> list[int]:
    """Primary keys of rows older than `cutoff` that are not a subject's latest.

    Reads every row once, oldest first, because "latest" can only be decided
    against the whole table: the newest record for a subject may be recent while
    the rows beneath it are old, and the reverse. It applies the same rule as
    `DjangoOutcomeStore.latest` — highest attempt, the later row on a tie — so
    what survives is exactly what that method would have reported. Memory is one
    number per subject plus one per row to be deleted, never the payloads.
    """
    latest: dict[tuple[str, str, str], tuple[int, int]] = {}
    old: list[int] = []
    rows = (
        ConsumerOutcome.objects.using(alias)
        .order_by("pk")
        .values_list("pk", "payload", "recorded_at")
        .iterator()
    )
    for pk, payload, recorded_at in rows:
        if recorded_at < cutoff:
            old.append(pk)
        record = decode_outcome(payload)
        if record is None:
            # Nothing reads a row this version cannot build, so it cannot be the
            # record a subject is judged by.
            continue
        key = (record.consumer, record.stream_path, record.subject)
        held = latest.get(key)
        if held is None or record.attempt >= held[0]:
            latest[key] = (record.attempt, pk)
    keep = {pk for _, pk in latest.values()}
    return [pk for pk in old if pk not in keep]
