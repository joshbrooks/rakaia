"""
`manage.py prune_orphan_events [--dry-run]` — delete `StreamEvent` rows that no
`StreamEntry` in any stream points at.
"""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.db import DEFAULT_DB_ALIAS

from django_rakaia.models import StreamEvent


class Command(BaseCommand):
    """Remove the payloads no stream references any longer.

    `DjangoStreamStore.delete()` removes its own stream's orphans as it goes, so
    this is for the ones already sitting in the table: left by deletes from
    before that, or by entries removed some other way. An event still reached
    from any stream is kept, and there is no copy of what is deleted, so run it
    with ``--dry-run`` first and take a backup if the payloads might matter.

    Deletion walks the table in primary-key order, one batch at a time, and
    reads only ids, so neither the batch query nor the delete loads a payload.
    """

    help = "Delete stream events that no stream references."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Report how many events would be deleted, and delete nothing.",
        )
        parser.add_argument(
            "--batch-size",
            dest="batch_size",
            type=int,
            default=1000,
            help="Events to delete per statement. Default: 1000.",
        )
        parser.add_argument(
            "--database",
            dest="database",
            default=DEFAULT_DB_ALIAS,
            help=f"Database alias to prune. Default: {DEFAULT_DB_ALIAS!r}.",
        )

    def handle(self, *args: Any, **options: Any) -> None:  # noqa: ARG002
        batch_size = options["batch_size"]
        if batch_size < 1:
            raise CommandError(f"--batch-size must be at least 1 (got {batch_size}).")

        alias = options["database"]
        dry_run = options["dry_run"]
        events = StreamEvent.objects.using(alias)
        orphans = events.filter(entries__isnull=True).order_by("pk")

        if dry_run:
            count = orphans.count()
        else:
            count = 0
            last = 0
            while True:
                pks = list(
                    orphans.filter(pk__gt=last).values_list("pk", flat=True)[
                        :batch_size
                    ]
                )
                if not pks:
                    break
                last = pks[-1]
                # No re-check for an entry here: entries are only ever written
                # with a new event, in its own transaction, so an orphan cannot
                # gain one.
                _, per_model = events.filter(pk__in=pks).only("pk").delete()
                count += per_model.get(StreamEvent._meta.label, 0)

        mode = "DRY RUN" if dry_run else "DELETED"
        self.stdout.write(
            self.style.SUCCESS(f"[{mode}] database={alias!r} orphans={count}")
        )
