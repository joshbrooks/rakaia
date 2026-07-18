"""
`manage.py replay <stream> [...]` — replay a rakaia stream through registered
versioned handlers, applying produced effects via the DjangoExecutor.
"""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.store import get_store
from rakaia.executors import CollectingExecutor
from rakaia.replay import replay


class Command(BaseCommand):
    help = "Replay a rakaia stream through registered versioned handlers."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "stream", help="Stream path to replay (e.g. 'room:5:messages')"
        )
        parser.add_argument(
            "--from",
            dest="start_seq",
            type=int,
            default=0,
            help="First event index to replay (inclusive). Default: 0.",
        )
        parser.add_argument(
            "--to",
            dest="end_seq",
            type=int,
            default=None,
            help="One past the last event index to replay. Default: stream head.",
        )
        parser.add_argument(
            "--strict-drift",
            action="store_true",
            help="Raise HandlerDriftError on source_hash mismatch instead of warning.",
        )
        parser.add_argument(
            "--include-external",
            action="store_true",
            help="Apply external effects (e.g. emails) instead of skipping them.",
        )
        parser.add_argument(
            "--dry-run",
            action="store_true",
            help="Resolve handlers and produce effects but do not apply them.",
        )

    def handle(self, *args: Any, **options: Any) -> None:  # noqa: ARG002
        dry_run = options["dry_run"]
        # On a dry run, collect the effects instead of applying them so we can
        # both report the count and list exactly what *would* be written.
        executor: Any = CollectingExecutor() if dry_run else DjangoExecutor()
        result = replay(
            store=get_store(),
            stream_path=options["stream"],
            executor=executor,
            start_seq=options["start_seq"],
            end_seq=options["end_seq"],
            include_external=options["include_external"],
            on_drift="raise" if options["strict_drift"] else "warn",
        )

        mode = "DRY RUN" if dry_run else "APPLIED"
        self.stdout.write(
            self.style.SUCCESS(
                f"[{mode}] stream={options['stream']!r} "
                f"events={result.events_processed} "
                f"effects={result.effects_applied} "
                f"external_skipped={result.external_effects_skipped}"
            )
        )
        if dry_run:
            for eff in executor.effects:
                target = eff.model_label or eff.kind or ""
                detail = eff.lookup if eff.op != "external" else eff.payload
                self.stdout.write(f"  {eff.op} {target} {detail}")
        if result.warnings:
            for w in result.warnings:
                self.stdout.write(self.style.WARNING(w))
        if result.drift_detected:
            self.stdout.write(
                self.style.WARNING(
                    f"Drift detected in {len(result.drift_detected)} "
                    f"handler(s): {result.drift_detected}"
                )
            )
