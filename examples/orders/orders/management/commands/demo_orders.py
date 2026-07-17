"""`manage.py demo_orders` — the versioned-handlers walkthrough in one command.

Because rakaia's StreamStore is in-memory and process-local, seeding and replay
must happen in the *same* process. This command:

    1. (re)seeds the `orders` stream with SAMPLE_ORDERS,
    2. replays it through the registered versioned handlers + upcaster,
    3. prints the materialized OrderSummary rows.

Watch the `tax` column: orders placed before the tax-rule change (seq < 3) keep
0% tax; later ones are taxed at 10% — even though the newer handler is what runs
today. That is the time-correctness guarantee.
"""

from __future__ import annotations

import json
from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.store import get_store
from orders.models import OrderSummary
from orders.seed import SAMPLE_ORDERS, TAX_CHANGE_SEQ
from rakaia import CollectingExecutor
from rakaia.replay import replay

STREAM = "orders"


class Command(BaseCommand):
    help = "Seed the orders stream and replay it through versioned handlers."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--twice",
            action="store_true",
            help="Replay a second time to demonstrate idempotency.",
        )
        parser.add_argument(
            "--include-external",
            action="store_true",
            help="Apply external effects (receipt emails) instead of skipping them.",
        )
        parser.add_argument(
            "--strict-drift",
            action="store_true",
            help="Raise HandlerDriftError on source-hash mismatch instead of warning.",
        )

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        store = get_store()

        # Reset so the command is re-runnable: fresh stream (seq restarts at 0)
        # and no stale projection rows.
        store.delete(STREAM)
        OrderSummary.objects.all().delete()

        store.create(STREAM)
        for event in SAMPLE_ORDERS:
            store.append(STREAM, json.dumps(event).encode("utf-8"))
        self.stdout.write(
            f"Seeded {len(SAMPLE_ORDERS)} order events "
            f"(tax rule changes at seq {TAX_CHANGE_SEQ}).\n"
        )

        # Dry run first: a CollectingExecutor records the effects replay would
        # apply, writing nothing. The count matches effects_applied below.
        preview = CollectingExecutor()
        replay(
            store=store,
            stream_path=STREAM,
            executor=preview,
            include_external=opts["include_external"],
            on_drift="raise" if opts["strict_drift"] else "warn",
        )
        self.stdout.write(
            f"Dry run: replay would apply {len(preview.effects)} effects "
            f"(no writes yet).\n"
        )

        result = replay(
            store=store,
            stream_path=STREAM,
            executor=DjangoExecutor(),
            include_external=opts["include_external"],
            on_drift="raise" if opts["strict_drift"] else "warn",
        )
        self._print_result(result)
        self._print_table()

        if opts["twice"]:
            before = OrderSummary.objects.count()
            replay(
                store=store,
                stream_path=STREAM,
                executor=DjangoExecutor(),
                include_external=opts["include_external"],
                on_drift="raise" if opts["strict_drift"] else "warn",
            )
            after = OrderSummary.objects.count()
            ok = before == after
            style = self.style.SUCCESS if ok else self.style.ERROR
            self.stdout.write(
                style(
                    f"\nReplayed again: {before} -> {after} rows — "
                    f"{'idempotent ✓' if ok else 'NOT idempotent ✗'}"
                )
            )

    def _print_result(self, result: Any) -> None:
        self.stdout.write(
            self.style.SUCCESS(
                f"[replay] events={result.events_processed} "
                f"effects_applied={result.effects_applied} "
                f"external_skipped={result.external_effects_skipped}"
            )
        )
        for w in result.warnings:
            self.stdout.write(self.style.WARNING(w))

    def _print_table(self) -> None:
        header = (
            f"\n{'order':<10}{'status':<11}{'subtotal':>10}"
            f"{'rate':>7}{'tax':>9}{'total':>10}{'points':>8}"
        )
        self.stdout.write(header)
        self.stdout.write("-" * len(header.strip("\n")))
        for o in OrderSummary.objects.all():
            self.stdout.write(
                f"{o.order_id:<10}{o.status:<11}{o.subtotal:>10}"
                f"{o.tax_rate:>7}{o.tax:>9}{o.total:>10}{o.loyalty_points:>8}"
            )
