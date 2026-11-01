"""`manage.py demo_migrate_consumer` — a copy keeps a consumer's position, or says why not.

It answers: *I moved this stream from the file store to the database. Can the
consumer that was already reading it just carry on?* Two runs, each asserted
hard (a failure raises CommandError):

  [1] KEPT       — the copy keeps every offset and adopts the source's stamp
                   (`migrate_stream` -> `adopt_generation`). The consumer resumes
                   and applies only the events appended after its cursor.
  [2] RENUMBERED — the source counts bytes and the target counts entries, so the
                   copy cannot keep the offsets. It does not adopt the stamp, and
                   the consumer stops as `replaced` instead of silently applying
                   the same events under new positions.

Both stores here are entry-counting, which is why [1] works: `JsonlStreamStore`
and `DjangoStreamStore` both issue plain, zero-padded entry offsets, while the
in-memory store's byte-counting format can never line up with either.
"""

from __future__ import annotations

import json
from typing import Any

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError

from django_rakaia import django_consumer
from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import ConsumerCursor, ConsumerOutcome
from django_rakaia.store import get_store
from django_rakaia.subscription import load_cursor, load_generation
from polyglot.consumer import make_apply
from polyglot.models import TranslationSnapshot
from rakaia import StreamStore, migrate_stream
from rakaia.migrate import Migration
from rakaia.subscription import Consumed

CONSUMER = "translation-snapshot"
KEPT_PATH = "/translations/consumer-demo"
RENUMBERED_PATH = "/translations/consumer-renumbered"

FIRST = [
    {"langcode": "tet", "msgid": "nav.product", "msgstr": "Produtu"},
    {"langcode": "pt", "msgid": "nav.product", "msgstr": "Produto"},
    {"langcode": "id", "msgid": "nav.product", "msgstr": "Produk"},
]
SECOND = [
    {"langcode": "tet", "msgid": "hero.title", "msgstr": "Hadia tradusaun moris"},
    {"langcode": "pt", "msgid": "hero.title", "msgstr": "Traduções vivas"},
]


class Command(BaseCommand):
    help = (
        "Migrate a stream under a live consumer, keeping its position or replacing it."
    )

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        # Self-contained: ensure the cursor/outcome tables exist even when run
        # directly rather than through the migrate-first `just` recipe.
        call_command("migrate", verbosity=0, interactive=False)

        self.jsonl = get_store()
        self.django = DjangoStreamStore()
        self._reset()

        self._check_kept_offsets()
        self._check_renumbered()

        self.stdout.write(self.style.SUCCESS("\nAll stream-migration checks passed ✓"))

    # -- setup --------------------------------------------------------------

    def _reset(self) -> None:
        for path in (KEPT_PATH, RENUMBERED_PATH):
            self.jsonl.delete(path)
            self.django.delete(path)
        TranslationSnapshot.objects.all().delete()
        ConsumerCursor.objects.all().delete()
        ConsumerOutcome.objects.all().delete()

    def _append(self, store: Any, path: str, events: list[dict[str, Any]]) -> None:
        store.append_many(
            path, [(json.dumps(event).encode(), None) for event in events]
        )

    def _run(self, store: Any, path: str) -> Consumed:
        return django_consumer(store, path, CONSUMER).run(make_apply(), on_error="halt")

    def _expect(self, condition: bool, message: str) -> None:
        if not condition:
            raise CommandError(message)

    def _report(self, migration: Migration) -> None:
        self.stdout.write(
            f"    copy: {migration.events} events, "
            f"offsets_preserved={migration.offsets_preserved}, "
            f"head_preserved={migration.head_preserved}, "
            f"cursors_valid={migration.cursors_valid}"
        )

    # -- [1] offsets kept ---------------------------------------------------

    def _check_kept_offsets(self) -> None:
        self.jsonl.create(KEPT_PATH, content_type="application/json")
        self._append(self.jsonl, KEPT_PATH, FIRST)
        first = self._run(self.jsonl, KEPT_PATH)
        old_cursor = load_cursor(CONSUMER, KEPT_PATH)

        self.stdout.write("[1] KEPT — the copy keeps every offset and the stamp:")
        self.stdout.write(
            f"    read {first.applied} events from the JSONL store, "
            f"position {old_cursor}"
        )
        self._expect(first.applied == 3, f"expected 3 applied, got {first.applied}")
        self._expect(
            old_cursor == self.jsonl.get_current_offset(KEPT_PATH),
            "the first run should be caught up",
        )

        # Two more events arrive *after* the cursor, so a resumed consumer that
        # applies two rows proves it read from its position rather than the start.
        self._append(self.jsonl, KEPT_PATH, SECOND)
        migration = migrate_stream(self.jsonl, self.django, KEPT_PATH)
        self._report(migration)
        self._expect(migration.offsets_preserved, "this copy should keep offsets")
        self._expect(migration.head_preserved, "this copy should keep the head")
        self._expect(migration.cursors_valid, "the saved cursor should still be valid")
        self._expect(
            self.django.get_generation(KEPT_PATH)
            == self.jsonl.get_generation(KEPT_PATH),
            "a copy that kept its offsets should adopt the source's stamp",
        )

        resumed = self._run(self.django, KEPT_PATH)
        self.stdout.write(
            f"    consumer on the SQLite copy: {resumed.status}, "
            f"applied {resumed.applied}, position {resumed.cursor}"
        )
        self._expect(
            resumed.status == "advanced",
            f"a kept-offset migration should resume, got {resumed.status}",
        )
        self._expect(
            resumed.applied == 2,
            f"it should apply only the two new events, got {resumed.applied}",
        )
        self._expect(
            TranslationSnapshot.objects.count() == 5,
            f"expected 5 snapshot rows, got {TranslationSnapshot.objects.count()}",
        )
        self._expect(
            resumed.cursor == self.django.get_current_offset(KEPT_PATH),
            "the resumed consumer should reach the copy's head",
        )
        self.stdout.write(
            self.style.SUCCESS(
                "    → the same offsets, the same stamp: the consumer carried on "
                "from where it was ✓"
            )
        )

    # -- [2] renumbered -----------------------------------------------------

    def _check_renumbered(self) -> None:
        # The in-memory store counts bytes, so its offsets are a different
        # format. Copying from it is the everyday way a migration renumbers.
        source = StreamStore()
        source.create(RENUMBERED_PATH)
        self._append(source, RENUMBERED_PATH, FIRST)
        first = self._run(source, RENUMBERED_PATH)
        old_cursor = load_cursor(CONSUMER, RENUMBERED_PATH)
        old_generation = load_generation(CONSUMER, RENUMBERED_PATH)

        self.stdout.write(
            "\n[2] RENUMBERED — the source and target cannot share offsets:"
        )
        self.stdout.write(
            f"    read {first.applied} events from the in-memory store, "
            f"position {old_cursor}"
        )
        self._expect(first.applied == 3, f"expected 3 applied, got {first.applied}")

        migration = migrate_stream(source, self.jsonl, RENUMBERED_PATH)
        self._report(migration)
        self._expect(
            not migration.offsets_preserved, "this copy cannot keep the offsets"
        )
        self._expect(
            not migration.cursors_valid, "the saved cursor cannot still be valid"
        )
        self._expect(
            self.jsonl.get_generation(RENUMBERED_PATH) != old_generation,
            "a renumbered copy must not adopt the source's stamp",
        )

        result = self._run(self.jsonl, RENUMBERED_PATH)
        self.stdout.write(
            f"    consumer on the JSONL copy: {result.status}, "
            f"applied {result.applied}, position {load_cursor(CONSUMER, RENUMBERED_PATH)}"
        )
        self._expect(
            result.status == "replaced", f"expected replaced, got {result.status}"
        )
        self._expect(result.needs_rebuild, "a replaced stream needs a rebuild")
        self._expect(
            result.applied == 0, f"a replaced stream applies nothing: {result}"
        )
        self._expect(result.outcomes == (), "a replaced stream records no outcome")
        self._expect(
            load_cursor(CONSUMER, RENUMBERED_PATH) == old_cursor,
            "a replaced stream must leave the cursor unchanged",
        )
        self.stdout.write(
            self.style.SUCCESS(
                "    → the consumer stopped and said the stream is not the one it "
                "was reading, rather than applying events under new offsets ✓"
            )
        )
