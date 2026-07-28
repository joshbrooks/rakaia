"""`manage.py demo_cookbook` — the whole staged-projection recipe in one command.

    StreamStore  ->  register_simple / register_handler(stage=)
                 ->  replay(reader=DjangoProjectionReader(), executor=DjangoExecutor())
                 ->  diff_effects_against_rows   (verify the rebuild reproduces the rows)

Because rakaia's StreamStore is in-memory and process-local, seeding and replay
happen in the *same* process. The command runs three asserted checks and exits
non-zero on failure, so it doubles as a smoke test.
"""

from __future__ import annotations

import json
from typing import Any

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError

from cookbook.models import Project, Task
from cookbook.seed import SAMPLE_EVENTS
from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.projection_reader import DjangoProjectionReader
from django_rakaia.store import get_store
from django_rakaia.verification import diff_effects_against_rows
from rakaia import CollectingExecutor
from rakaia.replay import replay

STREAM = "cookbook"


class Command(BaseCommand):
    help = "Seed a two-form stream and replay it into a staged projection."

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        # Self-contained: ensure this demo's tables exist so `manage.py
        # demo_*` works when run directly, not only via the migrate-first
        # `just` recipe. Idempotent — a no-op once migrations are applied.
        call_command("migrate", verbosity=0, interactive=False)
        store = get_store()

        # 1. Seed. Reset first so the command is re-runnable (fresh stream, empty
        #    projection). The events are deliberately out of order — the tasks for
        #    P-100 land before the PROJECT event that creates it.
        store.delete(STREAM)
        Task.objects.all().delete()
        Project.objects.all().delete()
        store.create(STREAM)
        for event in SAMPLE_EVENTS:
            store.append(STREAM, json.dumps(event).encode("utf-8"))
        self.stdout.write(
            f"Seeded {len(SAMPLE_EVENTS)} events "
            "(the P-100 tasks arrive before the P-100 project)."
        )

        # 2. Replay. Stage 0 builds every Project; stage 1 links each Task via the
        #    reader. One call, both stages — the reader is what stage 1 needs.
        replay(
            store,
            STREAM,
            DjangoExecutor(),
            reader=DjangoProjectionReader(),
        )
        self._print_table()

        # 3. Checks.
        self._check_out_of_order_link()
        self._check_replay_reproduces_rows(store)
        self._check_idempotent(store)

        self.stdout.write(self.style.SUCCESS("\nAll checks passed."))

    # ------------------------------------------------------------------ checks

    def _check_out_of_order_link(self) -> None:
        """The headline: a task that arrived before its project still linked."""
        t1 = Task.objects.get(task_id="T-1")
        if t1.project is None or t1.project.code != "P-100":
            raise CommandError(
                f"T-1 should link to P-100, but project={t1.project!r}. "
                "Staged replay did not resolve the out-of-order reference."
            )
        self.stdout.write(
            self.style.SUCCESS("[1] out-of-order link: T-1 (seeded first) → P-100 ✓")
        )

    def _check_replay_reproduces_rows(self, store: Any) -> None:
        """Prove replay reproduces exactly the rows we now have — read-only.

        Re-run the replay with a CollectingExecutor (which writes nothing) and
        diff every write effect's defaults against the live rows. This is the
        migration-verification primitive: "does replaying the log reproduce the
        projection?" — answered without touching the database.
        """
        ex = CollectingExecutor()
        replay(store, STREAM, ex, reader=DjangoProjectionReader())
        report = diff_effects_against_rows(ex.effects)
        if not report.ok:
            raise CommandError(f"Replay does not reproduce current rows:\n{report}")
        self.stdout.write(
            self.style.SUCCESS(
                f"[2] verification: replay reproduces all "
                f"{len(report.rows)} projected rows ✓"
            )
        )

    def _check_idempotent(self, store: Any) -> None:
        """Replaying again changes nothing (every effect is update_or_create)."""
        before = {(p.code, p.name) for p in Project.objects.all()} | {
            (t.task_id, t.title, t.project_id) for t in Task.objects.all()
        }
        replay(store, STREAM, DjangoExecutor(), reader=DjangoProjectionReader())
        after = {(p.code, p.name) for p in Project.objects.all()} | {
            (t.task_id, t.title, t.project_id) for t in Task.objects.all()
        }
        if before != after or Project.objects.count() != 2 or Task.objects.count() != 3:
            raise CommandError("Second replay changed the projection — not idempotent.")
        self.stdout.write(
            self.style.SUCCESS("[3] idempotent: a second replay is a no-op ✓")
        )

    # ----------------------------------------------------------------- display

    def _print_table(self) -> None:
        self.stdout.write("\nProjects and their tasks:")
        for project in Project.objects.all():
            self.stdout.write(f"  {project.code}  {project.name}")
            for task in project.tasks.all():
                self.stdout.write(f"    - {task.task_id}  {task.title}")
        orphans = Task.objects.filter(project__isnull=True)
        for task in orphans:
            self.stdout.write(f"  (unlinked) {task.task_id}  {task.title}")
        self.stdout.write("")
