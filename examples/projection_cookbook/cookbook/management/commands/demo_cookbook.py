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
from django_rakaia.hermeticity import assert_no_live_writes
from django_rakaia.projection_reader import DjangoProjectionReader
from django_rakaia.store import get_store
from django_rakaia.verification import (
    VACUOUS,
    VacuousVerification,
    diff_effects_against_rows,
)
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
        self._check_a_sweep_that_compared_nothing_is_refused(store)
        self._check_the_rebuild_does_not_touch_live(store)
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
        # `certified`, not `ok`: `ok` asks "did anything disagree?", which is
        # vacuously true when nothing was compared. `raise_if_diff()` refuses an
        # empty population outright — see the next check.
        if not report.certified:
            raise CommandError(f"Replay does not reproduce current rows:\n{report}")
        self.stdout.write(
            self.style.SUCCESS(
                f"[2] verification: replay reproduces all "
                f"{report.compared} projected rows — verdict {report.verdict} ✓"
            )
        )

    def _check_a_sweep_that_compared_nothing_is_refused(self, store: Any) -> None:
        """A verification that examined no rows must not report success.

        The failure this guards against is not a wrong answer, it is a confident
        one with nothing behind it. Every way a sweep silently compares zero rows
        is mundane — a store on the wrong backend, a replay over a renamed
        stream, a filter that stopped matching, a registry that failed to load —
        and each one used to print a clean bill of health.

        Here we cause it deliberately the way a rename does: replay a stream
        that exists but holds no events. Nothing errors, nothing is compared,
        and the sweep has no evidence either way.
        """
        renamed = f"{STREAM}-renamed"
        store.create(renamed)

        ex = CollectingExecutor()
        replay(store, renamed, ex, reader=DjangoProjectionReader())
        report = diff_effects_against_rows(ex.effects)

        # The false green itself: `ok` is True here and means nothing, because
        # it asks "did anything disagree?" of a population of zero. Asserting it
        # is the demonstration — a demo that only checked the new properties
        # would show the fix while hiding what it fixes.
        if not report.ok:
            raise CommandError(
                "expected the vacuous case to still report ok=True — that is "
                "precisely the false green this check exists to show"
            )
        if report.certified or report.verdict != VACUOUS:
            raise CommandError(
                f"An empty sweep certified itself: verdict={report.verdict}"
            )
        try:
            report.raise_if_diff()
        except VacuousVerification:
            pass
        else:
            raise CommandError("raise_if_diff() accepted a population of zero")

        self.stdout.write(
            self.style.SUCCESS(
                "[3] vacuous green: a sweep that compared 0 rows reports "
                f"{report.verdict.upper()} and refuses to certify ✓"
            )
        )

    def _check_the_rebuild_does_not_touch_live(self, store: Any) -> None:
        """Prove the read-only verification really is read-only.

        `assert_no_live_writes` is the write half of the rebuild gate: it
        compares row counts across the block and raises if anything moved. Here
        it certifies that the diff sweep above — which runs a full replay —
        wrote nothing, which is the claim `CollectingExecutor` makes and which
        nothing otherwise checks.
        """
        with assert_no_live_writes(Project, Task):
            ex = CollectingExecutor()
            replay(store, STREAM, ex, reader=DjangoProjectionReader())
            diff_effects_against_rows(ex.effects)

        self.stdout.write(
            self.style.SUCCESS(
                "[4] rebuild isolation: a dry-run replay left every live row "
                "untouched ✓"
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
            self.style.SUCCESS("[5] idempotent: a second replay is a no-op ✓")
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
