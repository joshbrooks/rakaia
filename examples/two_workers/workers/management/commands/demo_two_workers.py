"""`manage.py demo_two_workers` — two processes, one consumer name.

It answers: *what happens when two workers run the same consumer at once, and
what does a cursor actually guarantee afterwards?*

Three checks, each asserted hard (a failure raises CommandError):

  [1] TAKE TURNS — two worker *processes* share one consumer name. The second is
                   seen waiting on the cursor row (the server's own
                   `pg_stat_activity`, not a stopwatch) and then finds nothing
                   left, so all 200 events are applied exactly once, by the
                   first. Both processes finish, and the cursor ends at the
                   head. **Needs a backend with row locks.** On SQLite the
                   `select_for_update()` is compiled away, so this check prints
                   `skipped: SQLite has no row locks` and runs one worker
                   instead — which still leaves a real cursor for [2] and [3].
  [2] FORWARD ONLY — a manual rewind raises `CursorMovedBackwards` and writes
                   nothing. The lock is not what protects this; the commit is
                   forward-only on its own.
  [3] RESUME     — append five events and poll from the saved cursor: five
                   events come back, not the 205 in the log. This is the replay
                   cost, as an event count — elapsed time would be a flaky way
                   to say it.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from typing import Any

from django.conf import settings
from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError
from django.db import connection

from django_rakaia import django_consumer
from django_rakaia.models import ConsumerCursor, ConsumerOutcome
from django_rakaia.store import get_store
from django_rakaia.subscription import (
    CursorMovedBackwards,
    commit_cursor,
    load_cursor,
    poll_consumer,
)
from rakaia import seed_stream
from workers.consume import apply_event
from workers.models import AppliedEvent
from workers.seed import BACKLOG, CONSUMER, STREAM, TAIL, backlog, tail

#: How long the demo waits for a worker to reach the handshake, and for the
#: second one to be seen waiting at the cursor row. Generous on purpose: this
#: bounds a hang, it is not an assertion about how long anything takes.
START_TIMEOUT_SECONDS = 60.0
HANDOFF_TIMEOUT_SECONDS = 60.0
WORKER_TIMEOUT_SECONDS = 300.0


class Command(BaseCommand):
    help = "Two workers under one consumer name: the cursor row lock, and resuming."

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        # Self-contained: ensure this demo's tables exist so `manage.py
        # demo_two_workers` works when run directly, not only via the
        # migrate-first `just` recipe. Idempotent — a no-op once applied.
        call_command("migrate", verbosity=0, interactive=False)
        store = get_store()
        self._reset(store)

        if connection.features.has_select_for_update:
            self._check_take_turns(store)
        else:
            self._sqlite_has_no_locks(store)

        self._check_forward_only(store)
        self._check_resume(store)

        self.stdout.write(self.style.SUCCESS("\nAll two-worker checks passed ✓"))

    # -- setup --------------------------------------------------------------

    def _reset(self, store: Any) -> None:
        store.delete(STREAM)
        seed_stream(STREAM, backlog(), store=store)
        AppliedEvent.objects.all().delete()
        ConsumerOutcome.objects.all().delete()
        ConsumerCursor.objects.filter(consumer_id=CONSUMER, stream_path=STREAM).delete()
        # The cursor row is created up front, empty. Without it, two workers that
        # start together make the *second* wait on the INSERT rather than on the
        # row lock — which would look identical and prove nothing about
        # select_for_update(). An empty offset is not a position, so a worker
        # still starts from the beginning of the log.
        ConsumerCursor.objects.create(consumer_id=CONSUMER, stream_path=STREAM)
        self.stdout.write(
            f"Seeded {BACKLOG} booking events on {STREAM!r} for consumer "
            f"{CONSUMER!r}, cursor row created empty.\n"
        )

    # -- [1] two processes, one consumer name -------------------------------

    def _check_take_turns(self, store: Any) -> None:
        with tempfile.TemporaryDirectory(prefix="rakaia-two-workers-") as tmp:
            workdir = Path(tmp)
            marker = workdir / "a-holding"
            release = workdir / "release"
            reports = {role: workdir / f"{role}.json" for role in ("a", "b")}

            first = self._spawn("a", reports["a"], hold=marker, release=release)
            self._await_marker(marker, first)
            second = self._spawn("b", reports["b"])
            waited = self._seen_waiting(second)
            release.touch()
            # Join both before judging either, so a failure below cannot leave
            # one process running against a directory that is about to go.
            outputs = {
                role: worker.communicate(timeout=WORKER_TIMEOUT_SECONDS)[0]
                for role, worker in (("a", first), ("b", second))
            }
            a = self._report("a", first, reports["a"], outputs["a"])
            b = self._report("b", second, reports["b"], outputs["b"])
            expected = [event["key"] for event in backlog()]
            head = store.get_current_offset(STREAM)

        self.stdout.write("[1] TAKE TURNS — two processes, one consumer name:")
        for report in (a, b):
            self.stdout.write(
                f"    worker {report['role']}: {len(report['applied'])} applied, "
                f"status {report['status']}, cursor {report['cursor_after']}"
            )
        self.stdout.write(
            "    the second worker was observed blocking while the first held the "
            "pass (pg_stat_activity: wait_event_type=Lock)"
            if waited
            else "    the second worker was NEVER seen blocking — it did not wait"
        )

        applied_a = a["applied"]
        applied_b = b["applied"]

        if not waited:
            raise CommandError(
                "the second worker ran without blocking while the first held the "
                "pass — the cursor row was not locked"
            )
        # The poll status is the structural evidence, independent of what
        # `pg_stat_activity` happened to show: `fresh` means the cursor was
        # still empty when that worker looked, and `caught_up` means it read a
        # position the other worker had already committed. Only the second
        # reading is possible if it waited.
        if a["status"] != "fresh":
            raise CommandError(
                f"the first worker should have found an empty cursor, status was "
                f"{a['status']}"
            )
        if b["status"] != "caught_up":
            raise CommandError(
                f"the second worker polled as {b['status']} — it read the cursor "
                f"before the first committed, so it did not wait for the pass"
            )
        if b["halted"] or a["halted"]:
            raise CommandError(f"a worker halted: a={a['halted']} b={b['halted']}")
        if applied_a != expected:
            raise CommandError(
                f"the first worker did not apply the whole backlog "
                f"({len(applied_a)} of {len(expected)})"
            )
        if applied_b:
            raise CommandError(
                f"the second worker applied {len(applied_b)} events the first was "
                f"already applying: {applied_b[:5]}..."
            )
        duplicated = len(applied_a) + len(applied_b) - len(set(applied_a + applied_b))
        if duplicated:
            raise CommandError(f"{duplicated} events were applied by both workers")
        if AppliedEvent.objects.count() != BACKLOG:
            raise CommandError(
                f"expected {BACKLOG} projected rows, got {AppliedEvent.objects.count()}"
            )
        if not (
            a["cursor_after"] == b["cursor_after"] == load_cursor(CONSUMER, STREAM)
        ):
            raise CommandError(
                f"the workers disagree about the cursor: "
                f"a={a['cursor_after']} b={b['cursor_after']} "
                f"stored={load_cursor(CONSUMER, STREAM)}"
            )
        if load_cursor(CONSUMER, STREAM) != head:
            raise CommandError(
                f"the cursor did not reach the head: "
                f"{load_cursor(CONSUMER, STREAM)} != {head}"
            )
        self.stdout.write(
            self.style.SUCCESS(
                f"    → {BACKLOG} events, {BACKLOG} rows, each applied once; both "
                f"workers finished and the cursor moved forward to the head ✓"
            )
        )

    def _spawn(
        self,
        role: str,
        result: Path,
        *,
        hold: Path | None = None,
        release: Path | None = None,
    ) -> subprocess.Popen[str]:
        command = [
            sys.executable,
            "manage.py",
            "worker_pass",
            "--role",
            role,
            "--result",
            str(result),
        ]
        if hold is not None:
            command += ["--hold-marker", str(hold)]
        if release is not None:
            command += ["--release-marker", str(release)]
        return subprocess.Popen(
            command,
            cwd=settings.BASE_DIR,
            env=os.environ.copy(),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

    def _await_marker(self, marker: Path, worker: subprocess.Popen[str]) -> None:
        deadline = time.monotonic() + START_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            if marker.exists():
                return
            if worker.poll() is not None:
                output = worker.stdout.read() if worker.stdout else ""
                raise CommandError(
                    f"the first worker finished before it was ever contended: {output}"
                )
            time.sleep(0.02)
        worker.kill()
        worker.wait(timeout=10)
        raise CommandError("the first worker never reached the hold marker")

    def _seen_waiting(self, worker: subprocess.Popen[str]) -> bool:
        """True once the database itself reports another backend waiting on a lock.

        Asking `pg_stat_activity` is what makes "it waited" a measurement rather
        than a duration: a blocked backend appears there with
        ``wait_event_type = 'Lock'`` however long it has been blocked. The second
        worker cannot take any other lock first — `locked_cursor` is the first
        thing a pass does — so a lock wait here is the cursor row.
        """
        deadline = time.monotonic() + HANDOFF_TIMEOUT_SECONDS
        with connection.cursor() as cursor:
            while time.monotonic() < deadline:
                if worker.poll() is not None:
                    return False  # it finished, so it never blocked
                cursor.execute(
                    "SELECT count(*) FROM pg_stat_activity "
                    "WHERE wait_event_type = 'Lock' AND pid <> pg_backend_pid()"
                )
                if cursor.fetchone()[0]:
                    return True
                time.sleep(0.05)
        return False

    def _report(
        self, role: str, worker: subprocess.Popen[str], result: Path, output: str
    ) -> dict[str, Any]:
        if worker.returncode != 0:
            raise CommandError(f"worker {role} failed:\n{output}")
        if not result.exists():
            raise CommandError(f"worker {role} wrote no report:\n{output}")
        return json.loads(result.read_text())

    # -- the SQLite leg -----------------------------------------------------

    def _sqlite_has_no_locks(self, store: Any) -> None:
        """One worker instead of two, and say why the other half is missing."""
        self.stdout.write(
            "[1] TAKE TURNS — skipped: SQLite has no row locks\n"
            "    select_for_update() compiles to a plain SELECT here, so a "
            "second\n    worker would not wait for the first and the events "
            "would be applied\n    twice. Run this demo against Postgres "
            "(`just two-workers-demo-pg`)\n    to see it for real."
        )
        # Still run one pass: [2] needs a committed position to rewind from,
        # and the forward-only commit and the resume read are both real here.
        result = django_consumer(store, STREAM, CONSUMER).run(
            lambda message: apply_event("a", message), on_error="halt"
        )
        head = store.get_current_offset(STREAM)
        if result.applied != BACKLOG or load_cursor(CONSUMER, STREAM) != head:
            raise CommandError(
                f"one worker should apply {BACKLOG} and commit {head}, got "
                f"{result.applied} and {load_cursor(CONSUMER, STREAM)}"
            )
        self.stdout.write(
            f"    one worker, no contention: {result.applied} applied, status "
            f"{result.status}, cursor at the head."
        )

    # -- [2] the commit is forward-only -------------------------------------

    def _check_forward_only(self, store: Any) -> None:
        messages, _ = store.read(STREAM)
        earliest = messages[0].offset
        saved = load_cursor(CONSUMER, STREAM)

        self.stdout.write("\n[2] FORWARD ONLY — try to move the cursor back:")
        self.stdout.write(f"    committed at {saved}; rewinding to {earliest}")
        try:
            commit_cursor(CONSUMER, STREAM, earliest)
        except CursorMovedBackwards as exc:
            self.stdout.write(f"    CursorMovedBackwards: {exc}")
        else:
            raise CommandError(
                "a commit behind the saved position was accepted — the events "
                "would be applied twice"
            )
        if load_cursor(CONSUMER, STREAM) != saved:
            raise CommandError(
                f"the refused rewind still moved the cursor to "
                f"{load_cursor(CONSUMER, STREAM)}"
            )
        self.stdout.write(
            self.style.SUCCESS(
                "    → the rewind raised and wrote nothing: the watermark "
                "cannot go back over applied events ✓"
            )
        )

    # -- [3] resume reads the delta, not the log ----------------------------

    def _check_resume(self, store: Any) -> None:
        saved = load_cursor(CONSUMER, STREAM)
        seed_stream(STREAM, tail(), store=store)
        log_length = len(store.read(STREAM)[0])
        polled = poll_consumer(store, CONSUMER, STREAM)
        keys = [json.loads(message.data)["key"] for message in polled.messages]

        self.stdout.write("\n[3] RESUME — append five events and poll from the cursor:")
        self.stdout.write(
            f"    log now holds {log_length} events; cursor at {saved}; "
            f"poll status {polled.status}"
        )
        self.stdout.write(f"    read {len(polled.messages)} events: {keys}")
        if [event["key"] for event in tail()] != keys:
            raise CommandError(f"the resumed read returned the wrong events: {keys}")
        if len(polled.messages) != TAIL:
            raise CommandError(
                f"resuming after {log_length - TAIL} applied events read "
                f"{len(polled.messages)} — it should read {TAIL}, not the whole log"
            )
        if log_length != BACKLOG + TAIL:
            raise CommandError(
                f"expected {BACKLOG + TAIL} in the log, got {log_length}"
            )
        self.stdout.write(
            self.style.SUCCESS(
                f"    → {TAIL} events read from a {log_length}-event log: a resume "
                f"is a delta, not a replay ✓"
            )
        )
