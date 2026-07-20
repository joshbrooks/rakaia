"""`manage.py demo_submission_stream` — the converged-design spike (Decision #13).

Proves the arrow-flip: `SubmissionEvent` (append log = source of truth) →
`Submission` (latest-version projection). Each save appends an event and
reprojects in one transaction; `/history` is the ordered log; a direct write to
the projection is ephemeral because durable state *is* the events.

Runs on sqlite, so the Postgres coverage guard (Decision #10) is out of scope —
which is the point: in this topology the guard is belt-and-suspenders, not
load-bearing.
"""

from __future__ import annotations

import contextlib
import json
from typing import Any

from django.core.management.base import BaseCommand

from submission_stream import stream
from submission_stream.models import Submission, SubmissionHistory

A = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
B = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"


class Command(BaseCommand):
    help = "Spike: SubmissionEvent (append log) -> Submission (latest projection)."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument(
            "--reproject-only",
            action="store_true",
            help="Don't seed; rebuild Submission from the persisted event log "
            "(run in a second process to prove the log is durable).",
        )

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        store = stream.get_store()

        if opts["reproject_only"]:
            # Fresh process, no seeding: the SubmissionEvent log is the durable
            # source of truth, so rebuilding from it reconstructs current state.
            Submission.objects.all().delete()
            stream.reproject_all(store)
            self._print_current()
            # After a full seed run A is tombstoned (deleted) and B survives as
            # the latest import — both facts must be reconstructable from the log.
            a_gone = not Submission.objects.filter(key=A).exists()
            b = Submission.objects.filter(key=B).first()
            ok = a_gone and b is not None and b.status == 2
            self._say(
                ok,
                "[6] DURABILITY: rebuilt from the persisted SubmissionEvent log in "
                "a fresh process — B survived, A stayed tombstoned",
            )
            return

        with contextlib.suppress(KeyError):
            store.delete(stream.STREAM)
        store.create(stream.STREAM)
        Submission.objects.all().delete()
        SubmissionHistory.objects.all().delete()

        # -- write path: each save = append event + reproject, one transaction --
        stream.record_submission(
            store,
            A,
            fields={"title": "Well A", "budget": "100"},
            status=0,
            actor="amaral",
            url="/submit/A",
            label="create",
        )
        stream.record_submission(
            store,
            B,
            fields={"title": "Road B"},
            status=0,
            actor="guterres",
            url="/submit/B",
            label="create",
        )
        stream.record_submission(
            store,
            A,
            fields={"title": "Well A", "budget": "150"},
            status=1,
            actor="reviewer:tavares",
            url="/verify/A",
            label="verify",
        )
        self.stdout.write(
            "Recorded 3 events across 2 submissions (A created+verified, B created).\n"
        )
        self._print_current()

        # [1] APPEND -> PROJECT (latest wins) + REPLAY (pure function of the log)
        a = Submission.objects.get(key=A)
        latest_ok = (
            a.fields.get("budget") == "150"
            and a.status == 1
            and a.user == "reviewer:tavares"
        )
        before = self._snap()
        stream.reproject_all(store)  # rebuild from scratch
        replay_ok = before == self._snap()
        self._say(
            latest_ok and replay_ok,
            "[1] APPEND->PROJECT: A resolves to its latest event (budget=150, "
            "status=1, by reviewer); replay from scratch reproduces the projection",
        )

        # [2] HISTORY == THE LOG
        stream.materialize_history(store)
        messages, _ = store.read(stream.STREAM)
        rows = list(SubmissionHistory.objects.all())
        a_hist = [r for r in rows if r.key == A]
        history_ok = (
            len(rows) == len(messages)  # one audit row per event
            and [r.marker for r in a_hist] == ["+", "~"]  # create then verify
            and a_hist[0].actor == "amaral"
            and a_hist[1].actor == "reviewer:tavares"
            and a_hist[1].url == "/verify/A"  # provenance captured on append
        )
        self._say(
            history_ok,
            "[2] HISTORY == the log: one audit row per event; marker / actor / "
            "url recovered from the envelope",
        )
        for r in a_hist:
            self.stdout.write(
                f"    {r.key[:8]}  v{r.version}  {r.marker}  status={r.status}  "
                f"by {r.actor}  @ {r.url}"
            )

        # [3] SELF-HEALING: a direct write to the projection is ephemeral
        Submission.objects.filter(key=A).update(fields={"tampered": True}, status=9)
        tampered = Submission.objects.get(key=A).status == 9
        stream.reproject_all(store)
        healed = Submission.objects.get(key=A)
        heal_ok = (
            tampered and healed.status == 1 and healed.fields.get("budget") == "150"
        )
        self._say(
            heal_ok,
            "[3] SELF-HEALING: a direct Submission write is overwritten by "
            "reprojection (durable state == the event log, not the row)",
        )

        # [4] MODE B: a context-less write still records a full event
        stream.record_submission(
            store, B, fields={"title": "Road B"}, status=2, label="import"
        )  # no actor / url
        stream.materialize_history(store)
        b_last = SubmissionHistory.objects.filter(key=B).order_by("version").last()
        modeb_ok = (
            b_last is not None
            and b_last.actor is None
            and b_last.snapshot.get("status") == 2
            and b_last.marker == "~"
        )
        self._say(
            modeb_ok,
            "[4] MODE B: a context-less import still logs a full event; actor is "
            "null (graceful), snapshot intact — the pghistory-equivalent case",
        )

        # [5] TOMBSTONE: a delete event removes the row but stays in history
        stream.record_submission(
            store,
            A,
            fields={"title": "Well A", "budget": "150"},
            status=1,
            actor="reviewer:tavares",
            url="/delete/A",
            label="delete",
        )
        stream.materialize_history(store)
        gone = not Submission.objects.filter(key=A).exists()
        a_markers = list(
            SubmissionHistory.objects.filter(key=A)
            .order_by("version")
            .values_list("marker", flat=True)
        )
        tomb_ok = gone and a_markers == ["+", "~", "-"]
        self._say(
            tomb_ok,
            "[5] TOMBSTONE: a delete removes A's projection row, but the log keeps "
            f"the full create/verify/delete trail {a_markers}",
        )

    # -- helpers ------------------------------------------------------------

    def _snap(self) -> list[tuple]:
        return [
            (s.key, json.dumps(s.fields, sort_keys=True), s.status, s.user, s.version)
            for s in Submission.objects.all()
        ]

    def _print_current(self) -> None:
        self.stdout.write(f"{'submission':<12}{'status':>7}{'user':>18}   fields")
        self.stdout.write("-" * 60)
        for s in Submission.objects.all():
            self.stdout.write(
                f"{s.key[:8]:<12}{s.status:>7}{str(s.user):>18}   {json.dumps(s.fields)}"
            )
        self.stdout.write("")

    def _say(self, ok: bool, msg: str) -> None:
        style = self.style.SUCCESS if ok else self.style.ERROR
        self.stdout.write(style("\n" + msg + " " + ("✓" if ok else "✗")))
