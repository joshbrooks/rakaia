"""`manage.py demo_submissions` — the FormKit-Ninja adoption prototype.

This command answers the question that gates adopting rakaia for FormKit-Ninja's
Submissions table: *does re-deriving the typed rows from a rakaia stream produce
the same output as FormKit-Ninja's current `to_model()` write path?*

Because rakaia's StreamStore is in-memory and process-local, seeding and replay
happen in one process. The command runs three passes over the same submissions:

  A. rakaia replay      — stream -> versioned handlers -> DjangoExecutor
  B. reference (t-c)     — direct to_model(), time-correct per seq
  C. reference (naive)   — direct to_model(), always today's rule

Then it asserts and reports:

  1. PARITY:      A == B, row for row. Proves the rakaia plumbing faithfully
                  reproduces a direct to_model() write. Migration is safe.
  2. VALUE-ADD:   A vs C. Counts the historical rows that versioned replay
                  preserves but a naive to_model() re-run would silently
                  rewrite. This is what rakaia buys over plain signals.
"""

from __future__ import annotations

import json
from typing import Any

from django.core.management.base import BaseCommand, CommandParser

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.store import get_store
from rakaia import CollectingExecutor, upcast
from rakaia.replay import replay
from submissions import reference
from submissions.models import ActivityProgress, MonitoringVisit
from submissions.seed import POLICY_CHANGE_SEQ, SAMPLE_SUBMISSIONS

STREAM = "submissions"


def _snapshot() -> dict[str, list[tuple]]:
    """Serialise both projection tables to comparable, order-stable tuples."""
    visits = [
        (
            v.submission_id,
            v.project_code,
            v.suku,
            str(v.total_budget),
            str(v.overall_progress),
            v.status,
        )
        for v in MonitoringVisit.objects.all()
    ]
    activities = [
        (a.submission_id, a.activity_index, a.name, str(a.budget), a.progress_pct)
        for a in ActivityProgress.objects.all()
    ]
    return {"visits": visits, "activities": activities}


def _reset() -> None:
    MonitoringVisit.objects.all().delete()
    ActivityProgress.objects.all().delete()


class Command(BaseCommand):
    help = "Prove rakaia replay reproduces FormKit-Ninja's to_model() output."

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--twice",
            action="store_true",
            help="Replay a second time to demonstrate idempotency.",
        )

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        store = get_store()

        # Reset so the command is re-runnable: fresh stream (seq restarts at 0).
        store.delete(STREAM)
        store.create(STREAM)
        for event in SAMPLE_SUBMISSIONS:
            store.append(STREAM, json.dumps(event).encode("utf-8"))
        self.stdout.write(
            f"Seeded {len(SAMPLE_SUBMISSIONS)} submissions "
            f"(completion policy changes at seq {POLICY_CHANGE_SEQ}).\n"
        )

        # Dry run: what would replay write? A CollectingExecutor records the
        # effects without applying any of them — no DB writes here.
        preview = CollectingExecutor()
        replay(store=store, stream_path=STREAM, executor=preview)
        self.stdout.write(
            f"Dry run: replay would apply {len(preview.effects)} effects "
            f"(no writes yet).\n"
        )

        # ---- Pass A: rakaia replay -> snapshot --------------------------------
        _reset()
        result = replay(store=store, stream_path=STREAM, executor=DjangoExecutor())
        replay_snap = _snapshot()
        self.stdout.write(
            self.style.SUCCESS(
                f"[replay] events={result.events_processed} "
                f"effects_applied={result.effects_applied}"
            )
        )
        self._print_table()

        # Normalise the seed the same way replay does (upcast to current
        # schema), so the reference path sees identical input — isolating the
        # plumbing under test.
        normalised = [upcast(e, STREAM) for e in SAMPLE_SUBMISSIONS]

        # ---- Pass B: reference to_model(), time-correct -> snapshot -----------
        _reset()
        reference.populate(normalised, policy_seq=POLICY_CHANGE_SEQ)
        reference_snap = _snapshot()

        # ---- Pass C: reference to_model(), always-current -> snapshot ---------
        _reset()
        reference.populate(normalised, policy_seq=None)
        naive_snap = _snapshot()

        # Restore the (correct) replay projection for anyone inspecting the DB.
        _reset()
        replay(store=store, stream_path=STREAM, executor=DjangoExecutor())

        self._report_parity(replay_snap, reference_snap)
        self._report_value_add(replay_snap, naive_snap)
        self._demo_reconcile(store)

        if opts["twice"]:
            before = MonitoringVisit.objects.count()
            replay(store=store, stream_path=STREAM, executor=DjangoExecutor())
            after = MonitoringVisit.objects.count()
            ok = before == after
            style = self.style.SUCCESS if ok else self.style.ERROR
            self.stdout.write(
                style(
                    f"\nReplayed again: {before} -> {after} visit rows — "
                    f"{'idempotent ✓' if ok else 'NOT idempotent ✗'}"
                )
            )

    def _demo_reconcile(self, store: Any) -> None:
        # Append a correction: IR-108 resubmitted with ONE activity (it had
        # two). Replaying the full stream must prune the dropped child row —
        # reconcile_children's reason to exist. A naive fan-out would orphan it.
        corrected = {
            "schema_version": 2,
            "form_type": "monitoring_visit",
            "submission_id": "44444444-4444-4444-4444-444444444444",
            "fields": {
                "project_code": "IR-108",
                "suku": "Bobonaro",
                "monitor": "soares",
                "visit_date": "2025-06-20",
                "activities": [
                    {"name": "Canal lining", "budget": "8000.00", "progress_pct": 100},
                ],
            },
        }
        store.append(STREAM, json.dumps(corrected).encode("utf-8"))
        _reset()
        replay(store=store, stream_path=STREAM, executor=DjangoExecutor())

        count = ActivityProgress.objects.filter(
            submission_id=corrected["submission_id"]
        ).count()
        ok = count == 1
        style = self.style.SUCCESS if ok else self.style.ERROR
        self.stdout.write(
            style(
                f"\n[3] RECONCILE: IR-108 resubmitted with 1 activity (was 2) "
                f"-> {count} row(s) — {'no orphan ✓' if ok else 'ORPHAN ✗'}"
            )
        )

    # -- reporting ----------------------------------------------------------

    def _report_parity(self, replay_snap: dict, reference_snap: dict) -> None:
        ok = replay_snap == reference_snap
        style = self.style.SUCCESS if ok else self.style.ERROR
        self.stdout.write(
            style(
                "\n[1] PARITY: rakaia replay == direct to_model() — "
                + ("identical ✓" if ok else "DIVERGED ✗")
            )
        )
        if not ok:  # pragma: no cover - defensive; should not happen
            for key in ("visits", "activities"):
                r, ref = replay_snap[key], reference_snap[key]
                for a, b in zip(r, ref, strict=True):
                    if a != b:
                        self.stdout.write(self.style.ERROR(f"  {key}: {a} != {b}"))

    def _report_value_add(self, replay_snap: dict, naive_snap: dict) -> None:
        drifted = [
            # naive row `n`, time-correct replay row `t`; status is index 5.
            (n[0], n[5], t[5])  # submission_id, naive status, time-correct status
            for n, t in zip(naive_snap["visits"], replay_snap["visits"], strict=True)
            if n != t
        ]
        self.stdout.write(
            self.style.WARNING(
                f"\n[2] VALUE-ADD: a naive to_model() re-run would rewrite "
                f"{len(drifted)} historical visit row(s) that versioned replay "
                f"preserves:"
            )
        )
        for submission_id, naive_status, correct_status in drifted:
            self.stdout.write(
                f"    {submission_id[:8]}…  naive={naive_status}  "
                f"time-correct={correct_status}"
            )

    def _print_table(self) -> None:
        header = (
            f"\n{'submission':<12}{'project':<9}{'suku':<12}"
            f"{'budget':>10}{'progress':>10}{'status':>13}"
        )
        self.stdout.write(header)
        self.stdout.write("-" * len(header.strip("\n")))
        for v in MonitoringVisit.objects.all():
            self.stdout.write(
                f"{v.submission_id[:8]:<12}{v.project_code:<9}{v.suku:<12}"
                f"{v.total_budget:>10}{v.overall_progress:>10}{v.status:>13}"
            )
