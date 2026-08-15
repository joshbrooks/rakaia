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

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError, CommandParser

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.store import get_store
from rakaia import (
    AppendOptions,
    CollectingExecutor,
    envelope_actor,
    history_effects,
    label_marker,
    provenance,
    upcast,
)
from rakaia.replay import replay
from submissions import reference
from submissions.models import ActivityProgress, MonitoringVisit, SubmissionHistory
from submissions.seed import POLICY_CHANGE_SEQ, SAMPLE_SUBMISSIONS

STREAM = "submissions"
HISTORY_MODEL = "submissions.SubmissionHistory"
IR108_ID = "44444444-4444-4444-4444-444444444444"


def _append_event(store: Any, event: dict, *, label: str, actor: str) -> None:
    """Append one *enveloped* event: the raw payload plus a change label and the
    acting user — exactly what `ProvenanceMiddleware` stamps on a real request.
    The label drives the `/history` marker; the ambient `provenance(user=…)` is
    merged into the message metadata so the audit read-model can recover *who*.
    """
    with provenance(user=actor):
        store.append(
            STREAM,
            json.dumps(event).encode("utf-8"),
            AppendOptions(label=label),
        )


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
        # Self-contained: ensure this demo's tables exist so `manage.py
        # demo_*` works when run directly, not only via the migrate-first
        # `just` recipe. Idempotent — a no-op once migrations are applied.
        call_command("migrate", verbosity=0, interactive=False)
        store = get_store()

        # Reset so the command is re-runnable: fresh stream (seq restarts at 0).
        store.delete(STREAM)
        store.create(STREAM)
        for event in SAMPLE_SUBMISSIONS:
            # First recording of each submission -> "create" (a `+` in history),
            # stamped with the monitor who filed it as the acting user.
            _append_event(
                store, event, label="create", actor=event["fields"]["monitor"]
            )
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
        self._demo_history(store)

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
            if not ok:
                raise CommandError(
                    f"replay is not idempotent: {before} -> {after} visit rows"
                )

    def _demo_reconcile(self, store: Any) -> None:
        # Append a correction: IR-108 resubmitted with ONE activity (it had
        # two). Replaying the full stream must prune the dropped child row —
        # reconcile_children's reason to exist. A naive fan-out would orphan it.
        corrected = {
            "schema_version": 2,
            "form_type": "monitoring_visit",
            "submission_id": IR108_ID,
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
        # A later *correction* by a reviewer (not the original monitor): an
        # "update" (a `~` in history) whose actor differs from the create.
        _append_event(store, corrected, label="update", actor="reviewer:tavares")
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
        if not ok:
            raise CommandError(
                f"reconcile left an orphan: expected 1 activity row, got {count}"
            )

    def _demo_history(self, store: Any) -> None:
        """Materialise a streams-native `/history` and check it is equivalent to
        pghistory's output — *plus* the provenance a bulk re-derive would lose.

        The audit read-model is just another fan-out over the same stream: one
        `SubmissionHistory` row per event, keyed by (submission, version), shaped
        from the envelope (`label_marker` for the +/~/- diff marker,
        `envelope_actor` for who). Because the whole stream is read, the default
        version = message index is stable, so re-materialising is idempotent.
        """
        messages, _ = store.read(STREAM)
        effects = history_effects(
            messages,
            HISTORY_MODEL,
            subject_field="submission_id",
            subject_of=lambda e: e["submission_id"],
            defaults_of=lambda msg, event: {
                "marker": label_marker(msg.label),
                "actor": envelope_actor(msg, event),
                "label": msg.label,
                "ts": msg.timestamp,
                "snapshot": event,
            },
        )
        SubmissionHistory.objects.all().delete()
        DjangoExecutor().apply(effects)
        rows = list(SubmissionHistory.objects.all())

        # [4a] Faithful capture: every recorded change is stored with its source
        # snapshot intact and the correct diff marker — the property that lets
        # the audit log reconstruct any historical state (pghistory's core
        # guarantee). Snapshots must round-trip the source event byte-for-byte,
        # and the markers must reflect the labels: four creates (+) and the one
        # correction (~). (Unlike a bare count, this fails if the projection
        # drops a field, mis-keys a row, or maps a label to the wrong marker.)
        events_by_version = {i: json.loads(m.data) for i, m in enumerate(messages)}
        snapshots_faithful = all(
            r.snapshot == events_by_version[r.version] for r in rows
        )
        markers_ok = sorted(r.marker for r in rows) == ["+", "+", "+", "+", "~"]
        # [4b] Provenance: every change carries the acting user captured at write
        # time. pghistory only gets this when a request middleware is in scope;
        # here it rides the envelope, so it is never silently null.
        actors_ok = all(r.actor for r in rows)
        # [4c] The correction is a distinct, time-stamped version of IR-108 with a
        # *different* actor (reviewer, not the original monitor) — the per-save
        # attribution a naive reconcile_separated_submissions re-derive destroys.
        ir108 = [r for r in rows if r.submission_id == IR108_ID]
        correction_ok = (
            len(ir108) == 2
            and ir108[0].marker == "+"
            and ir108[1].marker == "~"
            and ir108[0].actor != ir108[1].actor
        )

        ok = snapshots_faithful and markers_ok and actors_ok and correction_ok
        style = self.style.SUCCESS if ok else self.style.ERROR
        self.stdout.write(
            style(
                f"\n[4] HISTORY: {len(rows)} audit rows from {len(messages)} events "
                f"— streams-native /history, provenance captured "
                f"{'✓' if ok else '✗'}"
            )
        )
        if not ok:
            raise CommandError(
                "history read-model diverged: "
                f"snapshots={snapshots_faithful} markers={markers_ok} "
                f"actors={actors_ok} correction={correction_ok}"
            )
        for r in ir108:
            self.stdout.write(
                f"    {r.submission_id[:8]}…  v{r.version}  {r.marker}  by {r.actor}"
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
        if not ok:
            for key in ("visits", "activities"):
                r, ref = replay_snap[key], reference_snap[key]
                for a, b in zip(r, ref, strict=True):
                    if a != b:
                        self.stdout.write(self.style.ERROR(f"  {key}: {a} != {b}"))
            raise CommandError(
                "replay does not reproduce direct to_model() — the whole claim "
                "of this example"
            )

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
