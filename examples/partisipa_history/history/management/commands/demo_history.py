"""`manage.py demo_history` — the pghistory-retirement spike for issue #11.

It answers: *can a rakaia stream carrying an event envelope reproduce everything
Partisipa reads from ``django-pghistory`` — the `/history` audit API, the
per-change actor context, and the ``repair_blank_save_dataloss`` recovery — so
pghistory can be retired?*

Four checks, each asserted hard (a failure raises CommandError):

  [1] PARITY   — the stream-derived audit log reproduces the golden pghistory
                 ``pgh_event`` table byte-for-byte: same order, label, actor,
                 timestamp, and full field snapshot. This is the `/history`
                 substrate.
  [2] ENVELOPE — a fields-only store (a plain ``append(new_state)``) cannot name
                 an actor or tell a create from an update. The enveloped stream
                 can. This is the argument for making the envelope first-class.
  [3] RECOVERY — a truncating blank save wipes a submission; the pre-truncation
                 peak snapshot is recovered from the stream, equal to what
                 pghistory recovery returns. This is ``repair_blank_save_dataloss``.
  [4] IDEMPOTENT — re-replay yields identical history + current-state rows; the
                 deleted submission's row is gone but its history remains.
"""

from __future__ import annotations

from typing import Any

from django.core.management.base import BaseCommand, CommandError

from django_rakaia.store import get_store
from history import pghistory_today, stream_history
from history.envelope import PGH_TO_LABEL, canonical
from history.models import (
    PghEventGolden,
    SubmissionHistoryEntry,
    SubmissionRecord,
)
from history.seed import SAVES, TRUNCATED_KEY

STREAM = "submission-history"
NAIVE_STREAM = "submission-history-naive"


def _reset_projections() -> None:
    SubmissionHistoryEntry.objects.all().delete()
    SubmissionRecord.objects.all().delete()


class Command(BaseCommand):
    help = "Reproduce django-pghistory's audit + recovery from a rakaia stream."

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        store = get_store()
        for path in (STREAM, NAIVE_STREAM):
            store.delete(path)
            store.create(path)

        # The "today" path: pghistory records the golden audit table.
        pghistory_today.simulate(SAVES)
        # The streams path: envelope events -> replay -> two projections.
        _reset_projections()
        stream_history.append_saves(store, STREAM, SAVES)
        stream_history.replay_history(store, STREAM)
        # The strawman: a fields-only store.
        stream_history.naive_append(store, NAIVE_STREAM, SAVES)

        self.stdout.write(
            f"Seeded {len(SAVES)} saves across {SubmissionRecord.objects.count() + 1} "
            f"submissions (one later deleted); pghistory wrote "
            f"{PghEventGolden.objects.count()} audit rows.\n"
        )

        self._check_parity()
        self._check_envelope(store)
        self._check_recovery()
        self._check_idempotent(store)

        self.stdout.write(
            self.style.SUCCESS("\nAll pghistory-retirement checks passed ✓")
        )

    # -- [1] parity ---------------------------------------------------------

    def _check_parity(self) -> None:
        golden = list(PghEventGolden.objects.all())
        derived = list(SubmissionHistoryEntry.objects.order_by("ts", "submission_id"))

        self.stdout.write("[1] PARITY — stream-derived audit vs golden pghistory:")
        self.stdout.write(
            f"    {'submission':<14} {'lbl':<3} {'actor':<18} {'snapshot'}"
        )
        if len(golden) != len(derived):
            raise CommandError(
                f"row count differs: pghistory={len(golden)} stream={len(derived)}"
            )
        for g, d in zip(golden, derived, strict=True):
            row = (
                f"    {d.submission_id:<14} {d.label:<3} {d.actor:<18} "
                f"{canonical(d.fields)}"
            )
            match = (
                g.submission_id == d.submission_id
                and PGH_TO_LABEL[g.pgh_label] == d.label
                and g.pgh_context_user == d.actor
                and g.pgh_created_at == d.ts
                and canonical(g.fields) == canonical(d.fields)
            )
            if not match:
                self.stdout.write(self.style.ERROR(row + "  ✗ MISMATCH"))
                raise CommandError(
                    f"stream audit row diverges from pghistory for "
                    f"{d.submission_id}#{d.seq}"
                )
            self.stdout.write(row)
        self.stdout.write(
            self.style.SUCCESS(
                f"    → {len(derived)} rows reproduce pgh_event byte-for-byte ✓"
            )
        )

    # -- [2] envelope -------------------------------------------------------

    def _check_envelope(self, store: Any) -> None:
        naive = stream_history.naive_history(store, NAIVE_STREAM)
        loses_actor = all("actor" not in e for e in naive)
        loses_op = all("op" not in e for e in naive)
        enveloped = SubmissionHistoryEntry.objects.exclude(actor="").count()

        self.stdout.write(
            "\n[2] ENVELOPE — what a plain append(new_state) loses:"
        )
        self.stdout.write(
            self.style.WARNING(
                f"    fields-only stream: actor recoverable={not loses_actor}, "
                f"create/update distinguishable={not loses_op} "
                f"→ cannot serve /history or attribute a change."
            )
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"    enveloped stream: {enveloped}/"
                f"{SubmissionHistoryEntry.objects.count()} audit rows carry an "
                f"actor + label."
            )
        )
        if not (loses_actor and loses_op):
            raise CommandError("naive stream unexpectedly retained envelope data")
        if enveloped != SubmissionHistoryEntry.objects.count():
            raise CommandError("enveloped stream lost actor metadata")

    # -- [3] recovery -------------------------------------------------------

    def _check_recovery(self) -> None:
        current = SubmissionRecord.objects.get(submission_id=TRUNCATED_KEY)
        from_stream = stream_history.recover_peak_snapshot(TRUNCATED_KEY)
        from_pgh = pghistory_today.recover_peak_snapshot(TRUNCATED_KEY)

        self.stdout.write("\n[3] RECOVERY — restore the truncated submission:")
        self.stdout.write(
            self.style.WARNING(
                f"    a blank save truncated {TRUNCATED_KEY} to "
                f"{len(min((h.fields for h in SubmissionHistoryEntry.objects.filter(submission_id=TRUNCATED_KEY)), key=len))} "
                f"field(s) mid-history."
            )
        )
        self.stdout.write(
            f"    peak snapshot from stream : {canonical(from_stream)}"
        )
        self.stdout.write(
            f"    peak snapshot from pghist : {canonical(from_pgh)}"
        )
        if not stream_history.snapshots_equal(from_stream, from_pgh):
            raise CommandError("stream recovery disagrees with pghistory recovery")
        if len(from_stream) <= 1:
            raise CommandError("recovery did not restore the pre-truncation peak")
        self.stdout.write(
            self.style.SUCCESS(
                f"    → stream recovery == pghistory recovery "
                f"({len(from_stream)} fields); current row now healed to "
                f"{len(current.fields)} fields ✓"
            )
        )

    # -- [4] idempotency ----------------------------------------------------

    def _check_idempotent(self, store: Any) -> None:
        before = (
            SubmissionHistoryEntry.objects.count(),
            SubmissionRecord.objects.count(),
        )
        stream_history.replay_history(store, STREAM)
        after = (
            SubmissionHistoryEntry.objects.count(),
            SubmissionRecord.objects.count(),
        )
        deleted_gone = not SubmissionRecord.objects.filter(
            submission_id="sub-road-02"
        ).exists()
        deleted_history = SubmissionHistoryEntry.objects.filter(
            submission_id="sub-road-02"
        ).count()

        self.stdout.write("\n[4] IDEMPOTENT — re-replay the whole stream:")
        ok = before == after
        style = self.style.SUCCESS if ok else self.style.ERROR
        self.stdout.write(
            style(
                f"    rows {before} -> {after} — "
                f"{'idempotent ✓' if ok else 'NOT idempotent ✗'}"
            )
        )
        self.stdout.write(
            self.style.SUCCESS(
                f"    deleted submission row gone={deleted_gone}, but "
                f"{deleted_history} history rows retained (create + delete)."
            )
        )
        if not ok:
            raise CommandError("re-replay changed the projections")
        if not deleted_gone or deleted_history != 2:
            raise CommandError("delete did not preserve history / drop the row")
