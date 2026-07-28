"""`manage.py demo_close` — the close-precondition state-machine spike.

It answers: *can a replay decide a cross-form state transition — Partisipa's
POM_1 cycle close, gated by `close_preconditions.py` — as a pure function of the
projected state, reject it with specific reasons when preconditions fail, and
self-heal to ACCEPTED when the missing facts arrive, with no bespoke re-check?*

Four checks, each asserted hard (a failure raises CommandError):

  [1] GATE       — replay evaluates each POM_1 close against the projections.
                   Fatuberliu (all preconditions met) is ACCEPTED; Maubara is
                   REJECTED with exactly its three failing reasons. A vacuous
                   guard (always accept) fails here.
  [2] SELF-HEAL  — append the events that fix Maubara's three failures and
                   re-replay: the same POM_1 close flips to ACCEPTED with no
                   backfill task and no code change.
  [3] REPLAY-SAFE — the Balance aggregate is recomputed, not incremented, so
                   re-replaying does not double-count: Maubara's operational
                   balance stays put across repeated replays.
  [4] DETERMINISTIC — re-replay leaves every CycleClose (status + reasons)
                   byte-identical.
"""

from __future__ import annotations

import json
from decimal import Decimal
from typing import Any

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError

from django_rakaia.store import get_store
from lifecycle import staged_replay as sr
from lifecycle.handlers import STAGES
from lifecycle.models import (
    Balance,
    CycleClose,
    FinanceLine,
    Meeting,
    Project,
)
from lifecycle.seed import EXPECTED_INITIAL, HEAL_EVENTS, SAMPLE_EVENTS

STREAM = "subproject-lifecycle"


def _reset() -> None:
    for model in (CycleClose, Balance, FinanceLine, Meeting, Project):
        model.objects.all().delete()


def _append(store: Any, event: dict) -> None:
    store.append(STREAM, json.dumps(event).encode("utf-8"))


def _close_state() -> dict[str, tuple[str, list]]:
    return {
        c.suku: (c.status, sorted(c.reasons)) for c in CycleClose.objects.all()
    }


def _balance(suku: str) -> tuple[Decimal, Decimal] | None:
    b = Balance.objects.filter(suku=suku).first()
    return None if b is None else (b.operational, b.infrastructure)


class Command(BaseCommand):
    help = "Decide a POM_1 cycle close from cross-form preconditions via replay."

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        # Self-contained: ensure this demo's tables exist so `manage.py
        # demo_*` works when run directly, not only via the migrate-first
        # `just` recipe. Idempotent — a no-op once migrations are applied.
        call_command("migrate", verbosity=0, interactive=False)
        store = get_store()
        store.delete(STREAM)
        store.create(STREAM)
        for event in SAMPLE_EVENTS:
            _append(store, event)
        _reset()
        sr.staged_replay(store, STREAM, STAGES)
        self.stdout.write(
            f"Seeded {len(SAMPLE_EVENTS)} events for 2 sukus, each ending in a "
            f"POM_1 close request.\n"
        )

        self._check_gate()
        self._check_self_heal(store)
        self._check_replay_safe(store)
        self._check_deterministic(store)

        self.stdout.write(self.style.SUCCESS("\nAll close-gate checks passed ✓"))

    # -- [1] gate -----------------------------------------------------------

    def _check_gate(self) -> None:
        state = _close_state()
        self.stdout.write("[1] GATE — POM_1 close decided from preconditions:")
        self._print_state(state)
        expected = {k: (v[0], sorted(v[1])) for k, v in EXPECTED_INITIAL.items()}
        if state != expected:
            raise CommandError(f"gate mismatch: expected {expected}, got {state}")
        self.stdout.write(
            self.style.SUCCESS(
                "    → Fatuberliu ACCEPTED, Maubara REJECTED with its exact "
                "3 failing reasons ✓"
            )
        )

    # -- [2] self-heal ------------------------------------------------------

    def _check_self_heal(self, store: Any) -> None:
        for event in HEAL_EVENTS:
            _append(store, event)
        _reset()
        sr.staged_replay(store, STREAM, STAGES)
        state = _close_state()

        self.stdout.write(
            "\n[2] SELF-HEAL — append the fixes for Maubara and re-replay:"
        )
        self._print_state(state)
        if state.get("Maubara") != ("ACCEPTED", []):
            raise CommandError(
                f"Maubara should self-heal to ACCEPTED, got {state.get('Maubara')}"
            )
        if state.get("Fatuberliu") != ("ACCEPTED", []):
            raise CommandError("Fatuberliu should stay ACCEPTED")
        self.stdout.write(
            self.style.SUCCESS(
                "    → same POM_1 close is now ACCEPTED — no backfill, no code "
                "change ✓"
            )
        )

    # -- [3] replay-safe aggregate ------------------------------------------

    def _check_replay_safe(self, store: Any) -> None:
        before = _balance("Maubara")
        # Re-run onto the EXISTING state (no reset) — that's the property that
        # matters: a recompute lands on the same value, an incrementing
        # aggregate would grow each replay. (A reset here would mask an
        # increment by clearing Balance first.)
        sr.staged_replay(store, STREAM, STAGES)
        sr.staged_replay(store, STREAM, STAGES)
        after = _balance("Maubara")

        self.stdout.write("\n[3] REPLAY-SAFE — the Balance aggregate is recomputed:")
        self.stdout.write(
            f"    Maubara operational balance: {before[0]} -> {after[0]} "
            f"across 2 extra replays."
        )
        # -50 (100 - 150) + 100 heal = 50; must not drift on re-replay.
        if before != after or after[0] != Decimal("50.00"):
            raise CommandError(
                f"aggregate not replay-safe: {before} -> {after} "
                f"(expected operational 50.00 stable)"
            )
        self.stdout.write(
            self.style.SUCCESS(
                "    → recompute-not-increment: balance stable across replays ✓"
            )
        )

    # -- [4] deterministic --------------------------------------------------

    def _check_deterministic(self, store: Any) -> None:
        before = _close_state()
        sr.staged_replay(store, STREAM, STAGES)  # onto existing state, no reset
        after = _close_state()
        # Assert a positive expected count too (one decision per distinct POM_1
        # suku), so this check can't pass vacuously on empty state if a
        # regression stopped stage 2 from writing any CycleClose rows.
        expected = len({e["suku"] for e in SAMPLE_EVENTS if e["form_type"] == "POM_1"})

        self.stdout.write("\n[4] DETERMINISTIC — re-replay the whole stream:")
        ok = before == after and len(after) == expected
        style = self.style.SUCCESS if ok else self.style.ERROR
        self.stdout.write(
            style(
                f"    {len(after)}/{expected} cycle-close decisions — "
                f"{'unchanged ✓' if ok else 'CHANGED ✗'}"
            )
        )
        if not ok:
            raise CommandError(
                f"re-replay changed a close decision or count "
                f"({len(before)} -> {len(after)}, expected {expected})"
            )

    # -- output -------------------------------------------------------------

    def _print_state(self, state: dict[str, tuple[str, list]]) -> None:
        for suku, (status, reasons) in sorted(state.items()):
            detail = ", ".join(reasons) if reasons else "—"
            self.stdout.write(f"    {suku:<12} {status:<9} {detail}")
