"""`manage.py demo_close` — the close-precondition state-machine spike.

It answers: *can a replay decide a cross-form state transition — Partisipa's
POM_1 cycle close, gated by `close_preconditions.py` — as a pure function of the
projected state, reject it with specific reasons when preconditions fail, and
self-heal to ACCEPTED when the missing facts arrive, with no bespoke re-check?*

Five checks, each asserted hard (a failure raises CommandError):

  [1] GATE       — replay evaluates each POM_1 close against the projections.
                   Fatuberliu (all preconditions met) is ACCEPTED and its open
                   finance lines are retired; Maubara is REJECTED with exactly
                   its three failing reasons and retires nothing. A vacuous
                   guard (always accept) fails here.
  [2] SELF-HEAL  — append the events that fix Maubara's three failures and
                   re-replay: the same POM_1 close flips to ACCEPTED with no
                   backfill task and no code change.
  [3] REPLAY-SAFE — the Balance aggregate is recomputed, not incremented, so
                   re-replaying does not double-count: Maubara's operational
                   balance stays put across repeated replays.
  [4] DETERMINISTIC — re-replay leaves every CycleClose (status + reasons)
                   byte-identical.
  [5] EVENT TIME — a retired row carries the *close event's* timestamp, not the
                   replay's clock: replay the same stream with the clock shifted
                   six years and the stamps are identical; the same replay with
                   a deliberately `now()`-stamped retire is not.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any
from unittest import mock

from django.core.management import call_command
from django.core.management.base import BaseCommand, CommandError

from django_rakaia import get_store
from lifecycle import staged_replay as sr
from lifecycle.handlers import STAGES, retire_with_now
from lifecycle.models import (
    Balance,
    CycleClose,
    FinanceLine,
    Meeting,
    Project,
)
from lifecycle.seed import (
    EXPECTED_INITIAL,
    HEAL_EVENTS,
    SAMPLE_EVENTS,
    expected_retired,
)
from rakaia import seed_stream

STREAM = "subproject-lifecycle"

#: The clock the determinism check freezes Django's `now()` at — six years after
#: the close events in the seed, so a clock-stamped retire cannot be mistaken
#: for an event-stamped one.
SHIFTED_CLOCK = datetime(2031, 6, 1, 9, 30, tzinfo=UTC)

#: The same stage plan with the one deliberately wrong handler, for [5].
WRONG_RETIRE_STAGES: dict[int, dict[str, Any]] = {
    **STAGES,
    2: {"events": [("POM_1", retire_with_now)]},
}


@contextmanager
def _frozen_clock(moment: datetime) -> Iterator[None]:
    """Freeze Django's clock — what a handler that reaches for `now()` reads.

    `django.utils.timezone.now` rather than the stdlib module, because that is
    the clock the mistake would be written against, and patching it changes
    nothing that reads `datetime.now` directly. The demo's claim is scoped the
    same way: a handler that stamps from Django's clock is what [5] catches.
    """
    with mock.patch("django.utils.timezone.now", return_value=moment):
        yield


def _reset() -> None:
    for model in (CycleClose, Balance, FinanceLine, Meeting, Project):
        model.objects.all().delete()


def _close_state() -> dict[str, tuple[str, list]]:
    return {c.suku: (c.status, sorted(c.reasons)) for c in CycleClose.objects.all()}


def _balance(suku: str) -> tuple[Decimal, Decimal] | None:
    b = Balance.objects.filter(suku=suku).first()
    return None if b is None else (b.operational, b.infrastructure)


def _retired() -> dict[str, datetime]:
    """Every soft-deleted finance line, keyed by its submission, with its stamp."""
    return {
        line.submission_id: line.retired_at
        for line in FinanceLine.objects.filter(retired_at__isnull=False)
    }


class Command(BaseCommand):
    help = "Decide a POM_1 cycle close from cross-form preconditions via replay."

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        # Self-contained: ensure this demo's tables exist so `manage.py
        # demo_*` works when run directly, not only via the migrate-first
        # `just` recipe. Idempotent — a no-op once migrations are applied.
        call_command("migrate", verbosity=0, interactive=False)
        store = get_store()
        store.delete(STREAM)
        seed_stream(STREAM, SAMPLE_EVENTS, store=store)
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
        self._check_event_time(store)

        self.stdout.write(self.style.SUCCESS("\nAll close-gate checks passed ✓"))

    # -- [1] gate -----------------------------------------------------------

    def _check_gate(self) -> None:
        state = _close_state()
        self.stdout.write("[1] GATE — POM_1 close decided from preconditions:")
        self._print_state(state)
        expected = {k: (v[0], sorted(v[1])) for k, v in EXPECTED_INITIAL.items()}
        if state != expected:
            raise CommandError(f"gate mismatch: expected {expected}, got {state}")

        # Only the accepted close retires its finance lines, and it stamps them
        # with its own event's time.
        close_ts = {
            event["suku"]: datetime.fromisoformat(event["ts"])
            for event in SAMPLE_EVENTS
            if event["form_type"] == "POM_1"
        }
        retired_fb = {
            event["key"]: close_ts[event["suku"]]
            for event in SAMPLE_EVENTS
            if event["form_type"] == "FINANCE" and event["suku"] == "Fatuberliu"
        }
        if _retired() != retired_fb:
            raise CommandError(
                f"Fatuberliu's accepted close should retire its finance lines at "
                f"the event's time: expected {retired_fb}, got {_retired()}"
            )
        if FinanceLine.objects.filter(
            suku="Maubara", retired_at__isnull=False
        ).exists():
            raise CommandError("a REJECTED close retired finance lines")
        self.stdout.write(
            self.style.SUCCESS(
                "    → Fatuberliu ACCEPTED, Maubara REJECTED with its exact "
                "3 failing reasons ✓"
            )
        )
        self.stdout.write(
            f"    → the accepted close retired {len(retired_fb)} finance lines at "
            f"the event's time; the rejected one retired none ✓"
        )

    # -- [2] self-heal ------------------------------------------------------

    def _check_self_heal(self, store: Any) -> None:
        seed_stream(STREAM, HEAL_EVENTS, store=store)
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
                "    → same POM_1 close is now ACCEPTED — no backfill, no code change ✓"
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

    # -- [5] a retired row keeps the event's clock ---------------------------

    def _check_event_time(self, store: Any) -> None:
        expected = {
            key: datetime.fromisoformat(ts) for key, ts in expected_retired().items()
        }

        # The same stream, replayed with Django's clock six years on.
        with _frozen_clock(SHIFTED_CLOCK):
            _reset()
            sr.staged_replay(store, STREAM, STAGES)
            shifted = _retired()

        # And once with the clock left alone.
        _reset()
        sr.staged_replay(store, STREAM, STAGES)
        real = _retired()

        # And once with the deliberately wrong handler, same shifted clock.
        with _frozen_clock(SHIFTED_CLOCK):
            _reset()
            sr.staged_replay(store, STREAM, WRONG_RETIRE_STAGES)
            wrong = _retired()

        self.stdout.write(
            "\n[5] EVENT TIME — a retired row carries the close event's clock:"
        )
        self.stdout.write(f"    {'line':<10} {'retired_at':<26} source")
        for line in sorted(expected):
            self.stdout.write(
                f"    {line:<10} {expected[line].isoformat():<26} the close event"
            )
        self.stdout.write(
            f"    {'':<10} {SHIFTED_CLOCK.isoformat():<26} the replay clock, if "
            f"the retire read now()"
        )

        if shifted != real:
            raise CommandError(
                f"the same replay wrote different stamps with the clock shifted: "
                f"{shifted} != {real}"
            )
        if real != expected:
            raise CommandError(
                f"retired rows do not carry the close event's time: "
                f"expected {expected}, got {real}"
            )
        stamp = next(iter(wrong.values()), None)
        if wrong == real or stamp != SHIFTED_CLOCK:
            raise CommandError(
                f"the now()-stamped retire was not caught: got {stamp}, "
                f"expected {SHIFTED_CLOCK}"
            )
        self.stdout.write(
            self.style.SUCCESS(
                f"    → {len(real)} rows retired at {min(real.values()).isoformat()} "
                f"- {max(real.values()).isoformat()} whether the clock reads 2026 "
                f"or 2031 ✓"
            )
        )
        self.stdout.write(
            self.style.WARNING(
                f"    → the now() variant wrote {stamp.isoformat()}: a replay a "
                f"year later would rewrite history ✗ (caught, not shipped)"
            )
        )

        # Leave the projections on the correct replay, so the demo ends in the
        # state it claims rather than in the mutant's.
        _reset()
        sr.staged_replay(store, STREAM, STAGES)
        if _retired() != expected:
            raise CommandError("a clean replay after the mutant did not converge")

    # -- output -------------------------------------------------------------

    def _print_state(self, state: dict[str, tuple[str, list]]) -> None:
        for suku, (status, reasons) in sorted(state.items()):
            detail = ", ".join(reasons) if reasons else "—"
            self.stdout.write(f"    {suku:<12} {status:<9} {detail}")
