"""`manage.py demo_merge` — the multi-stream merge spike (issue #7 #2).

It answers: *can a subproject view built from several separate form streams
(Partisipa's SF / TF / FF pipelines) be replayed as one deterministic order —
reproducing what a single combined stream would produce, resolving equal
timestamps stably, and self-healing when a fix lands on one pipeline?*

Four checks, each asserted hard (a failure raises CommandError):

  [1] PARITY      — the projection built by merging three streams equals the one
                    built from a single combined stream, field-for-field
                    (including the order-sensitive Claim row); and the merged
                    order reconstructs the authored canonical order.
  [2] DETERMINISM — merging is a pure function of the streams: passing the paths
                    in a different order yields the identical merged sequence.
  [3] TIE-BREAK   — two events sharing a timestamp across streams resolve by the
                    stable `(ts, stream_path, offset)` tiebreak, every time.
  [4] SELF-HEAL   — a fix appended to one pipeline, re-merged, updates the view;
                    single-stream and merged stay in agreement.
"""

from __future__ import annotations

import json
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from django_rakaia.store import get_store
from subproject import merge_replay as mr
from subproject.handlers import STAGES
from subproject.models import (
    Balance,
    Claim,
    FinanceLine,
    Meeting,
    Project,
    Readiness,
)
from subproject.seed import (
    EXPECTED_INITIAL_READINESS,
    HEAL_EVENTS,
    INITIAL_EVENTS,
    SINGLE_STREAM,
    STREAMS,
    THREE_STREAMS,
    TIE_KEYS,
)


def _reset() -> None:
    for model in (Readiness, Balance, Claim, FinanceLine, Meeting, Project):
        model.objects.all().delete()


def _append(store: Any, path: str, event: dict) -> None:
    store.append(path, json.dumps(event).encode("utf-8"))


def _snapshot() -> dict[str, Any]:
    return {
        "readiness": {
            r.suku: (r.ready, sorted(r.reasons)) for r in Readiness.objects.all()
        },
        "balance": {
            b.suku: (b.operational, b.infrastructure) for b in Balance.objects.all()
        },
        "project": {
            (p.suku, p.output): p.percent for p in Project.objects.all()
        },
        "meeting": {
            (m.suku, m.meeting_id): m.verified for m in Meeting.objects.all()
        },
        # Order-sensitive across streams: the tied FINANCE/MEETING pair both
        # write this, so its value depends on the merged order between them.
        "claim": {c.slot: c.claimed_by for c in Claim.objects.all()},
    }


def _replay_baseline(store: Any) -> dict[str, Any]:
    _reset()
    mr.staged_replay_events(mr.read_events(store, SINGLE_STREAM), STAGES)
    return _snapshot()


def _replay_merged(store: Any, paths: list[str] | None = None) -> dict[str, Any]:
    _reset()
    mr.staged_replay_events(mr.merge_streams(store, paths or THREE_STREAMS), STAGES)
    return _snapshot()


class Command(BaseCommand):
    help = "Merge several form streams into one deterministic subproject replay."

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        store = get_store()
        self._build_single(store, INITIAL_EVENTS)
        self._build_three(store, INITIAL_EVENTS)
        self.stdout.write(
            f"Seeded {len(INITIAL_EVENTS)} events into 1 combined stream and "
            f"across {len(THREE_STREAMS)} form pipelines "
            f"({', '.join(STREAMS.values())}).\n"
        )

        self._check_parity(store)
        self._check_determinism(store)
        self._check_tiebreak(store)
        self._check_self_heal(store)

        self.stdout.write(self.style.SUCCESS("\nAll merge checks passed ✓"))

    # -- stream construction ------------------------------------------------

    def _build_single(self, store: Any, events: list[dict]) -> None:
        store.delete(SINGLE_STREAM)
        store.create(SINGLE_STREAM)
        for event in events:
            _append(store, SINGLE_STREAM, event)

    def _build_three(self, store: Any, events: list[dict]) -> None:
        for path in THREE_STREAMS:
            store.delete(path)
            store.create(path)
        for event in events:
            _append(store, STREAMS[event["form_type"]], event)

    # -- [1] parity ---------------------------------------------------------

    def _check_parity(self, store: Any) -> None:
        baseline = _replay_baseline(store)
        merged = _replay_merged(store)
        order = [e["key"] for e in mr.merge_streams(store, THREE_STREAMS)]
        authored = [e["key"] for e in INITIAL_EVENTS]
        expected = {
            k: (v[0], sorted(v[1])) for k, v in EXPECTED_INITIAL_READINESS.items()
        }

        self.stdout.write("[1] PARITY — 3 merged streams vs 1 combined stream:")
        self._print_readiness(merged["readiness"])
        if order != authored:
            raise CommandError(
                f"merged order != authored canonical order\n"
                f"  merged:   {order}\n  authored: {authored}"
            )
        if baseline != merged:
            raise CommandError("merged projection differs from single-stream baseline")
        if merged["readiness"] != expected:
            raise CommandError(
                f"readiness mismatch: expected {expected}, got {merged['readiness']}"
            )
        self.stdout.write(
            self.style.SUCCESS(
                "    → merged projection == single-stream baseline, and the "
                "merged order reconstructs the canonical sequence ✓"
            )
        )

    # -- [2] determinism ----------------------------------------------------

    def _check_determinism(self, store: Any) -> None:
        order1 = [e["key"] for e in mr.merge_streams(store, THREE_STREAMS)]
        order2 = [
            e["key"] for e in mr.merge_streams(store, list(reversed(THREE_STREAMS)))
        ]
        self.stdout.write("\n[2] DETERMINISM — stream argument order must not matter:")
        self.stdout.write(
            f"    paths given forwards vs reversed -> "
            f"{'identical' if order1 == order2 else 'DIFFERENT'} sequence."
        )
        if order1 != order2:
            raise CommandError("merge order depends on the order paths are passed")
        self.stdout.write(
            self.style.SUCCESS("    → merge is a pure function of the streams ✓")
        )

    # -- [3] tie-break ------------------------------------------------------

    def _check_tiebreak(self, store: Any) -> None:
        keys = [e["key"] for e in mr.merge_streams(store, THREE_STREAMS)]
        first, second = TIE_KEYS  # ("f-mb-1", "m-mb-1") — same ts, finance first
        i_first, i_second = keys.index(first), keys.index(second)
        # Materialize the projection so the tie's effect on state is observable.
        _replay_merged(store)
        claimed_by = Claim.objects.get(slot="mb-claim").claimed_by

        self.stdout.write("\n[3] TIE-BREAK — two events share a timestamp:")
        self.stdout.write(
            f"    {first} (forms/finance) and {second} (forms/meetings) both at "
            f"12:30 -> merged positions {i_first}, {i_second}; "
            f"Claim.claimed_by={claimed_by}."
        )
        # finance sorts before meetings (path tiebreak), and they stay adjacent;
        # the LATER one ({second}) therefore wins the shared Claim row.
        if not (i_first + 1 == i_second):
            raise CommandError(
                f"tied events not resolved by the stable tiebreak: "
                f"{first}@{i_first}, {second}@{i_second}"
            )
        if claimed_by != second:
            raise CommandError(
                f"tie resolved wrong in the projection: Claim won by "
                f"{claimed_by}, expected {second} (the later event in merged order)"
            )
        self.stdout.write(
            self.style.SUCCESS(
                "    → equal timestamps break by (stream_path, offset), stably, "
                "and the projection reflects it ✓"
            )
        )

    # -- [4] self-heal ------------------------------------------------------

    def _check_self_heal(self, store: Any) -> None:
        # Each fix lands on its own pipeline (and on the combined stream).
        for event in HEAL_EVENTS:
            _append(store, STREAMS[event["form_type"]], event)
            _append(store, SINGLE_STREAM, event)

        baseline = _replay_baseline(store)
        merged = _replay_merged(store)

        self.stdout.write("\n[4] SELF-HEAL — fixes arrive on separate pipelines:")
        self._print_readiness(merged["readiness"])
        if merged["readiness"].get("Maubara") != (True, []):
            raise CommandError(
                f"Maubara should be READY after heal, got "
                f"{merged['readiness'].get('Maubara')}"
            )
        if baseline != merged:
            raise CommandError("single-stream and merged diverged after heal")
        self.stdout.write(
            self.style.SUCCESS(
                "    → Maubara now READY; merged still matches the combined "
                "stream ✓"
            )
        )

    # -- output -------------------------------------------------------------

    def _print_readiness(self, readiness: dict[str, tuple]) -> None:
        for suku, (ready, reasons) in sorted(readiness.items()):
            state = "READY" if ready else "NOT-READY"
            detail = ", ".join(reasons) if reasons else "—"
            self.stdout.write(f"    {suku:<12} {state:<10} {detail}")
