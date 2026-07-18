"""`manage.py demo_staged` — the staged-replay spike for issue #7 (feature #1).

It answers: *can a two-stage replay resolve the cross-form link that Partisipa
resolves today with reactive signals + one-shot backfill tasks — including when
the dependent form arrives before its reference entity, and self-healing when
the reference arrives late?*

Three checks, each asserted hard (a failure raises CommandError):

  [1] NAIVE     — one pass in stream order (signals today). SF forms that precede
                  their TF are left UNLINKED. Reproduces the backfill bug.
  [2] STAGED    — stage 0 builds all Projects, stage 1 links all SF. Every form
                  links regardless of arrival order.
  [3] SELF-HEAL — append an SF whose TF is missing -> unlinked; then append the
                  late TF and re-run staged -> it links, with NO backfill task.
                  Re-running once more is idempotent.
"""

from __future__ import annotations

import json
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from django_rakaia.store import get_store
from partisipa import staged_replay as sr
from partisipa.handlers import ALL_ONE_STAGE, STAGED
from partisipa.models import Project, Sf12
from partisipa.seed import LATE_SF, LATE_TF, SAMPLE_SUBMISSIONS

STREAM = "submissions"


def _reset_projections() -> None:
    Sf12.objects.all().delete()
    Project.objects.all().delete()


def _link_state() -> tuple[list, list]:
    """The link state that idempotency must preserve — the actual project_id and
    link_reason of every form, plus the project registry, not just row counts."""
    links = sorted(
        (s.submission_id, s.project_id, s.link_reason) for s in Sf12.objects.all()
    )
    projects = sorted((p.suku, p.output, p.name) for p in Project.objects.all())
    return links, projects


def _append(store: Any, event: dict) -> None:
    store.append(STREAM, json.dumps(event).encode("utf-8"))


class Command(BaseCommand):
    help = "Demonstrate staged replay resolving late-arriving cross-form links."

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        store = get_store()
        store.delete(STREAM)
        store.create(STREAM)
        for event in SAMPLE_SUBMISSIONS:
            _append(store, event)
        self.stdout.write(
            f"Seeded {len(SAMPLE_SUBMISSIONS)} submissions "
            f"(every SF_1_2 precedes its TF_6_1_1 in the stream).\n"
        )

        self._check_naive(store)
        self._check_staged(store)
        self._check_self_heal(store)

        self.stdout.write(self.style.SUCCESS("\nAll staged-replay checks passed ✓"))

    # -- checks -------------------------------------------------------------

    def _check_naive(self, store: Any) -> None:
        _reset_projections()
        sr.naive_replay(store, STREAM, ALL_ONE_STAGE)
        unlinked = Sf12.objects.filter(project__isnull=True).count()
        linked = Sf12.objects.filter(project__isnull=False).count()
        self.stdout.write(
            self.style.WARNING(
                f"[1] NAIVE (signals today): {linked} linked, {unlinked} UNLINKED "
                f"— SF forms processed before their TF link to nothing."
            )
        )
        self._print_links()
        if unlinked == 0:
            raise CommandError(
                "expected the naive pass to leave unlinked forms (the bug)"
            )

    def _check_staged(self, store: Any) -> None:
        _reset_projections()
        sr.staged_replay(store, STREAM, STAGED)
        unlinked = Sf12.objects.filter(project__isnull=True).count()
        linked = Sf12.objects.filter(project__isnull=False).count()
        # Assert a positive expected count, not just "unlinked == 0" — the latter
        # passes vacuously if stage 1 produces no rows at all (a broken dispatch
        # or empty STAGE_1). Expected = every SF_1_2 in the seed.
        expected = len([e for e in SAMPLE_SUBMISSIONS if e["form_type"] == "SF_1_2"])
        ok = unlinked == 0 and linked == expected
        style = self.style.SUCCESS if ok else self.style.ERROR
        self.stdout.write(
            style(
                f"\n[2] STAGED: {linked} linked, {unlinked} unlinked "
                f"(expected {expected} linked) — stage 0 builds every Project "
                f"before stage 1 links, so arrival order no longer matters."
            )
        )
        self._print_links()
        if not ok:
            raise CommandError(
                f"staged replay: expected {expected} linked / 0 unlinked, "
                f"got {linked} linked / {unlinked} unlinked"
            )

    def _check_self_heal(self, store: Any) -> None:
        # A new SF whose TF has NOT been submitted yet.
        _append(store, LATE_SF)
        _reset_projections()
        sr.staged_replay(store, STREAM, STAGED)
        late = Sf12.objects.get(submission_id=LATE_SF["key"])
        if late.project_id is not None:
            raise CommandError("late SF should be unlinked before its TF arrives")
        self.stdout.write(
            self.style.WARNING(
                f"\n[3] SELF-HEAL: appended {LATE_SF['key']} with no project yet "
                f"-> link_reason={late.link_reason} (unlinked)."
            )
        )

        # The TF finally arrives. Re-run staged — no bespoke backfill task.
        _append(store, LATE_TF)
        _reset_projections()
        sr.staged_replay(store, STREAM, STAGED)
        healed = Sf12.objects.get(submission_id=LATE_SF["key"])
        if healed.project_id is None:
            raise CommandError("late SF did not self-heal after its TF arrived")
        self.stdout.write(
            self.style.SUCCESS(
                f"    late TF arrived -> re-replay links {LATE_SF['key']} "
                f"to '{healed.project.name}' (link_reason={healed.link_reason}) "
                f"— no backfill task."
            )
        )

        # Idempotency: replaying again must not change the link *state*, not
        # merely the row counts. update_or_create on stable lookups can never
        # change a count, so a count check has no teeth — a re-derivation that
        # flipped project_id back to NULL (or NM->NPO) would still pass it.
        before = _link_state()
        sr.staged_replay(store, STREAM, STAGED)
        after = _link_state()
        ok = before == after
        style = self.style.SUCCESS if ok else self.style.ERROR
        self.stdout.write(
            style(
                f"    replayed again: {len(after[0])} links / {len(after[1])} "
                f"projects — {'link state unchanged ✓' if ok else 'CHANGED ✗'}"
            )
        )
        if not ok:
            raise CommandError("staged replay is not idempotent (link state changed)")

    # -- output -------------------------------------------------------------

    def _print_links(self) -> None:
        for sf in Sf12.objects.all():
            target = sf.project.name if sf.project_id else "— UNLINKED —"
            self.stdout.write(f"    {sf.submission_id:<24} {sf.link_reason:<4} {target}")
