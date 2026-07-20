"""`manage.py demo_repeaters` — the tree-reconcile spike for issue #7 #4.

It answers: *when a submission's unbounded nested-repeater tree is re-submitted
with a pruned subtree, can replay leave the projection holding exactly the new
tree — no orphaned nodes at any depth, no double-counted rollup — the recurring
`reconcile_separated_submissions` bug?*

Four checks, each asserted hard (a failure raises CommandError):

  [1] BUILD        — the v1 tree materializes: 6 nodes, valid parent links,
                     Total = 60.
  [2] NAIVE-ORPHANS — resubmit v2 with upsert-only: the pruned B/D/E subtree
                     survives and Total double-counts (65, not 35). The bug.
  [3] TREE-RECONCILE — resubmit v2 with a whole-submission reconcile: the pruned
                     subtree is gone at every depth, no dangling parents, and
                     Total is correct (35).
  [4] IDEMPOTENT   — re-replay leaves the node set and Total unchanged.
"""

from __future__ import annotations

import json
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from django_rakaia.store import get_store
from repeaters import tree_replay as tr
from repeaters.models import Node, Total
from repeaters.seed import (
    NAIVE_TOTAL,
    PRUNED,
    SUBMISSION,
    V1_EVENT,
    V1_NODES,
    V1_TOTAL,
    V2_EVENT,
    V2_NODES,
    V2_TOTAL,
)

STREAM = "repeater-submissions"


def _reset() -> None:
    Total.objects.all().delete()
    Node.objects.all().delete()


def _node_ids() -> set[str]:
    return set(Node.objects.filter(submission_id=SUBMISSION).values_list(
        "node_id", flat=True
    ))


def _total() -> int:
    row = Total.objects.filter(submission_id=SUBMISSION).first()
    return row.total if row else 0


def _dangling_parents() -> list[str]:
    """Node ids whose parent_node_id is neither root ("") nor a present node."""
    present = _node_ids()
    return [
        n.node_id
        for n in Node.objects.filter(submission_id=SUBMISSION)
        if n.parent_node_id and n.parent_node_id not in present
    ]


class Command(BaseCommand):
    help = "Reconcile an unbounded nested-repeater tree on replay (no orphans)."

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        store = get_store()
        store.delete(STREAM)
        store.create(STREAM)
        self.stdout.write(f"Submission {SUBMISSION}: v1 tree, then a pruned v2.\n")

        self._check_build(store)
        self._check_naive(store)
        self._check_tree(store)
        self._check_idempotent(store)

        self.stdout.write(self.style.SUCCESS("\nAll tree-reconcile checks passed ✓"))

    def _append(self, store: Any, event: dict) -> None:
        store.append(STREAM, json.dumps(event).encode("utf-8"))

    # -- [1] build ----------------------------------------------------------

    def _check_build(self, store: Any) -> None:
        self._append(store, V1_EVENT)
        _reset()
        tr.replay_tree(store, STREAM)
        ids, total, dangling = _node_ids(), _total(), _dangling_parents()

        self.stdout.write("[1] BUILD — v1 tree materialized:")
        self.stdout.write(f"    {len(ids)} nodes {sorted(ids)}, Total={total}")
        if ids != V1_NODES or total != V1_TOTAL or dangling:
            raise CommandError(
                f"v1 build wrong: nodes={sorted(ids)} total={total} "
                f"dangling={dangling}"
            )
        self.stdout.write(
            self.style.SUCCESS(f"    → 6 nodes, valid tree, Total={V1_TOTAL} ✓")
        )

    # -- [2] naive orphans --------------------------------------------------

    def _check_naive(self, store: Any) -> None:
        self._append(store, V2_EVENT)  # the resubmission
        _reset()
        tr.replay_naive(store, STREAM)
        ids, total = _node_ids(), _total()
        survived = PRUNED & ids

        self.stdout.write("\n[2] NAIVE-ORPHANS — resubmit v2, upsert-only:")
        self.stdout.write(
            f"    {len(ids)} nodes {sorted(ids)}, Total={total} "
            f"(pruned survivors: {sorted(survived)})"
        )
        # The bug: the pruned subtree survives and the rollup double-counts.
        if survived != PRUNED or total != NAIVE_TOTAL:
            raise CommandError(
                f"expected naive replay to orphan {sorted(PRUNED)} and total "
                f"{NAIVE_TOTAL}, got survivors {sorted(survived)} total {total}"
            )
        self.stdout.write(
            self.style.WARNING(
                f"    → orphaned {sorted(PRUNED)} survive; Total double-counts "
                f"({total} ≠ {V2_TOTAL}) — the bug."
            )
        )

    # -- [3] tree reconcile -------------------------------------------------

    def _check_tree(self, store: Any) -> None:
        _reset()
        tr.replay_tree(store, STREAM)  # same stream ([v1, v2]) — reconciled
        ids, total, dangling = _node_ids(), _total(), _dangling_parents()
        survived = PRUNED & ids

        self.stdout.write("\n[3] TREE-RECONCILE — resubmit v2, whole-subtree reconcile:")
        self.stdout.write(
            f"    {len(ids)} nodes {sorted(ids)}, Total={total}, "
            f"dangling parents={dangling}"
        )
        if ids != V2_NODES:
            raise CommandError(f"expected exactly {sorted(V2_NODES)}, got {sorted(ids)}")
        if survived:
            raise CommandError(f"pruned subtree survived at some depth: {sorted(survived)}")
        if dangling:
            raise CommandError(f"dangling parent pointers: {dangling}")
        if total != V2_TOTAL:
            raise CommandError(f"rollup wrong: {total} != {V2_TOTAL}")
        self.stdout.write(
            self.style.SUCCESS(
                f"    → pruned subtree gone at every depth (incl. grandchildren "
                f"D/E), no dangling parents, Total={V2_TOTAL} ✓"
            )
        )

    # -- [4] idempotent -----------------------------------------------------

    def _check_idempotent(self, store: Any) -> None:
        before = (sorted(_node_ids()), _total())
        tr.replay_tree(store, STREAM)  # onto existing state, no reset
        after = (sorted(_node_ids()), _total())

        self.stdout.write("\n[4] IDEMPOTENT — re-replay the stream:")
        ok = before == after
        style = self.style.SUCCESS if ok else self.style.ERROR
        self.stdout.write(
            style(
                f"    {len(after[0])} nodes / Total={after[1]} — "
                f"{'unchanged ✓' if ok else 'CHANGED ✗'}"
            )
        )
        if not ok:
            raise CommandError("re-replay changed the reconciled tree")
