"""Reconciling an unbounded nested-repeater tree on replay (issue #7 #4).

Each submission event carries the **full** repeater tree (a snapshot). Replaying
the stream must leave the `Node` projection holding exactly the current tree —
no rows left over from a previous version, at any depth.

Two strategies are contrasted:

* ``replay_naive`` — upsert every node, no delete. A resubmission that prunes a
  subtree leaves the old nodes orphaned, so the `Total` rollup double-counts. This
  is the `reconcile_separated_submissions` bug class.
* ``replay_tree`` — upsert every node **plus one reconcile delete keyed by the
  submission**, excluding the current node-id set. Because it is scoped to the
  whole submission (not one parent level), it removes orphans at *any* depth —
  including entire pruned subtrees whose intermediate parent is gone, which a
  one-level ``reconcile_children`` would miss.

The reconcile uses the shipped ``delete`` + ``exclude`` Effect op (#6); the only
new idea is scoping it to the submission subtree rather than a single parent.
"""

from __future__ import annotations

import json
from typing import Any

from django_rakaia import DjangoExecutor
from rakaia import Effect

NODE = "repeaters.Node"
TOTAL = "repeaters.Total"


def flatten(
    tree: dict[str, Any],
    parent_id: str = "",
    depth: int = 0,
) -> list[dict[str, Any]]:
    """Depth-first flatten of a nested ``{id, value, children}`` tree."""
    children = tree.get("children", [])
    node = {
        "node_id": tree["id"],
        "parent_node_id": parent_id,
        "depth": depth,
        "value": tree.get("value", 0),
        "is_leaf": not children,
    }
    rows = [node]
    for child in children:
        rows.extend(flatten(child, tree["id"], depth + 1))
    return rows


def _node_upserts(submission_id: str, nodes: list[dict[str, Any]]) -> list[Effect]:
    return [
        Effect(
            op="update_or_create",
            model_label=NODE,
            lookup={"submission_id": submission_id, "node_id": n["node_id"]},
            defaults={
                "parent_node_id": n["parent_node_id"],
                "depth": n["depth"],
                "value": n["value"],
                "is_leaf": n["is_leaf"],
            },
        )
        for n in nodes
    ]


def _tree_reconcile(submission_id: str, kept_ids: list[str]) -> Effect:
    """Delete every node of this submission NOT in the current tree — any depth."""
    return Effect(
        op="delete",
        model_label=NODE,
        lookup={"submission_id": submission_id},
        exclude={"node_id__in": kept_ids},
    )


def _recompute_totals() -> None:
    """Rollup: Total = sum of leaf values per submission (replay-safe)."""
    from .models import Node

    # A plain set() dedupes reliably; `.distinct()` would be defeated by the
    # model's Meta ordering (node_id) leaking into the query's SELECT.
    submissions = sorted(set(Node.objects.values_list("submission_id", flat=True)))
    effects = []
    for sid in submissions:
        total = sum(
            n.value for n in Node.objects.filter(submission_id=sid, is_leaf=True)
        )
        effects.append(
            Effect(
                op="update_or_create",
                model_label=TOTAL,
                lookup={"submission_id": sid},
                defaults={"total": total},
            )
        )
    if effects:
        DjangoExecutor().apply(effects)


def _events(store: Any, stream_path: str) -> list[dict[str, Any]]:
    messages, _ = store.read(stream_path)
    return [json.loads(m.data) for m in messages]


def replay_naive(store: Any, stream_path: str) -> None:
    """Upsert nodes, never delete — orphans survive a resubmission (the bug)."""
    executor = DjangoExecutor()
    for event in _events(store, stream_path):
        nodes = flatten(event["tree"])
        executor.apply(_node_upserts(event["key"], nodes))
    _recompute_totals()


def replay_tree(store: Any, stream_path: str) -> None:
    """Upsert nodes + reconcile the whole submission subtree — no orphans."""
    executor = DjangoExecutor()
    for event in _events(store, stream_path):
        nodes = flatten(event["tree"])
        kept = [n["node_id"] for n in nodes]
        executor.apply(
            [*_node_upserts(event["key"], nodes), _tree_reconcile(event["key"], kept)]
        )
    _recompute_totals()
