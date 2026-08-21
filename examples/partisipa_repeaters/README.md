# Tree-reconcile — a spike for unbounded nested repeaters

An evaluation prototype for [issue #7](https://github.com/joshbrooks/rakaia/issues/7)
feature #4, extending the `reconcile_children` helper shipped in #6 to arbitrarily
deep repeater trees.

> **This is a spike.** The tree flatten + reconcile live in the example, not rakaia
> core — it prototypes the shape before a core `reconcile_tree` helper lands. See
> [`docs/tree-reconcile.md`](../../docs/tree-reconcile.md) for the design.

## The problem it validates

FormKit-Ninja's `SeparatedSubmission` has a self-FK `repeater_parent`: a
submission's repeaters nest to **unbounded** depth. When a submission is
re-processed with a restructured tree — a pruned subtree, moved nodes — the old
nested rows are orphaned, and rollups **double-count** them. That's the recurring
bug behind `reconcile_separated_submissions` / issue #2252.

`reconcile_children` (#6) fixes *one* level: delete a parent's direct children not
in the kept set. But a tree needs the reconcile scoped to the **whole submission**,
excluding the full node set at every depth — otherwise an entire pruned subtree
(whose intermediate parent is gone) survives as deep orphans.

## Run

```sh
just partisipa-tree-demo
```

Or directly:

```sh
cd examples/partisipa_repeaters
uv run python manage.py migrate
uv run python manage.py demo_repeaters
```

Expected output:

```
Submission sub-1: v1 tree, then a pruned v2.
[1] BUILD — v1 tree materialized:
    6 nodes ['A', 'B', 'C', 'D', 'E', 'F'], Total=60
    → 6 nodes, valid tree, Total=60 ✓

[2] NAIVE-ORPHANS — resubmit v2, upsert-only:
    8 nodes ['A', 'B', 'C', 'D', 'E', 'F', 'G', 'H'], Total=65 (pruned survivors: ['B', 'D', 'E'])
    → orphaned ['B', 'D', 'E'] survive; Total double-counts (65 ≠ 35) — the bug.

[3] TREE-RECONCILE — resubmit v2, whole-subtree reconcile:
    5 nodes ['A', 'C', 'F', 'G', 'H'], Total=35, dangling parents=[]
    → pruned subtree gone at every depth (incl. grandchildren D/E), no dangling parents, Total=35 ✓

[4] IDEMPOTENT — re-replay the stream:
    5 nodes / Total=35 — unchanged ✓
```

Each check is asserted hard — a regression raises `CommandError` and exits non-zero.

## The tree

```
v1:            A                    v2:          A
             /   \                              /   \
            B     C                            C     G
           / \     \                            \     \
         D=10 E=20  F=30                        F=30  H=5
```

v2 prunes the **entire B subtree** (B, D, E) and adds G→H. The D/E leaves —
*grandchildren* of the removed B — are the ones a one-level reconcile orphans, and
they're what makes `Total` double-count (60/65 instead of 35).

## The reconcile is one shipped Effect, scoped to the submission

```python
Delete(
    model_label="repeaters.Node",
    lookup={"submission_id": sid},  # the whole subtree, any depth
    spare=Exclude({"node_id__in": current_node_ids}),
)
```

This is the `Delete` + `Exclude` effect from #6 — the only new idea is scoping the
reconcile to the **submission**, not a single parent level, so it catches orphans
at any depth in one pass.

## The checks have teeth

Each assertion was verified to fail on an injected regression:

- **dropping the reconcile** leaves `{B, D, E}` → `[3]` fails (node set wrong);
- a **shallow reconcile** (`depth ≤ 1` only, i.e. the one-level `reconcile_children`
  shape) deletes B but leaves the deep orphans `{D, E}` → `[3]` still fails. This is
  the proof that the reconcile must span all depths.

## Files

* **`seed.py`** — the v1 tree and the pruned/restructured v2, plus expected node sets and totals.
* **`tree_replay.py`** — `flatten` (nested tree → node rows), `replay_naive` (upsert-only, the bug) vs `replay_tree` (upsert + whole-submission reconcile), and the leaf-sum rollup.
* **`models.py`** — `Node` (self-referential tree) and the `Total` rollup.
* **`management/commands/demo_repeaters.py`** — builds v1, resubmits v2, runs all four asserted checks.

## Caveats

- One submission, depth 2, one restructuring. Real trees are deeper and wider; the
  reconcile is O(nodes) per submission regardless of depth.
- The rollup recomputes leaf sums from `Node` (replay-safe, per the aggregate spike).
- A production `reconcile_tree` helper would take the flattened node list and emit
  the upserts + the single submission-scoped reconcile delete — the shape this
  example hand-rolls.
