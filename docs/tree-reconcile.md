# Tree-reconcile (design spike)

> Status: **design spike** for [issue #7](https://github.com/joshbrooks/rakaia/issues/7)
> feature #4. Prototyped in [`examples/partisipa_repeaters`](../examples/partisipa_repeaters);
> not yet part of rakaia core. This page is the design; the example is the proof.

## The problem: unbounded nesting orphans deep rows

FormKit-Ninja's `SeparatedSubmission` nests repeaters to arbitrary depth via a
self-FK `repeater_parent`. Each submission event carries the **full** tree, so
replaying a resubmission must leave the projection holding exactly the new tree.
Upsert-only replay leaves the previous version's nodes behind; when a subtree is
pruned, those become **deep orphans**, and any rollup over the nodes
double-counts. This is the `reconcile_separated_submissions` bug class (issue
#2252).

## Why one-level reconcile is not enough

The shipped [`reconcile_children`](projections-and-fan-out.md) helper reconciles a
**single parent's** direct children — `delete children WHERE parent = P AND
child_key NOT IN kept`. That is correct for a flat repeater, but for a tree it
misses orphans whose parent was itself removed:

```
v1:   A─┬─B─┬─D        v2:   A─┬─C──F
        │   └─E              └─G──H
        └─C──F
```

Reconciling *A's children* deletes B (not in v2), but D and E — B's children — are
never visited, because there is no surviving "parent B" to reconcile against. They
survive as deep orphans. The example demonstrates exactly this: a `depth ≤ 1`
reconcile deletes B yet leaves `{D, E}`.

## The proposal: reconcile the whole submission subtree

Scope the reconcile to the **submission**, not a parent, and exclude the full set
of node ids in the current tree — at every depth, in one delete:

```python
def reconcile_tree(submission_id, nodes):
    kept = [n["node_id"] for n in nodes]
    return [
        *[upsert(submission_id, n) for n in nodes],
        Effect(op="delete", model_label=NODE,
               lookup={"submission_id": submission_id},   # whole subtree
               exclude={"node_id__in": kept}),            # any depth
    ]
```

Because the delete is keyed by `submission_id` and excludes every kept node id, a
pruned subtree is removed no matter how deep its intermediate parent was. This
reuses the shipped `delete` + `exclude` Effect op (#6); the only new idea is the
**scope**.

## Correctness

- **No orphans at any depth** — the reconcile's scope is the whole submission, so
  every stale node (leaf or internal) not in the current tree is deleted.
- **No dangling parents** — since a removed internal node's descendants are also
  not in `kept`, they're deleted in the same pass; no node is left pointing at a
  vanished parent.
- **Replay-safe** — upserts are idempotent and the reconcile is a set difference,
  so re-replaying the same tree is a no-op.
- **Rollup correctness** — a leaf-sum (or any aggregate) over the reconciled nodes
  is correct because there are no stale contributors; this is where the
  double-count bug is actually killed.

## What making it first-class requires

The spike hand-rolls `flatten` + the reconcile. A core `reconcile_tree(model,
parent_lookup, node_key, nodes)` helper would:

1. take the flattened node list (the caller flattens their own tree shape),
2. emit one upsert per node keyed by `(parent_lookup, node_key)`, and
3. emit the single submission-scoped reconcile delete.

It is the tree generalization of `reconcile_children`: same primitives, scope
widened from one parent level to the whole subtree.

## Relationship to the other spikes

- **`reconcile_children`** (#6) is the one-level case this generalizes.
- **`reconcile_aggregate`** (#7 #3) is the rollup that this keeps honest — no stale
  nodes means no double-count.
- **Staged replay / merge** feed the submission events; tree-reconcile is what each
  submission handler does to its own nested rows.
