# multi_owner — one row, many writers (no Django)

The projection helpers most examples use (`reconcile_children`, `project_latest`)
own a **whole** row. This example is for the harder case: a single projection row
composed by **several independent owners**, or a child FK that must point at a
sibling row whose primary key doesn't exist until apply time. Those primitives —
the newest additions to rakaia — weren't exercised by any other example.

It runs the scenarios end to end through a tiny in-memory executor
([`executor.py`](./executor.py), a ~90-line stand-in for `DjangoExecutor`), so
`Ref` resolution and the reconcile passes are *applied to real rows*, not just
printed. No Django, no database.

Companion docs: [`docs/dry-run-and-executors.md`](../../docs/dry-run-and-executors.md),
[`docs/projections-and-fan-out.md`](../../docs/projections-and-fan-out.md),
[`docs/alerts-projection.md`](../../docs/alerts-projection.md).

## Run

```sh
just multi-owner-demo
# or: cd examples/multi_owner && uv run python demo.py
```

## What it proves

| Section | Primitive | Point |
|---|---|---|
| 1 | `Ref` / `RefResolver` | An effect binds an FK to a **sibling** effect's generated pk — no staging split, no reader lookup. A dangling `Ref` raises `UnresolvedRefError`, never a silent `NULL`. |
| 2 | `reconcile_aggregate(owns=…)` | Two reducers share one row, each owning disjoint columns; when a group vanishes in one owner, only *that owner's* columns are null-cleared — the row and the other owner survive. |
| 3 | `reconcile_by_key(retire=…)` | Reconcile rows on a composite natural key, **soft-deleting** stale rows (stamp `resolved_at`) instead of dropping them, scoped off authored rows via `retire_filter`. |
| 4 | `check_disjoint_defaults` | The invariant that makes multi-owner rows safe: two owners writing the same column is caught as an `EffectCollisionError`. |
| 5 | `ExternalEffect` | Route the effects rakaia deliberately never applies (email, webhooks) to per-`kind` handlers — a two-line loop in the app. |

`executor.py` is intentionally minimal — it supports only the lookup operators
these demos need (`__in`, `__isnull`). For the production executor see
`django_rakaia/effect_executor.py`.
