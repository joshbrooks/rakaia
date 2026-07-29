---
type: Example
title: "multi_owner — one row, many writers (no Django)"
description: "A zero-dependency script that drives rakaia's newest effect/projection primitives end-to-end through a ~90-line in-memory executor standing in for `DjangoExecutor`."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/multi_owner
tags: [example, standalone, effects, projections]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
verified:
  - { by: process:just-multi-owner-demo, at: 2026-07-28T00:00:00Z }
---

# What it proves

A zero-dependency script that drives rakaia's newest effect/projection
primitives end-to-end through a ~90-line in-memory executor standing in for
`DjangoExecutor`. It proves: `Ref`/`RefResolver` binding an FK to a sibling
effect's generated primary key (with `UnresolvedRefError` on a dangling ref);
`reconcile_aggregate(owns=)` composing a shared row across two owners so a
vanished group null-clears only its own columns; `reconcile_by_key(retire=)`
soft-deleting stale rows on a composite natural key; `check_disjoint_defaults`
catching a same-column collision; and `dispatch_external` routing
`op="external"` effects.

# Run

```sh
just multi-owner-demo
```

# Concepts demonstrated

* [Effects & executors](../concepts/effects-and-executors.md)
* [Projections & fan-out](../concepts/projections-and-fan-out.md)
