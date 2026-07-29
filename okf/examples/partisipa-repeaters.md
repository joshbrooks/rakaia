---
type: Example
title: "partisipa_repeaters — nested-repeater tree reconcile"
description: "Resubmits a pruned repeater tree and asserts no deep orphans and no double-count."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/partisipa_repeaters
tags: [example, django, spike, tree]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
verified:
  - { by: process:just-partisipa-tree-demo, at: 2026-07-28T00:00:00Z }
---

# What it proves

Resubmits a pruned repeater tree and asserts no deep orphans and no double-
count. Demonstrates a whole-subtree reconcile using `delete` + `exclude`
scoped by node id (the tree generalisation of `reconcile_children`).

# Run

```sh
just partisipa-tree-demo
```

# Concepts demonstrated

* [Projections & fan-out](../concepts/projections-and-fan-out.md)
