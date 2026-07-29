---
type: Example
title: "partisipa_merge — multi-stream deterministic merge"
description: "Merges three form streams into one deterministic replay order and asserts parity plus stable tie-breaks."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/partisipa_merge
tags: [example, django, spike, merge]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
verified:
  - { by: process:just-partisipa-merge-demo, at: 2026-07-28T00:00:00Z }
---

# What it proves

Merges three form streams into one deterministic replay order and asserts
parity plus stable tie-breaks. Demonstrates `merge_replay`, a cross-stream
readiness rollup, and self-healing on re-merge.

# Run

```sh
just partisipa-merge-demo
```

# Concepts demonstrated

* [Versioned handlers & replay](../concepts/versioned-handlers-and-replay.md)
