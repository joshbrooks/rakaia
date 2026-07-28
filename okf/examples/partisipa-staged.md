---
type: Example
title: "partisipa_staged — staged replay for late-arriving links"
description: "A spike on a real Partisipa form pipeline: reproduces an unlinked cross-form reference bug, then resolves it with staged replay — stage 0 builds reference entities, stage 1 resolves links via the reader regardless of arrival order — and self-heals on re-replay."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/partisipa_staged
tags: [example, django, spike, staged-replay]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
verified:
  - { by: process:just-partisipa-demo, at: 2026-07-28T00:00:00Z }
---

# What it proves

A spike on a real Partisipa form pipeline: reproduces an unlinked cross-form
reference bug, then resolves it with staged replay — stage 0 builds reference
entities, stage 1 resolves links via the reader regardless of arrival order —
and self-heals on re-replay.

# Run

```sh
just partisipa-demo
```

# Concepts demonstrated

* [Versioned handlers & replay](../concepts/versioned-handlers-and-replay.md)
