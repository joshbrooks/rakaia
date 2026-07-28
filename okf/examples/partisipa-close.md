---
type: Example
title: "partisipa_close — close-precondition state machine"
description: "Decides a POM_1 cycle-close purely from cross-form projected state: a guarded transition rejects a premature close, then self-heals once the preconditions are met."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/partisipa_close
tags: [example, django, spike, state-machine]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
verified:
  - { by: process:just-partisipa-close-demo, at: 2026-07-28T00:00:00Z }
---

# What it proves

Decides a POM_1 cycle-close purely from cross-form projected state: a guarded
transition rejects a premature close, then self-heals once the preconditions
are met. Demonstrates staged replay with per-stage reducers and
`reconcile_children`.

# Run

```sh
just partisipa-close-demo
```

# Concepts demonstrated

* [Versioned handlers & replay](../concepts/versioned-handlers-and-replay.md)
* [Projections & fan-out](../concepts/projections-and-fan-out.md)
