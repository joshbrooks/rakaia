---
type: Example
title: "formkit_submissions — projections, fan-out & migration parity"
description: "An adoption spike for formkit-ninja: replaying a submission stream reproduces the same rows a direct `to_model()` writes (migration parity), while adding time-correctness."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/formkit_submissions
tags: [example, django, projections]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
verified:
  - { by: process:just-formkit-demo, at: 2026-07-28T00:00:00Z }
---

# What it proves

An adoption spike for formkit-ninja: replaying a submission stream reproduces
the same rows a direct `to_model()` writes (migration parity), while adding
time-correctness. Demonstrates `reconcile_children` fan-out, versioned
handlers, upcasters, and the event envelope (`AppendOptions`, `provenance()`).

# Run

```sh
just formkit-demo
```

# Concepts demonstrated

* [Projections & fan-out](../concepts/projections-and-fan-out.md)
* [Versioned handlers & replay](../concepts/versioned-handlers-and-replay.md)
* [Event envelope & provenance](../concepts/event-envelope-and-provenance.md)
