---
type: Example
title: "partisipa_history — pghistory-retirement spike"
description: "Reproduces django-pghistory's audit log and blank-save recovery from an enveloped stream: a deleted submission's row is gone but every history row is retained."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/partisipa_history
tags: [example, django, spike, history]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
verified:
  - { by: process:just-partisipa-history-demo, at: 2026-07-28T00:00:00Z }
---

# What it proves

Reproduces django-pghistory's audit log and blank-save recovery from an
enveloped stream: a deleted submission's row is gone but every history row is
retained. Demonstrates the history read-model and `recover_peak_snapshot`.

# Run

```sh
just partisipa-history-demo
```

# Concepts demonstrated

* [Event envelope & provenance](../concepts/event-envelope-and-provenance.md)
