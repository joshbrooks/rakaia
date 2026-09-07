---
type: Example
title: "partisipa_intake — the consuming loop, end to end"
description: "Consumes a stream with a durable reading position and records every event refused, failed or skipped."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/partisipa_intake
tags: [example, django, consumer, outcomes]
status: stable
generated: { by: claude-code/opus-5, at: 2026-09-07T00:00:00Z }
verified:
  - { by: process:just-intake-demo, at: 2026-09-07T00:00:00Z }
---

# What it proves

Submits progress forms with repeating rows and consumes them with
`django_consumer(store, path, name)` and `run(apply, on_error=…)`, keeping both
the reading position and the outcome records in the database.

Each of the three failure paths leaves the record ADR 0007 names for it: a row
the submitting side's rules decline never reaches the log and is recorded at
stage `append` with status `refused` and no offset; an event that fails to apply
is recorded at stage `project` with status `failed`, with the reading position
left *below* it under `on_error="halt"`; an event for a closed reporting period
is recorded as `skipped` and the position advances. A run under
`on_error="skip"` advances past the same failure, and a fresh consumer object
reads the position and every record back.

# Run

```sh
just intake-demo
```

# Concepts demonstrated

* [Django integration](../concepts/django-integration.md)
* [Protocol & streams](../concepts/protocol-and-streams.md)
