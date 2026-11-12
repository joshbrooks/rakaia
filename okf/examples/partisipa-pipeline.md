---
type: Example
title: "partisipa_pipeline — one suku's forms, step by step"
description: "Saves seven forms through a six-step pipeline, then rebuilds every table from the log and compares it with live."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/partisipa_pipeline
tags: [example, django, staged-replay, reducers, rebuild, verification, explainers]
status: stable
generated: { by: claude-code/opus-5, at: 2026-11-12T00:00:00Z }
verified:
  - { by: process:just-pipeline-demo, at: 2026-11-12T00:00:00Z }
---

# What it proves

A save and a rebuild produce the same tables when both take their handlers from
one ordered list of steps. Each form is appended with `append_event` and then
replayed on its own (`replay` with `start_seq`/`end_seq` set to that one event).
The rebuild replays the whole stream into an empty alias through
`rebuild_and_verify`, which reports `green`, and then `red` naming the field once
a live row is changed by hand.

Along the way it uses `reconcile_children`, so a resubmitted report loses the
line it dropped. A save assigns each report to the registration for its suku and
output and records the assignment, with its reason, as an event in the log; a
report saved before its registration is assigned by the save that brings it, and
a rebuild replays the assignments without deciding. A stage-1 reducer
(`register_reducer`) adds up the totals, and a stage-2 reducer works out
whether the month is ready. The email
step is an `ExternalEffect`, which the save sends and the rebuild drops. It is
the one step whose list entry says, with a reason, that a rebuild does not
reproduce it.

The step list (`pipeline/steps.py`) is the example's own code, modelled on
partisipa-import's proposal (catalpainternational/partisipa-import GitHub #3443).
It is not a rakaia API; joshbrooks/rakaia GitHub #374 asks for one.

# Run

```sh
just pipeline-demo                    # every step, then rebuild and compare
just pipeline-demo --until totals     # stop after one step
```

The plain-language walkthrough is `docs/explainers/`.

# Concepts demonstrated

* [Versioned handlers & replay](../concepts/versioned-handlers-and-replay.md)
* [Projections & fan-out](../concepts/projections-and-fan-out.md)
* [Effects & executors](../concepts/effects-and-executors.md)
* [Django integration](../concepts/django-integration.md)
