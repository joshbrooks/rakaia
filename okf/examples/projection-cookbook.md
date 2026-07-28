---
type: Example
title: "projection_cookbook — staged replay + reader + verification"
description: "A two-form staged projection: stage 0 builds Projects, stage 1 links Tasks that may have arrived before their Project, resolved via a `ProjectionReader`."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/projection_cookbook
tags: [example, django, staged-replay]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
verified:
  - { by: process:just-cookbook-demo, at: 2026-07-28T00:00:00Z }
---

# What it proves

A two-form staged projection: stage 0 builds Projects, stage 1 links Tasks
that may have arrived before their Project, resolved via a `ProjectionReader`.
Demonstrates staged `replay`, `register_simple` with `match_field` routing,
`DjangoProjectionReader`, and `diff_effects_against_rows` verification that
replay reproduces the rows.

# Run

```sh
just cookbook-demo
```

# Concepts demonstrated

* [Versioned handlers & replay](../concepts/versioned-handlers-and-replay.md)
* [Effects & executors](../concepts/effects-and-executors.md)
