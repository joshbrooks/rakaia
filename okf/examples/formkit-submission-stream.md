---
type: Example
title: "formkit_submissions (stream) — append log as source of truth"
description: "The arrow-flip (Decision #13): a `SubmissionEvent` append log is the source of truth and `Submission` is a `project_latest` projection rebuilt from it, durable across processes."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/formkit_submissions/submission_stream
tags: [example, django, projections, event-sourcing]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
verified:
  - { by: process:just-formkit-stream-demo, at: 2026-07-28T00:00:00Z }
---

# What it proves

The arrow-flip (Decision #13): a `SubmissionEvent` append log is the source of
truth and `Submission` is a `project_latest` projection rebuilt from it,
durable across processes. Demonstrates `project_latest`, the durable
`DjangoStreamStore`, the history read-model (`history_effects`), a self-
healing reprojection, and tombstone deletes.

# Run

```sh
just formkit-stream-demo
```

# Concepts demonstrated

* [Projections & fan-out](../concepts/projections-and-fan-out.md)
* [Event envelope & provenance](../concepts/event-envelope-and-provenance.md)
* [Django integration](../concepts/django-integration.md)
