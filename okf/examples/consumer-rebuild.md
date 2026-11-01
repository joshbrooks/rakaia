---
type: Example
title: "consumer_rebuild — a reader meets a changed log"
description: "A consumer whose stream was deleted and recreated, a transient apply error, and a pass that dies halfway."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/consumer_rebuild
tags: [example, django, consumer, outcomes, cursors]
status: stable
generated: { by: claude-code/opus-5, at: 2026-11-01T00:00:00Z }
verified:
  - { by: process:just-consumer-rebuild-demo, at: 2026-11-01T00:00:00Z }
---

# What it proves

A durable consumer facing the three things a happy-path demo never meets, with
`django_consumer(store, path, name)` and `run(apply, on_error="halt")`.

* **Replaced.** The stream is deleted and recreated, which issues a new stamp,
  and more events are appended. The next run reports `replaced`, applies zero
  rows, records no outcome and leaves the cursor and its saved stamp unchanged
  (`Consumed.needs_rebuild`). The demo then clears the derived rows, replays the
  recreated log and moves the position to the new head; the next event resumes
  from there rather than replaying the whole stream.
* **Transient.** An apply raises Django's `OperationalError`. The run reports
  `transient` and `halted`, records **no** outcome, leaves the cursor below the
  event and the next run hands back the same offset. A database blink is not a
  bad event, so it is not recorded as one.
* **Crash.** The process dies part-way through a five-event pass. The cursor and
  that pass's writes roll back together, the next run delivers every event again,
  and the projection converges because the apply writes absolute values.

# Run

```sh
just consumer-rebuild-demo
```

# Concepts demonstrated

* [Consuming a stream & failure records](../concepts/consuming-and-outcomes.md)
* [Django integration](../concepts/django-integration.md)
