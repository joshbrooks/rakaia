---
type: Example
title: "two_workers — one consumer name, two processes"
description: "Two worker processes sharing one consumer name: the second waits on the cursor row rather than applying the same events, the commit is forward-only, and a resume reads the delta instead of the log."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/two_workers
tags: [example, django, concurrency, consuming, postgres]
status: stable
generated: { by: opencode/deepseek-v4.1-flash, at: 2026-11-01T00:00:00Z }
verified:
  - { by: process:just-two-workers-demo-pg, at: 2026-11-01T00:00:00Z }
---

# What it proves

Two worker processes start under one consumer name. The first holds the pass,
and the second is observed — in `pg_stat_activity`, not on a stopwatch — waiting
on the cursor row. It then polls `caught_up` and applies nothing, so 200 events
produce exactly 200 rows. A manual commit behind the saved position raises
`CursorMovedBackwards` and writes nothing. Appending five events and polling
from the saved cursor returns five, not the 205 in the log: a resume is a delta,
not a replay.

The lock half needs a backend with row locks. On SQLite the demo prints
`skipped: SQLite has no row locks` and runs one worker; the forward-only and
resume checks run on both backends.

# Run

```sh
just two-workers-demo       # SQLite — the lock check is skipped, with the reason
just two-workers-demo-pg    # Postgres — the lock check runs for real
```

# Concepts demonstrated

* [Consuming & outcomes](../concepts/consuming-and-outcomes.md)
* [Django integration](../concepts/django-integration.md)
* [Protocol layer & streams](../concepts/protocol-and-streams.md)
