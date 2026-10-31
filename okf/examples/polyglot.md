---
type: Example
title: "polyglot — language-scoped streams, live-editable translations"
description: "Live-editable translations delivered over language-scoped streams and SSE, multi-instance via channels-redis."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/polyglot
tags: [example, django, live-sse, translations]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
verified:
  - { by: process:manage.py-check, at: 2026-07-28T00:00:00Z }
---

# What it proves

Live-editable translations delivered over language-scoped streams and SSE,
multi-instance via channels-redis. Demonstrates `create_stream_event`,
language-scoped stream paths, and SSE fan-out.

# Run

```sh
just polyglot-dev
just migrate-consumer-demo
```

`migrate-consumer-demo` is headless: it consumes a JSONL stream, copies it into
SQLite with `migrate_stream`, and shows the consumer resume when the offsets and
stamp survive. It then copies a stream whose offsets cannot line up, and the same
consumer stops as `replaced` because the copy kept its own stamp.

# Concepts demonstrated

* [Django integration](../concepts/django-integration.md)
* [Consuming a stream & failure records](../concepts/consuming-and-outcomes.md)
