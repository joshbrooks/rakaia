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
```

# Concepts demonstrated

* [Django integration](../concepts/django-integration.md)
