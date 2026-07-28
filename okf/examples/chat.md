---
type: Example
title: "chat — @stream_model, multi-stream events, live SSE"
description: "A live chat app: each message save emits events to two streams (room + user activity) and fans out to browsers over Server-Sent Events."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/chat
tags: [example, django, live-sse]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
verified:
  - { by: process:manage.py-check, at: 2026-07-28T00:00:00Z }
---

# What it proves

A live chat app: each message save emits events to two streams (room + user
activity) and fans out to browsers over Server-Sent Events. Demonstrates the
`@stream_model` decorator, multi-stream events per save, and Channels-backed
SSE.

# Run

```sh
just dev
```

# Concepts demonstrated

* [Django integration](../concepts/django-integration.md)
