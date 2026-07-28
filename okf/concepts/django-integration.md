---
type: Concept
title: Django integration
description: Emit stream events from Django models and fan out changes over Server-Sent Events.
tags: [concept, django, sse]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
---

# Definition

`django_rakaia` mounts rakaia in Django: model saves emit stream events (one save
can fan out to several stream paths), events are stored durably in normalized
`Stream` / `StreamEvent` / `StreamEntry` tables, and changes broadcast to browsers
over Server-Sent Events via Django Channels. It provides the ORM-backed executor
and reader that the [event-sourcing layer](../concepts/versioned-handlers-and-replay.md)
runs against.

# Public API

From `django_rakaia`:

* `@stream_model(stream_paths=…, to_dataclass=…)` — decorate a model so saves/
  deletes emit events.
* `create_stream_event(...)` — emit an event manually (e.g. for built-in models).
* `DjangoStreamStore` — the durable, DB-backed store (`RAKAIA_STORE="durable"`).
* `DjangoExecutor`, `DjangoProjectionReader`, `replay_stream` — apply/replay
  against the ORM.
* `diff_effects_against_rows` — migration/verification helper.
* SSE views + Channels signals for live broadcast.

# Demonstrated by

* [chat](../examples/chat.md) — `@stream_model`, multi-stream events, live SSE.
* [polyglot](../examples/polyglot.md) — `create_stream_event`, language-scoped streams, SSE.
* [formkit_submissions (stream)](../examples/formkit-submission-stream.md) — durable `DjangoStreamStore`.

# Deeper reference

* Human docs: `docs/django-integration.md`, `docs/streams-backend-storage.md`, `docs/deployment.md`.
* Source: `src/django_rakaia/`.
