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
* `stream_coverage` and `manage.py check_stream_coverage` — check that a stream
  still has an event for every row of the table it is written from.
* SSE views + Channels signals for live broadcast.

Settings and operator commands:

* `RAKAIA_READ_PAGE_SIZE` — the page a catch-up read answers with, applied when
  `get_asgi_app()` is called without `options`; on the durable store the page is a
  SQL `LIMIT`, so a first sync no longer loads the whole log.
* `RAKAIA_PERMANENT_STREAMS` — off by default. With it on the durable store
  refuses a create that asks for a TTL or an expiry (`ExpiryNotAllowed`) and a
  client's `DELETE` (`DeleteNotAllowed`), and serves an already-expired stream
  instead of removing it. `DjangoStreamStore.delete()` from Python still works.
* `manage.py prune_orphan_events` — delete events no stream refers to, with
  `--dry-run`, `--batch-size` and `--database`. For payloads left behind by
  deletes made before a stream's deletion took its events with it.
* A model save locks its streams in path order, and before reserving their
  offsets — the same order a protocol append takes them in, so the two cannot
  deadlock each other on Postgres.

# Demonstrated by

* [chat](../examples/chat.md) — `@stream_model`, multi-stream events, live SSE.
* [polyglot](../examples/polyglot.md) — `create_stream_event`, language-scoped streams, SSE.
* [formkit_submissions (stream)](../examples/formkit-submission-stream.md) — durable `DjangoStreamStore`.
* [partisipa_intake](../examples/partisipa-intake.md) — `django_consumer`, durable reading position, outcome records.
* [stream_coverage](../examples/stream-coverage.md) — `check_stream_coverage` finding missing and stale rows, and passing after repair.

# Deeper reference

* Human docs: `docs/django-integration.md`, `docs/streams-backend-storage.md`, `docs/deployment.md`.
* Source: `src/django_rakaia/`.
