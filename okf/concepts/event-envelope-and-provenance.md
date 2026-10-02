---
type: Concept
title: Event envelope & provenance
description: The label/metadata/timestamp envelope on each event, and the history read-model derived from it.
tags: [concept, event-sourcing, history]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
---

# Definition

Each appended event can carry an envelope: a change `label` (create/update/delete
→ +/~/-), an open `metadata` dict (actor, url, causation), and a logical
`event_ts`. `provenance()` attaches an ambient actor to appends within its scope.
Because the log retains every enveloped event, a stream reproduces a
django-pghistory-style audit trail and can recover a "peak" snapshot even after a
blank save — the history read-model.

# Public API

Imported from `rakaia`:

* Envelope fields on `AppendOptions(label, metadata, event_ts, tags,
  correlation_id)` and `StreamMessage(label, metadata, event_ts, tags,
  correlation_id)`. Tags read back sorted and de-duplicated; a tag is at most
  100 characters and a correlation id at most 128, checked before anything is
  written (`ValueError`).
* `AppendResult.event_id` — the `StreamEvent` id from `DjangoStreamStore`;
  `None` from the in-memory and JSONL stores.
* `provenance(...)` context manager; `get_provenance()`.
  `provenance(correlation=...)` supplies the correlation id of an append that
  gives none; an explicit one wins. Unrelated to `Upsert(produces=...)`.
* History helpers: `history_effects`, `recover_peak_snapshot`, `label_marker`,
  `envelope_actor`.

# Demonstrated by

* [formkit_submissions (stream)](../examples/formkit-submission-stream.md) — envelope, `history_effects`, actor recovery.
* [partisipa_history](../examples/partisipa-history.md) — pghistory-parity audit + `recover_peak_snapshot`.
* [formkit_submissions](../examples/formkit-submissions.md) — `AppendOptions(label=…)` + `provenance()`.

# Deeper reference

* Human docs: `docs/event-envelope.md`, `docs/history-read-model.md`, `docs/pghistory-retirement.md`.
* Source: `src/rakaia/history.py`, `src/rakaia/context.py`.
