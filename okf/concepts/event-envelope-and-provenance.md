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

* Envelope fields on `AppendOptions(label, metadata, event_ts)` and
  `StreamMessage(label, metadata, event_ts)`.
* `provenance(...)` context manager; `get_provenance()`.
* History helpers: `history_effects`, `recover_peak_snapshot`, `label_marker`,
  `envelope_actor`.

# Demonstrated by

* [formkit_submissions (stream)](../examples/formkit-submission-stream.md) — envelope, `history_effects`, actor recovery.
* [partisipa_history](../examples/partisipa-history.md) — pghistory-parity audit + `recover_peak_snapshot`.
* [formkit_submissions](../examples/formkit-submissions.md) — `AppendOptions(label=…)` + `provenance()`.

# Deeper reference

* Human docs: `docs/event-envelope.md`, `docs/history-read-model.md`, `docs/pghistory-retirement.md`.
* Source: `src/rakaia/history.py`, `src/rakaia/context.py`.
