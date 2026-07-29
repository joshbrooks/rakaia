---
type: Concept
title: Versioned handlers & replay
description: Pure event→Effect handlers registered against sequence ranges, replayed for time-correct projections.
tags: [concept, event-sourcing, replay]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
---

# Definition

Projections are *derived* from the event log by pure functions (handlers) that
map an event to an [Effect](../concepts/effects-and-executors.md). Handlers are
registered against a **sequence range**, so an old event runs through the handler
version that was correct *when it happened* — fixing a rule forward never
rewrites the past. `replay` folds a stream through the registered handlers and
applies the effects; multi-stage replay lets a later stage read what earlier
stages materialised, resolving cross-entity references regardless of arrival order.

# Public API

Imported from `rakaia`:

* `register_handler(name, event_match, effective_from, effective_to, stage=…,
  match_field=…)` — versioned, optionally staged, optionally content-routed.
* `register_simple(name, event_match)` — an always-on, single-version handler.
* `register_reducer(name, stage)` — a per-stage reduce step run once after the
  stage's per-event handlers (`fn(reader)` or `fn(reader, touched)`).
* `register_upcaster` / `upcast` — normalise old event shapes before handlers run.
* `replay(store, stream_path, executor, *, reader=…, on_drift=…, start_seq, end_seq)`.
* `merge_replay(store, stream_paths, executor, *, order_key=…)` — replay N streams
  in one deterministic total order (`order_key=ENVELOPE_TS` for envelope time).
* Registries and drift errors: `HandlerRegistry`, `UpcasterRegistry`,
  `HandlerVersion`, `ReducerVersion`, `HandlerOverlapError`, `HandlerGapError`,
  `HandlerDriftError`, `UpcasterConflictError`, `UpcasterChainError`.
* Results: `ReplayResult`, `TouchedSubject`, `ENVELOPE_TS`.

# Demonstrated by

* [orders](../examples/orders.md) — `effective_from`/`effective_to`, upcasters, drift.
* [projection_cookbook](../examples/projection-cookbook.md) — staged replay, `register_simple`, `match_field`, reader.
* [partisipa_staged](../examples/partisipa-staged.md) — staged replay for late-arriving links.
* [partisipa_close](../examples/partisipa-close.md) — staged replay with per-stage reducers.
* [partisipa_merge](../examples/partisipa-merge.md) — `merge_replay`.
* [formkit_submissions](../examples/formkit-submissions.md) — versioned handlers + upcasters.

# Known gaps

* `register_reducer` as the public API is not directly used by an example (the
  partisipa demos wire reducers via raw `{"reduce": […]}` stage config).
* `replay_stream` (the Django convenience wrapper) has no example.

# Deeper reference

* Human docs: `docs/versioned-handlers.md`, `docs/staged-replay.md`, `docs/multi-stream-merge.md`.
* Source: `src/rakaia/registry.py`, `src/rakaia/replay.py`.
