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
* Drift fingerprints (`rakaia.source_hash.fingerprint_handler`) cover a handler's source, the
  plain functions it calls and the constants it reads, and are prefixed `v2:`; see
  `RAKAIA_STRICT_HANDLER_FINGERPRINTS` in [Django integration](django-integration.md).
  A `functools.partial` is unwrapped, so its hash still describes the wrapped function;
  a closure hashes its own wrapper, so an edit to what it calls is invisible.
* Declared tables: `register(..., reads=…, writes=…)` (and the same keywords on
  `register_handler` / `register_simple`) name the tables (`"app.Model"`) or columns
  (`"app.Model.field"`) a handler touches. Replay warns with `UndeclaredTableWarning` at most
  once per handler and name in a replay when an emitted effect or a reader call falls outside them. Not part
  of the handler's identity, so declaring them appends nothing to the meta-stream (#375).
* Registries and drift errors: `HandlerRegistry`, `UpcasterRegistry`,
  `HandlerVersion`, `ReducerVersion`, `HandlerOverlapError`, `HandlerGapError`,
  `HandlerDriftError`, `UpcasterConflictError`, `UpcasterChainError`.
* Results: `ReplayResult`, `TouchedSubject`, `ENVELOPE_TS`.

# Demonstrated by

* [orders](../examples/orders.md) — `effective_from`/`effective_to`, upcasters, drift.
* [orders (hermeticity)](../examples/orders.md) — impure handlers, strict fingerprints, partial vs closure.
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
* Source: `src/rakaia/registry.py`, `src/rakaia/replaying.py`.
