---
type: Concept
title: Protocol layer & streams
description: The zero-dependency Durable Streams server underneath rakaia's event-sourcing layer.
tags: [concept, protocol, standalone]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
---

# Definition

The lower of rakaia's two layers: an append-only, ordered, offset-addressed byte
log (`StreamStore`) implementing the [Durable Streams protocol]. It has no Django
and no third-party dependencies. Everything in the [event-sourcing layer]
(../concepts/versioned-handlers-and-replay.md) is built on top of it.

# Public API

Imported from `rakaia`:

* `StreamStore` — `create` / `append` / `read` / `close_stream` / `delete` /
  `get_current_offset` / `get_generation` / `adopt_generation`. `read(path, offset)`
  returns messages after an offset, so a reader can resume, and takes a `limit`
  when the caller wants one page of them. `delete` takes the events only that
  stream referred to; one shared with another stream is kept.
  `get_generation` is the stamp that changes when a stream is recreated;
  `adopt_generation` gives a copied stream the source's stamp, for `rakaia.migrate`
  alone, and only when the copy kept every offset — so a consumer that ran before
  the migration resumes instead of stopping as `replaced`. A custom target without
  it still migrates, but the consumer stops, and `Migration.notes` says so.
* `AppendOptions(producer_id, producer_epoch, producer_seq, close, label,
  metadata, event_ts)` and `AppendResult(message, producer_result,
  stream_closed)`.
* Producer fencing result types: `ProducerAccepted`, `ProducerDuplicate`,
  `ProducerStaleEpoch`, `ProducerInvalidEpochSeq`, `ProducerSequenceGap`,
  `ProducerStreamClosed` (idempotent retries, gap detection, zombie fencing).
* `close_stream` -> `CloseResult(final_offset, already_closed)`; `ClosedBy`.
* `append_if_changed` / `snapshots_equal` — write-side no-op suppression.
* Subscriber cursors: `poll(store, path, cursor)` -> `Poll(messages, cursor,
  status)`; `PollStatus` is one of `fresh` / `advanced` / `caught_up` / `rewound`
  / `absent`.
* CDN cursors: `calculate_cursor`, `generate_response_cursor`, `CursorOptions`.
* `ServerOptions(authorize=…, allowed_origins=…, max_body_bytes=…, sse_max_seconds=…)` —
  a `(scope, action)` access hook (default allows all), the CORS origin list (default
  `*`), the body limit that answers `413` (default 8 MiB), and the timer after which
  a live SSE connection is closed (default 60 s). An SSE handler also ends when the
  client disconnects.
* `ServerOptions(read_page_size=…)` — the most messages one catch-up read answers
  with, a thousand by default and `None` to turn paging off. A page that stops
  short omits `Stream-Up-To-Date` and points `Stream-Next-Offset` at its last
  message, which the protocol permits, so a conforming client reads on unchanged.
* `RAKAIA_ENABLE_FAULT_INJECTION` — an environment variable, not a Django setting, read
  when `ServerOptions(enable_fault_injection=…)` is left at its default. A truthy value
  (`1`/`true`/`yes`/`on`) routes the unauthenticated `/_test/inject-error` endpoint; off,
  that path answers `404` like any other. Never set it on a deployed server.
* Permanent streams: `ExpiryNotAllowed` (a create asking for a TTL or an expiry,
  answered `400`) and `DeleteNotAllowed` (a client `DELETE`, answered `405`).
  Raised by a store whose streams are permanent: `StreamStore(permanent=True)`,
  `JsonlStreamStore(..., permanent=True)`, or `DjangoStreamStore` under
  `RAKAIA_PERMANENT_STREAMS`. `delete(path, force=True)` deletes from Python anyway.
* `JsonlStreamStore` records its `segment_size` per stream and raises
  `SegmentSizeMismatch` when opened with another; its deletes and creates take the
  stream's lock, so they wait for a writer instead of failing it.
* The time rakaia stamps on an event is taken after its position is settled and is
  never earlier than that stream's previous stamp, so ordering by `event_ts` agrees
  with ordering by offset. A producer's own `event_ts` is stored as sent.

* Writing your own store: `plan_appends(facts, items, *, payloads, producer_states,
  now)` decides a whole batch of appends (all or nothing) and returns an `AppendPlan` of
  `AppendVerdict`s and `PlannedWrite`s; `StreamFacts(closed, closed_by, content_type,
  last_seq)` is what it is told about the stream; `plan_initial_write(content_type,
  data)` does the same for a create's body; `AppendPlan.stamped(last_event_ts, now)` fills
  in the store's own event times. `page_of(messages, limit)` cuts a read to
  `(page, up_to_date)` and `check_page_size(limit)` refuses a limit below 1. These are
  the rules `StreamStore`, `JsonlStreamStore` and `DjangoStreamStore` share, so a store
  that calls them cannot disagree with those three.
* `rakaia.testing` — the contract suites a store, executor, outcome store or reader is
  held to, shipped in the wheel: `store_contract.StoreContract` (`WritableStore`),
  `server_store_contract.ServerStoreContract` (`StreamServerStore`),
  `executor_contract.ExecutorContract`, `outcome_store_contract.OutcomeStoreContract`
  and `projection_reader_contract.ProjectionReaderContract`. Subclass one in your own
  tests and provide the fixture it names. The package and the five module paths are
  Tier 1. Importing a suite needs pytest (all but `outcome_store_contract`) and says so if it is missing;
  `import rakaia.testing` does not.

# Demonstrated by

* [protocol_streams](../examples/protocol-streams.md) — every item above, asserted in one script.

# Deeper reference

* Human docs: `docs/protocol.md`, `docs/producer-fencing.md`, `docs/subscriber-cursors.md`,
  `docs/write-your-own-store.md`.
* Source: `src/rakaia/store.py`, `src/rakaia/subscription.py`, `src/rakaia/append.py`, `src/rakaia/cursor.py`, `src/rakaia/types.py`.
