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
  `get_current_offset`. `read(path, offset)` returns messages after an offset, so
  a reader can resume, and takes a `limit` when the caller wants one page of them.
  `delete` takes the events only that stream referred to; one shared with another
  stream is kept.
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

# Demonstrated by

* [protocol_streams](../examples/protocol-streams.md) — every item above, asserted in one script.

# Deeper reference

* Human docs: `docs/protocol.md`, `docs/producer-fencing.md`, `docs/subscriber-cursors.md`.
* Source: `src/rakaia/store.py`, `src/rakaia/subscription.py`, `src/rakaia/append.py`, `src/rakaia/cursor.py`, `src/rakaia/types.py`.
