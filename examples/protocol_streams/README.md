# protocol_streams — the raw Durable Streams protocol (no Django)

Every other example mounts rakaia in Django and drives the **event-sourcing
layer** (versioned handlers, projections, replay). This one exercises the *other
half*: the zero-dependency `StreamStore` and the pure-protocol primitives that
sit underneath all of that. It imports nothing but `rakaia` and the stdlib.

It is the runnable companion to [`docs/protocol.md`](../../docs/protocol.md),
[`docs/producer-fencing.md`](../../docs/producer-fencing.md) and
[`docs/subscriber-cursors.md`](../../docs/subscriber-cursors.md).

## Run

```sh
just protocol-demo
# or: uv run python examples/protocol_streams/demo.py
```

No database, no migrations, no server — it runs in-process and asserts each
outcome, so a protocol-layer regression turns the final
`All protocol checks passed ✓` into a stack trace.

## What it proves

| Section | Primitive | Point |
|---|---|---|
| 1 | `StreamStore.append` / `read` | An ordered, offset-addressed log; reads resume from an offset. |
| 2 | `append_if_changed` / `snapshots_equal` | No-op saves are suppressed (write-side pghistory parity). |
| 3 | `AppendOptions(producer_id, epoch, seq)` → `ProducerAccepted`/`Duplicate`/`SequenceGap`/`StaleEpoch`/`InvalidEpochSeq` | Idempotent retries, gap detection, and zombie fencing across an epoch change. |
| 4 | `close_stream` → `CloseResult` | A closed stream is sealed; later appends are refused, idempotent re-close. |
| 5 | `poll` → `Poll` / `PollStatus` | Incremental subscriber cursors: `fresh` → `caught_up` → `advanced` (delta only) → `rewound`. |
| 6 | `calculate_cursor` / `generate_response_cursor` | CDN cache-collapsing interval cursors with monotonic progression (protocol §8.1). |
