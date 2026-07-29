---
type: Example
title: "protocol_streams — the raw Durable Streams protocol (no Django)"
description: "A zero-dependency script (imports only `rakaia` + the stdlib) that exercises the non-Django half of rakaia."
resource: https://github.com/joshbrooks/rakaia/tree/main/examples/protocol_streams
tags: [example, standalone, protocol]
status: stable
generated: { by: claude-code/opus-4-8, at: 2026-07-28T00:00:00Z }
verified:
  - { by: process:just-protocol-demo, at: 2026-07-28T00:00:00Z }
---

# What it proves

A zero-dependency script (imports only `rakaia` + the stdlib) that exercises
the non-Django half of rakaia. It asserts, in-process: an ordered offset-
addressed log (`StreamStore` append/read with resumable offset reads); no-op
suppression (`append_if_changed` / `snapshots_equal`); producer fencing
(accepted / duplicate / sequence-gap / stale-epoch / invalid-epoch across an
epoch change); stream `close_stream` sealing; subscriber cursors (`poll` ->
fresh / caught_up / advanced / rewound); and CDN interval cursors
(`calculate_cursor` / `generate_response_cursor`).

# Run

```sh
just protocol-demo
```

# Concepts demonstrated

* [Protocol layer & streams](../concepts/protocol-and-streams.md)
