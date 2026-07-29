---
okf_version: "0.2"
---

# Rakaia knowledge bundle

An [Open Knowledge Format](https://github.com/GoogleCloudPlatform/knowledge-catalog)
bundle describing **rakaia** — a Python implementation of the Durable Streams
protocol plus a Django event-sourcing layer — for machine consumption by agents
and tools.

It catalogs two things:

* **Concepts** — rakaia's public API surface, grouped into six areas, each with
  the APIs it covers and the examples that demonstrate it.
* **Examples** — every runnable demo in `examples/`, what it proves, the one
  command that runs it, and which concepts it exercises. Demos verified green are
  marked with a `verified` entry.

The human-facing equivalent is the docs site page `docs/examples.md` ("Examples &
concept coverage"); this bundle is its structured, cross-linked counterpart.

## Concepts

* [Protocol layer & streams](concepts/protocol-and-streams.md) - the zero-dependency Durable Streams server.
* [Versioned handlers & replay](concepts/versioned-handlers-and-replay.md) - pure event→Effect handlers, replayed time-correctly.
* [Effects & executors](concepts/effects-and-executors.md) - effect data, executors, dry-run, symbolic refs.
* [Projections & fan-out](concepts/projections-and-fan-out.md) - orphan-free child/aggregate reconcile helpers.
* [Event envelope & provenance](concepts/event-envelope-and-provenance.md) - the event envelope and history read-model.
* [Django integration](concepts/django-integration.md) - stream events from models, live SSE.

## Examples

See the [examples index](examples/index.md) for the full list, or start with:

* [protocol_streams](examples/protocol-streams.md) - the protocol layer, no Django.
* [multi_owner](examples/multi-owner.md) - the newest effect/projection primitives, no Django.
* [orders](examples/orders.md) - versioned handlers, upcasters, replay, dry-run.
