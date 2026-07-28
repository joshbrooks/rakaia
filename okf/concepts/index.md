# Concepts

Rakaia's public API surface, grouped into six areas. Each concept doc lists the
APIs it covers, the examples that demonstrate it, and any still-uncovered APIs.

* [Protocol layer & streams](protocol-and-streams.md) - `StreamStore`, producer fencing, close, subscriber/CDN cursors, no-op suppression.
* [Versioned handlers & replay](versioned-handlers-and-replay.md) - `register_handler`, upcasters, staged `replay`, `merge_replay`, reducers.
* [Effects & executors](effects-and-executors.md) - `Effect`, `Ref`/`RefResolver`, `CollectingExecutor`/`DjangoExecutor`, `dispatch_external`, verification.
* [Projections & fan-out](projections-and-fan-out.md) - `reconcile_children`/`_by_key`/`_tree`/`_aggregate`, `project_latest`.
* [Event envelope & provenance](event-envelope-and-provenance.md) - envelope fields, `provenance()`, history read-model.
* [Django integration](django-integration.md) - `@stream_model`, `create_stream_event`, durable store, SSE.
