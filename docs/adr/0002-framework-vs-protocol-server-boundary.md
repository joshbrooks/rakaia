# ADR 0002 — Name the framework ↔ protocol-server boundary; formalize the framework seams

- **Status:** Proposed
- **Date:** 2026-07-21
- **Deciders:** rakaia maintainers
- **Related:** [`django-integration.md`](../django-integration.md),
  [`dry-run-and-executors.md`](../dry-run-and-executors.md),
  [`staged-replay.md`](../staged-replay.md), [`protocol.md`](../protocol.md),
  [ADR 0001](./0001-ordering-child-collections-in-projections.md);
  issues #41 (epic), #36, #37, #38, #39, #40. Origin: an architecture/seams review.

## Context

rakaia is effectively **two products in one package**:

- **Tier 1 — the event-sourcing framework.** `Effect` / `Executor` /
  handler·upcaster·reducer registries / `replay` / `merge_replay` / projections,
  reading through the `ReadableStore` and `ProjectionReader` protocols. Pure,
  dependency-inverted, deterministic, testable. This is the layer consumers build
  on, and its internals are well-designed.
- **Tier 2 — the Durable Streams protocol server.** `handler.py` (raw ASGI
  PUT/POST/GET/HEAD/DELETE), producer epoch/seq fencing, CDN cursors, SSE, TTL,
  and the full `StreamStore` lifecycle. Elaborate and self-consistent.

The two share one package, one `StreamMessage`, one `AppendOptions`, and one
`store` object — and **nearly every rough edge a consumer hits lives at the seam
where they fuse**, not inside either tier:

- The **event-sourcing envelope is a bolt-on**: `label`/`metadata` are optional
  fields on the transport `StreamMessage`, explicitly "ignored by the transport."
- **Two timestamps, one name.** `StreamMessage.timestamp` is transport time;
  `merge_replay`'s `order_key="ts"` reads a **payload body** field, yet its
  docstring calls that "the envelope timestamp."
- **The store is really two interfaces, and only the read half is a protocol.**
  `protocols.py` defines `ReadableStore` (and `ProjectionReader`) — but the
  write/lifecycle side is duck-typed, and neither protocol is exported in
  `rakaia.__all__`. `DjangoStreamStore` implements only the framework subset
  (`read`/`append`/`create`); it silently omits producer dedup, close, TTL, and
  long-poll. `RAKAIA_STORE` swaps stores by string with no interface check.
- **Routing is asymmetric.** Handlers can content-route (`match_field`);
  upcasters cannot — they match only the stream path.
- **Two unrelated concepts named "cursor"** (CDN cache-collapsing vs consumer
  subscription); reportedly **two HTTP protocol surfaces** (`handler.py` vs
  `protocol_views.py`) with different offset formats.

This has concrete downstream cost. In the first real consumer (Partisipa's
`rebuild_ida`): a per-form upcaster was **silently dormant** because upcasters
can't content-route on `submissions:<uuid>` streams; a projection broke because
the payload had no `ts` for `merge_replay` to order on; backfill needed a
hand-rolled `pgh_id` watermark because the **durable store has no producer
dedup** and nothing said so; and the consumer's own docs said a primitive was
"pending" when it had in fact shipped. Each traces to an under-formalized Tier-1
seam or to the unmarked Tier-1/Tier-2 boundary — not to a bug inside either tier.

The paradox: the **protocol** tier is more polished than the **framework**
tier's *extension points*, even though the framework is the part people extend.

## Decision

**Name the boundary and bring the framework-tier seams up to the protocol tier's
standard — without (yet) splitting the package.**

1. **Treat rakaia as two named layers** with an explicit, documented boundary.
   Keep Tier-1 (`replay`, registries, effects, projections) free of
   protocol-server assumptions; keep producer/epoch/cursor/SSE concerns in Tier-2.
   Ship a package-boundary doc with a "what requires Django / what is pure" matrix.
2. **Make the framework extension seams first-class protocols and export them:**
   `ReadableStore`, a new `WritableStore`/`AppendableStore`, `Executor`, and
   `ProjectionReader` in `rakaia.__all__`, with a **shared conformance suite**
   every store must pass (offset semantics, JSON mode, timestamp contract,
   append-requires-stream). *(#36)*
3. **Symmetric routing:** content-routing (`match_field`) is available to
   upcasters as well as handlers, so per-form schema evolution works on
   entity-keyed streams. *(#37)*
4. **Registries are injectable and resettable**, with a documented test-isolation
   story; correctness must not depend on the process-global singleton. *(#38)*
5. **Determinism is a written contract:** one canonical timestamp source for
   merge ordering (envelope, not an ambiguously-named payload field), documented
   upcaster-rewrites-history semantics, one offset contract across stores. *(#39)*
6. **Effect model hygiene:** the two row-sparing mechanisms (`exclude`,
   `spare_keys`) must not silently AND together; `external` effects get a
   documented dispatch story. *(#40)*
7. **The envelope is conceptually first-class** (label + metadata + timestamp) —
   the Tier-1 unit of an event — even while it physically rides on
   `StreamMessage`. Docs and naming treat it as such.

## Alternatives considered

| Option | Effort | Boundary clarity | Fixes consumer traps | Verdict |
|---|---|---|---|---|
| **Explicit boundary + formalize framework seams in place (chosen)** | Medium | High | Yes | **Chosen** |
| Full package split (`rakaia-streams` + `rakaia-projections`) | High | Highest | Yes | Deferred — high churn; the tiers still share `StreamMessage`/store; revisit if divergence grows |
| Boundary as docs only (no protocol formalization) | Low | Medium | No | Insufficient — the store/executor/reader seams need real, exported, conformance-tested protocols, not prose |
| Status quo (leave fused, undocumented) | None | None | No | Rejected — the fusion traps keep reaching consumers |

- **Full split** is the "correct" end-state but premature: the tiers genuinely
  share types and the store object today, so splitting now trades one set of seams
  for a harder cross-package one. This ADR keeps it on the table (item revisited
  when/if the durable store grows a real protocol-server implementation).
- **Docs-only** was rejected because the highest-impact issue (#36) is a *missing
  protocol + conformance suite*, which prose cannot substitute for.

## Consequences

**Positive**

- A consumer can see, in one page, which surface they're using and what a custom
  store/executor/reader must implement — the seams stop being reverse-engineered
  from code.
- The "test on the in-memory store, ship on the durable store" pattern becomes
  safe: a conformance suite makes divergence a test failure, not a production
  surprise.
- Per-form upcasting, deterministic merge ordering, and test isolation become
  supported rather than accidental.

**Negative / caveats**

- Adding `WritableStore` + a conformance suite is real work and may surface
  existing divergences (offset format, JSON-array flattening) that must be either
  unified or documented as explicit, tested differences (#36, #39).
- Exporting protocols widens the public API surface and the compatibility
  commitment.
- Not splitting the package leaves the two tiers sharing `StreamMessage`/
  `AppendOptions`; the boundary is a **convention** enforced by docs and layering
  discipline, not by the packaging.

**Neutral**

- No behavior change ships with this ADR itself; it records the direction. The
  tactical work is tracked in #36–#40 under epic #41, each landing independently
  behind its own tests.
