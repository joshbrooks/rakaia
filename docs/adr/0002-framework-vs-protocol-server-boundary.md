# ADR 0002 — Name the framework ↔ protocol-server boundary; formalize the framework seams

- **Status:** Accepted (items 2 and 5 landed; boundary enforced 2026-08-21; the
  package split declined and #191 closed 2026-08-26 — see the update notes below)
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

## Update — 2026-08-11

Items 2 and 5 have landed, and one premise of this ADR has changed.

The ADR's context says "the store is really two interfaces, and only the read
half is a protocol", and its alternatives defer a package split until "the
durable store grows a real protocol-server implementation". Both are now
addressed: `StreamServerStore` names the protocol-server half, and
`DjangoStreamStore` implements it, held to `tests/server_store_contract.py`
alongside the in-memory store.

Note the ADR's trigger condition was already met when it was written —
`django_rakaia.protocol_views` was a second, partial implementation of the
protocol, recorded here only as "reportedly two HTTP protocol surfaces". With
one implementation now able to run on either store, that duplicate is being
removed rather than grown.

## Update — 2026-08-21: the boundary is now enforced, and the split stays deferred

The trigger this ADR set has fired, so #191 asked the question again. It was
answered by measuring the coupling rather than arguing about it, and the answer is
**keep the package together, but stop calling the boundary a convention.**

What the measurement found, across both packages' internal import graphs:

- **One wrong-direction dependency, and it was doing real damage.**
  `rakaia.registry` — framework tier — imported the in-memory `StreamStore` to
  annotate `HandlerRegistry(store=...)` and `UpcasterRegistry(store=...)`. The
  import was `TYPE_CHECKING`-only, so it created no runtime coupling and nothing
  in the suite could see it, but the *annotation* named one protocol-server
  implementation where the parameter accepts any `WritableStore`. Consequence:
  `HandlerRegistry(store=DjangoStreamStore())` was a pyright error — and a
  durable meta-stream is precisely the case that parameter exists for, since the
  docstring's promise is registrations surviving a *process* restart. Both
  signatures now name `WritableStore`. This is item 2 of the decision below,
  arriving late at the one seam that had been missed.
- **Nothing else crosses.** No other framework module imports the protocol-server
  tier. The single remaining crossing, `seed` → `store`, is a default value
  (`seed_stream` promises "omit the store and get a fresh in-memory one") and its
  parameter is already typed as the protocol.
- **`django_rakaia` splits along the same line.** Eight modules depend only on
  framework surfaces, `subscription` only on protocol-server ones, and
  `django_store` is the one straddler — which is the tier boundary itself, and
  crosses in the permitted direction.
- **The shared vocabulary is four modules** — `types`, `protocols`, `json_mode`,
  `offsets` — and none of them depends on either tier. That is the whole of what
  a split would have to move, duplicate, or extract into a third package.

So the tiers *are* separable, and the reason to defer is unchanged from the
original alternatives table: a split buys clarity the enforcement now buys more
cheaply, and costs a cross-package seam plus a second release cadence. The
evidence #191 asked for is the four-module shared surface: while it stays that
small and tier-independent, splitting remains a mechanical option that can be
taken whenever there is a reason beyond tidiness.

The consequence recorded below — "the boundary is a **convention** enforced by
docs and layering discipline, not by the packaging" — no longer holds.
`tests/test_rakaia/test_tier_boundary.py` asserts it: the tier map, the
one-way rule, the crossing allowlist, and the tier-independence of the shared
vocabulary. It reads type-only imports too, since that is the form the defect
above took. A module added to neither tier fails the test rather than escaping
it, so the next person cannot defer the classification by omission.

This does not reopen or close #191; it replaces the guesswork in it with a
measurement, and makes the cost of *not* splitting visible as a list of
crossings that is currently one entry long.

## Update — 2026-08-26: the split question is answered, and #191 closes

The previous update deliberately left #191 open. It is closed now, not because
the question got less interesting but because it has been measured twice with
the same answer and there is nothing left to learn by asking a third time. **The
tiers stay in one distribution.** What changed is that the last argument *for*
splitting has been removed without splitting.

**The boundary held through the largest change since it was written.** #229 added
`JsonlStreamStore` and `migrate` — 1,405 lines of protocol-server code, a third
store, and a tool that reads one store and writes another. The crossing list is
still one entry. That is not luck: `test_the_tier_map_covers_every_module` fails
on an unclassified module, so both new modules had to be placed in a tier as they
landed, and the one-way rule was checked against them from their first commit.

**Separability is now demonstrated rather than inferred.** Deleting the entire
protocol-server tier from a copy of the tree and importing each framework module
in turn: **11 of 12 import cleanly.** The single failure is `seed`, which is
exactly the documented `ALLOWED_CROSSINGS` entry. The claim "a split would be
mechanical" is no longer an argument from the import graph; it is a thing that
was run.

**The largest consumer would not benefit.** `django_rakaia`'s imports of `rakaia`
are **16 framework, 3 protocol-server, 14 shared** — it is overwhelmingly a
framework consumer that happens to serve the protocol. A framework/server split
would leave it depending on all three resulting packages, so the biggest
downstream gains no isolation and pays a three-way version matrix for it.

**The one measurable cost of not splitting is gone.** The honest argument for a
split was never install weight — the core has no runtime dependencies, so a
framework consumer already installs nothing extra. It was that `import rakaia`
eagerly loaded all ten protocol-server modules and *constructed an in-memory
store*, because `app = create_app()` ran at module scope. Measured at 80ms and
one unwanted store per process. Resolving the root's exports lazily (PEP 562, as
`django_rakaia` already did) drops a framework consumer to **3ms and zero
protocol-server modules**. A consumer now pays for the tier it uses, which is the
benefit a split was being considered for.

`replay` is the one export that cannot be lazy, and it is worth recording as a
property of *this* seam rather than an implementation note: it is both a public
name and a submodule, so importing the submodule binds the module over the
function and `__getattr__` is never consulted. Left lazy, `from rakaia import
replay` would return a function or a module depending on what else the process
had imported — order-dependent, and worse than the shadowing described in #161
item 1. It stays bound eagerly.

### What would reopen this

The trigger is no longer "the durable store grows into a protocol server"; that
fired and was answered. The new ones, each a thing that would make a split buy
something it does not buy today:

- **The crossing list grows past one**, or an entry appears that is not a default
  value. That is the tiers actually fusing, and the allowlist is where it shows.
- **The shared vocabulary stops being tier-independent.** Four modules — `types`,
  `protocols`, `json_mode`, `offsets` — currently depend on neither tier. A
  shared module that needs one of them is the point where a third package stops
  being optional.
- **A consumer wants one tier without the other badly enough to say so.** Not
  hypothetically: an actual report that installing the framework brings something
  unwanted. Import cost no longer counts, since it has been paid down.
- **The two tiers need different release cadences.** A protocol-conformance fix
  that cannot ship because the framework half is mid-change is the cost a shared
  distribution actually imposes, and nothing so far has hit it.

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
| Full package split (`rakaia-streams` + `rakaia-projections`) | High | Highest | Yes | **Declined 2026-08-26** (#191) — separability demonstrated, but a split buys nothing left to buy; see the update above for the new triggers |
| Boundary as docs only (no protocol formalization) | Low | Medium | No | Insufficient — the store/executor/reader seams need real, exported, conformance-tested protocols, not prose |
| Status quo (leave fused, undocumented) | None | None | No | Rejected — the fusion traps keep reaching consumers |

- **Full split** was recorded here as the "correct" end-state, deferred as
  premature. Revisited twice (2026-08-21, 2026-08-26) and now declined: the tiers
  are separable — demonstrated by deleting one and importing the other — but the
  reasons to split have been answered without splitting, and the largest
  consumer, `django_rakaia`, depends mostly on the framework and so would gain no
  isolation from it. "Correct end-state" overstated it; a shared distribution
  with an asserted boundary is a fine end-state.
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
