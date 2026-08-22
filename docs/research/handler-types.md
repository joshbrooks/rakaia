# What handler types does a durable-stream event-sourcing library actually need?

**Status:** research notes, 2026-08-15. Not a decision — no ADR is implied by this
file, and nothing here changes source.

**Where this lives and why.** `docs/` holds prose docs that are listed in
`zensical.toml`'s explicit `nav`; `docs/adr/` holds decisions; `okf/` holds the
machine-readable bundle. None of those is right for a research note: this is not
a decision (so not `adr/`), not a user-facing manual page (so not a `nav` entry),
and not part of the concept bundle (so not `okf/`). A new `docs/research/`
directory keeps it inside the docs tree — searchable, versioned with the code it
argues about — without claiming any of those three roles. It is deliberately
**not added to `zensical.toml`'s nav**; see the "Docs build" section at the end
for what that costs.

---

## The question, and why now

rakaia names three extension points:

| Concept | Shape | Registration | Selection |
|---|---|---|---|
| **handler** | `event -> Effect` (stage 0) / `event, reader -> Effect` (stage > 0) | `register_handler(name, event_match, effective_from, effective_to, match_field=, stage=)` | seq-bracketed version series; glob on stream path *or* on `event[match_field]` (`src/rakaia/registry.py:268`) |
| **reducer** | `reader -> Effect` / `reader, touched -> Effect` | `register_reducer(name, stage)` | one current definition per name, last-write-wins, **not** seq-versioned (`src/rakaia/registry.py:161`) |
| **upcaster** | `event -> event` | `register_upcaster(event_match, from_version, match_field=)` | chained by `from_version` (`src/rakaia/registry.py:595`) |

Its first real production consumer, `partisipa-import`, has grown five app-side
shapes that rakaia does not name: edge/relation binders, thin attribute folds,
correction folds, derivation folds, and producers. The question is whether any of
those is a genuinely distinct **handler type** that deserves first-class support,
or whether each is a composition of what already exists.

The question is live now because the consumer is at the point of flipping the
durable log to authoritative, and because ADR 0002 already committed rakaia to
bringing its *framework-tier extension seams* up to the protocol tier's standard.
If a sixth registration shape is coming, this is the moment to know.

---

## The headline finding, up front

**Four of the five candidates are the same thing: a content-routed stage-1
handler.** Not "similar to" — literally the same three-line registration, varying
only in the value of `event_match`:

```python
# relations.py            location_correction.py      project_status.py           status.py
reg.register(             reg.register(               reg.register(               reg.register(
  name="project_edge_…",    name="location_correc…",    name="project_status_…",    name="status_binding",
  event_match="project",    event_match="location_…",   event_match="project_st…",  event_match="status_ch…",
  fn=…, effective_from=0,   fn=…, effective_from=0,     fn=…, effective_from=0,     fn=…, effective_from=0,
  match_field="relation",   match_field="relation",     match_field="relation",     match_field="relation",
  stage=1,                  stage=1,                    stage=1,                    stage=1,
)                         )                           )                           )
```

(`backend/ida_forms/streams/{relations,location_correction,project_status,status}.py`,
each in its own `build_registry()`.)

What differs between them is entirely inside the function body — which projection
they read, which column they write, and what rule decides. That is application
logic, which is what a handler body is *for*. The variety the consumer perceives
is domain variety, not mechanism variety.

The one candidate that is **not** a handler at all is the producer, and it is the
only one where rakaia genuinely has a hole. That hole is narrower than "rakaia
needs a producer concept", and is discussed under Question 2.

---

## The five candidates

| Candidate | What it does | Distinct? | Covered by | Verdict |
|---|---|---|---|---|
| **1. Edge / relation binder** (`relations.py`) | Binds a link between two existing subjects; `LINK`/`UNLINK` lifecycle; must not materialise a node | **No** | Content-routed stage-1 handler + `Effect(op="update")` / `op="delete"`. The "must not materialise a node" rule (their ADR-0005) is *already* rakaia's `op="update"` semantics — "the update-if-exists primitive a secondary owner of a multi-owned projection row uses instead of a hand-rolled exists-guard" (`src/rakaia/effects.py:79`) | **Build nothing.** Their ADR-0008 declined to upstream; still correct — see Q3. Worth telling them `op="update"` replaces their hand-rolled `reader.get(...) is None` guard |
| **2. Thin attribute fold** (`status.py`) | A `submissions/status` stream carrying identity + form_type + status only, folded over the content stream | **No** | Content-routed stage-1 handler emitting `op="update"`. There is no rakaia-side concept in the *fold* at all — the event is thin because the **producer** made it thin | **Build nothing on the read side.** The real content of this candidate is a write-side decision (see below), and rakaia already ships the primitive for it: `append_if_changed` (`src/rakaia/append.py`) |
| **3. Correction fold** (`location_correction.py`, their ADR-0016) | A later stream whose events override an earlier stream's projection; precedence by fold order | **No** as a handler; the *precedence mechanism* is the real question | Handler: identical stage-1 shape. Precedence: today an unwritten convention (call order of `replay()` in `rebuild_tf611.py:170-193`). rakaia has two mechanisms that could carry it — `stage=` and `merge_replay(order_key=)` | **Build nothing; name something.** See Q1 |
| **4. Derivation fold** (`project_status.py`) | Recomputes a value from another aggregate; a user-authored terminal status beats a derived one regardless of arrival order | **No** | Content-routed stage-1 handler. "Discriminated authority" is four lines of `if` in the handler body (`project_status.py`, `_TERMINAL` check before deriving) | **Build nothing.** A library concept for "not last-write-wins" would be a DSL for one `if` statement |
| **5. Producer** (`produce.py`, `dual_write.py`, `_envelope.py`) | Decides *what to emit*: classify the save, choose the stream, build the envelope, append, then fold | **Yes — genuinely absent** | Partially: `AppendOptions` + `provenance()` (envelope), `append_if_changed` (no-op suppression), `rakaia.producer` (fencing only — epoch/seq, *not* "what to emit") | **Build a little, and name a rule.** Not a `Producer` abstraction — see Q2 |

### Note on the naming collision inside rakaia

`src/rakaia/producer.py` exists and is about **producer fencing**: `(Producer-Id,
Producer-Epoch, Producer-Seq)` → accept / duplicate / stale epoch / gap. It is a
protocol-tier concept (ADR 0002 Tier 2), not a framework-tier one, and it says
nothing about *what* to emit. So "rakaia has no producer concept" is not quite
right — rakaia has the word, spent on something else. Any framework-tier
write-side concept must not be called `Producer` without resolving that.

---

## Question 1 — Is "fold order between stream families" first-class, or emergent?

### What is actually there today

Emergent, and *doubly* so. In `rebuild_tf611.py` the whole precedence rule is four
consecutive statements:

```python
result = replay(
    store,
    TF611_STREAM,
    executor,
    handler_registry=build_tf611_authoritative_registry(...),
    reader=reader,
)
if store.has(LOCATION_STREAM):
    replay(
        store,
        LOCATION_STREAM,
        executor,
        handler_registry=location_registry(),
        reader=reader,
    )
status_result = replay(
    store,
    PROJECT_STATUS_STREAM,
    executor,
    handler_registry=project_status_registry(),
    reader=reader,
)
if store.has(STATUS_STREAM):
    replay(
        store,
        STATUS_STREAM,
        executor,
        handler_registry=status_registry(),
        reader=reader,
    )
```

Two separate things are emergent here, and they should not be conflated:

1. **Order between families** is the caller's statement order. Nothing declares it,
   nothing checks it, and a reordering is a silent correctness change.
2. **The registries are disjoint.** Each family builds its *own fresh*
   `HandlerRegistry()`. So `stage=1` in `location_registry()` and `stage=1` in
   `status_registry()` are unrelated numbers in unrelated registries. Even if
   someone wanted to express precedence as a stage, today's structure cannot: a
   stage only orders passes *within one `replay()` call*.

`merge_replay` is used **nowhere** in the consumer (grep: zero hits across the
worktree). So the multi-stream merge primitive rakaia built is, on this evidence,
not the thing the consumer reached for; four sequential single-stream replays is.

### Is correction-precedence expressible in `merge_replay`?

**Not by time, no — and this is the decisive point.** `merge_replay` orders by an
order key: a payload field, or `ENVELOPE_TS` (the envelope's `event_ts`,
`src/rakaia/replay.py:67`). Their ADR-0016 requires that a `location_corrected`
event beat the base coordinate **and any later re-import's coordinate**. A later
re-import has a later `event_ts`. So under `merge_replay(order_key=ENVELOPE_TS)`,
the re-import wins and the correction is lost — precisely the failure the whole
ADR exists to prevent, and precisely the alternative ADR-0016 already rejected
("Same event type, last-write-wins vs the base coord … A later re-import would
revert the fix").

Correction precedence is therefore **not a temporal fact**. It is a *typed
authority* fact: this class of event outranks that class, forever, independent of
when either happened. Ordering by `event_ts` vs append order is the wrong axis to
argue about — both are time, and time is not what decides this.

`stage=` **is** the right axis: it is rakaia's existing name for "this pass runs
after that one, regardless of event order". Expressing correction-precedence as
`stage` would need one composed registry over merged streams, i.e.
`merge_replay(paths, order_key=ENVELOPE_TS)` with content-routed handlers at
stages 0/1/2 — base content at the lower stage, corrections at the higher.

That is available today with no new API. It has one real cost, documented in
`merge_replay`'s own docstring (`src/rakaia/replay.py:506`): merged `seq` is the
position in the *merged* order, so any handler with a **closed** `effective_to`
sized to one stream raises `HandlerGapError`. The consumer's folds all use
`effective_from=0, effective_to=None`, so they would survive — but this is a
constraint a migration must state, not discover.

### Recommendation

**Name it; do not build it.**

1. Add **fold order** to `docs/glossary.md` as a named concept, defined as: *when
   several stream families project into one read model, the order in which they
   are folded is part of the projection's definition, not an implementation
   detail of the rebuild command.*
2. Document the two ways rakaia already expresses it, and when each applies:
   - **`stage=` within one replay (or one `merge_replay`)** — for precedence that
     is *typed*: correction-beats-base, derived-beats-nothing. Declared in the
     registration, visible to `stages()`, checkable.
   - **`merge_replay(order_key=…)`** — for precedence that is *temporal*: several
     streams that genuinely interleave and where "later wins" is the rule.
   State plainly that correction-precedence is the first kind and **must not** be
   modelled as the second.
3. Do **not** add a cross-replay ordering API. A list of `(stream, registry)` run
   in order is a `for` loop; wrapping it buys a name and nothing else, and the
   composed-registry route gives the same name for free with real checking behind
   it.

**Prior art supports this.** Axon draws the line in exactly the same place: within
one processor, "the order in which components are registered … is guiding" and can
be made explicit with `@Order`; but "it is **not possible** to order event handlers
belonging to different Event Processors" — and the docs warn that ordering at all
"means those components are inclined to interact with one another, introducing a
form of coupling". rakaia's `stage` is the intra-replay `@Order`; the four
sequential `replay()` calls are four processors with no ordering contract, which
is the arrangement Axon says has none to offer.

---

## Question 2 — Does a producer belong in the library at all?

### The asymmetry argument, examined

The claim is: rakaia has the read-side hermeticity rule (ADR 0003 →
`django_rakaia.hermeticity.deny_database_access`) but no write-side twin, and the
consumer had to invent one (their ADR-0021, "a producer must not consult the
projection to decide whether to emit — ask the log").

The asymmetry is real but it does **not** argue for a producer *abstraction*. Look
at what the read-side twin actually is: `deny_database_access` is not an
abstraction over handlers — it is an **enforcement seam**. It works because the
rule it enforces is mechanically decidable: "no statement may reach the `default`
alias during handler dispatch". A Django `execute_wrapper` can see every such
statement.

ADR-0021's rule is **not** mechanically decidable, and their own ADR says so:

> "Reading the projection for the *payload* of an event remains fine and
> unavoidable (the producer must read the rows it is describing). The rule is
> about **control flow**: what decides whether an event exists at all."

A query wrapper cannot distinguish a read that shaped a payload from a read that
decided an emission. So the read-side pattern — *ship the guard, not the
abstraction* — does not transfer. There is no guard to ship.

### Be skeptical: ADR-0021 is ahead of its code

The lead flagged this and it checks out.

- `ranked_priority_ids` and `RANK_STREAM` **do not exist** on this branch. Grep
  finds them only inside ADR-0021's own text; `streams/sf23.py` has neither, and
  its docstring still says `PriorityOrder.order` "is maintained by a pgtrigger
  group-rank … not by the handler, so it is a derived column excluded from
  projection parity". The ADR names its own status honestly ("validated on
  `spike/priority-fractional-rank` @ `4e7b7983`"), but the flagship
  implementation is on a different branch.
- The live edge producer **does** consult the projection to decide whether to
  emit. `produce.bind_project_edge` reads `SeparatedSubmissionProject.objects
  .filter(...).exists()` and the typed row's resolved `project` FK, and skips the
  append on that basis. It is a *hybrid* — `_latest_project_edge` asks the log
  first, and the SSP query is described in the code as "a fast negative" — but the
  ORM read is still in the emit/skip control flow, which is what ADR-0021
  forbids. Its own comment concedes the shape: "the SSP row is a fast negative
  (already linked, incl. by a prior produce of this same resolver)".
- `location_correction.py`'s producer side does not exist at all: "The producer
  side (append on the admin/GIS edit) … [is] the follow-on (Phase 3)".

So: **one recorded decision, zero conforming producers on this branch, and one
producer that violates it.** That is not evidence of a validated pattern. It is
evidence of a rule someone learned the hard way once, in a spike, and wrote down.
Upstreaming a producer abstraction on that basis would be abstracting from a
single unshipped data point.

### What *is* worth taking

Two concrete things, both small, both already asking to be upstreamed by name:

1. **The append envelope + scratch-fold ritual.** `streams/_envelope.py` is 79
   lines wrapping `append_event(store, path, payload, label=, actor=, event_ts=)`
   and `fold_events(events, registry, reader=)`. Its docstring is explicit that
   its location is a dependency-management accident, not a design choice:

   > "These helpers live app-side deliberately: `rakaia`/`django_rakaia` are an
   > external dependency, so upstreaming is a later version bump, not a blocker
   > (ADR-0020, alternatives)."

   And its stated motivation is exactly the drift a library exists to prevent:
   "~37 appends across 18 files, 11 scratch rituals" hand-rolled, with the warning
   "a second write path which re-implements the envelope is a path no gate
   covers." That is a library-shaped problem. `fold_events` in particular — seed a
   scratch `StreamStore`, replay one subject's events through a registry with a
   live reader — is *the* live-projection primitive, and it is currently
   reimplemented per adopter.

2. **`append_if_changed` needs to be findable.** rakaia already ships no-op
   suppression (`src/rakaia/append.py`), the glossary already names it, and the
   consumer's candidate #2 — the thin-attribute-fold, motivated by "62% of
   re-saves re-carry the blob for nothing" — is a bespoke reimplementation of the
   same idea (`fields_fingerprint` + a `_PRIOR_STATE` pre-save stash + a
   three-way save classification in `dual_write.py`). Grep confirms the consumer
   never imports `append_if_changed`. Their version does more (it *routes* to a
   different stream rather than just suppressing), so this is not "they missed
   it" — but a library whose flagship consumer independently rebuilds its
   write-side change-detection has a discoverability problem, and possibly a
   generality one: `append_if_changed` returns `bool` (append or not), where the
   consumer needed a three-way classification (content / status-only / no change).

### Recommendation

- **Yes, narrowly — and it is already done.** ~~Upstream~~ the envelope +
  scratch-fold helpers (`append_event` / `fold_events`) shipped in
  `src/django_rakaia/envelope.py` in PR #94 (2026-08-14), pinned byte-for-byte
  against the longhand they replace. **Correction to this section's premise:** the
  consumer still carries `streams/_envelope.py` only because it is pinned to PyPI
  `rakaia-streams 0.1.0`, which predates the upstream. Deleting their copy needs a
  release, not a design decision. `SCRATCH_PATH` was also renamed
  `"produce/submission"` → `"_scratch/fold"` (#100) precisely because the
  consumer's domain vocabulary had no business being the library default.
- **Write the ADR-0021 rule into rakaia's docs** as a stated contract next to
  ADR 0003, on the strength of the reasoning (which is sound) rather than the
  evidence (which is one branch-local spike). Say plainly it is not enforceable.
- **No `Producer` type, no producer registry, no emit-decision framework.** The
  read/write asymmetry is real, but it argues for a *documented rule*, not a
  mechanism: the read side got a mechanism only because a mechanism was possible
  there.
- **Do not name any of it `Producer`** — `rakaia.producer` is taken by fencing.

---

## Question 3 — Is an edge/relation binder just a stage-1 handler?

**Yes, and their ADR-0008 is still right.** Three reasons, in increasing order of
weight:

1. **Still one adopter.** ADR-0008's own extraction trigger — "if a second adopter
   needs edges, *then* consider extracting a rakaia helper … with two real call
   sites to design against" — has not fired. Nothing has changed on that axis.
2. **The pattern got *smaller*, not larger, since the ADR.** ADR-0008 was written
   when edges looked like a subsystem: "the edge model (`relations.py`, the
   binder, the commands)". What is actually load-bearing today is one handler
   registration and a ~40-line function body. There is less to extract now than
   when they declined to extract it.
3. **The one genuinely general bit is already in rakaia, and they are not using
   it.** ADR-0005 ("an edge annotates an existing node — never materialises one")
   is implemented as a hand-rolled guard:

   ```python
   if reader.get(label, submission_id=source) is None:
       return []
   ```

   followed by `Effect(op="update_or_create", …)`. That is exactly what
   `Effect(op="update", …)` does natively — rows matching `lookup` are "updated in
   place and **never** inserted" (`src/rakaia/effects.py:79`), and rakaia's own
   docstring pitches it as the alternative to "a hand-rolled exists-guard". Their
   other three folds already use `op="update"`; `relations.py` is the one that
   didn't, because it also needs the existence answer to decide about the
   `SeparatedSubmissionProject` row.

**Recommendation:** decline again, and close the loop by telling them about
`op="update"`. If a second adopter ever appears, the thing to extract is not "an
edge primitive" — it is a documented *recipe* in `docs/projection-cookbook.md`:
edge event shape (`{relation, source, target, action}`), content-routing on
`relation`, stage 1 for the reader, `op="update"` so a missing node is a no-op,
`op="delete"` for `UNLINK`, and last-write-wins by append order. Recipes cost
nothing to be wrong about.

Note also that `relations.py`'s own docstring still says **"Spike status
(read-only). … Nothing durable / on the live path here"**, even though
`produce.bind_project_edge` now appends to `RELATIONS_STREAM` and folds through
`relations.build_registry()` on the live save path. The docstring is stale. That is
a small thing, but it is the same class of problem as ADR-0021: the written record
and the code disagree, in both directions, and neither can be trusted alone.

---

## Prior art — what other frameworks call these things

Sources are official docs only; each row is what that project's own documentation
says, not a community summary.

| | per-event projector | cross-aggregate / multi-stream projector | schema-upgrade step | write-side component |
|---|---|---|---|---|
| **rakaia** | **handler** (`event -> Effect`) | **reducer** (per-stage, `reader -> Effect`); **`merge_replay`** (k-way merge by order key) | **upcaster** (`from_version` chain) | — (framework tier); `rakaia.producer` = protocol-tier **fencing** only |
| **EventStoreDB / KurrentDB** | **projection** — but it means something else entirely (below) | `fromAll()` / `fromStreams([])` + `partitionBy` inside a user-defined projection | — (no equivalent; events are immutable JSON, versioning is the client's problem) | `emit()` / `linkTo()` **inside** a projection |
| **Axon Framework** | **event handler** ("a method that is capable of handling an `EventMessage`"), grouped into an **event handling component**, assigned to an **event processor** (subscribing / streaming / pooled streaming) | **saga** (long-running, cross-aggregate process); handler ordering via `@Order` **within** one processor only | **upcaster** — `EventUpcaster`, `SingleEventUpcaster`, `EventUpcasterChain`; "transforms events from their original stored structure to a new structure" | command handler + aggregate |
| **Marten** | **single-stream projection** / **aggregation** (`Apply`, `Create`); **event projection** (`Project`, `Create`) for ad-hoc document ops | **multi-stream projection** — "a view is aggregated over events between streams", with `Identity<T>` / `Identities<T>` / a custom `IAggregateGrouper` / a custom `IEventSlicer` | **upcasting** — "transforming the old JSON schema into the new one … performed on the fly each time the event is read"; `EventUpcaster<T>`, `MapEventType()` | append (`Quick` / `Rich` append modes) |
| **Eventide** | **entity projection** — "applies one event to one entity"; `apply` blocks per event type | — (no named multi-stream projector; consumers/handlers compose) | — | **handler**, run by a **consumer** in a **component** |
| **Akka** | **Projection** — "you process a stream of events or records from a source to a projected model or external system"; **Handler**; **SourceProvider**; **offset store** | (same Projection machinery over a tagged/sliced source) | **event adapter** — `WriteEventAdapter` / `ReadEventAdapter`, for schema evolution between journal and domain representation | persist / event handler in an `EventSourcedBehavior` |

### Where rakaia's names agree, and where they clash

**Agree, safely:**

- **upcaster** is the near-universal term for the schema-upgrade step (Axon,
  Marten). Akka calls it an *event adapter*; the concept is the same. rakaia's
  `from_version` chain matches Axon's `EventUpcasterChain` closely. **No change
  needed and none available — this is the settled word.**
- **handler** as "the per-event function" matches Axon and Eventide directly.
- **projection** as "a derived read model" matches Marten, Akka, and Eventide.

**Clash, and worth knowing about:**

- **"projection" means the opposite thing in EventStoreDB/KurrentDB.** There, a
  projection is a *server-side, event-producing* subsystem: it "let[s] you append
  new events or link existing events to streams", via `emit()` and `linkTo()`, and
  its documented cost is "write amplification because emitting new events or link
  events creates additional load on the server IO". Read models there are built by
  *subscriptions*, not projections. Anyone arriving at rakaia from EventStoreDB
  will read "projection" backwards. This is not a reason to rename — Marten, Akka
  and Eventide are all on rakaia's side — but the glossary entry could say so in
  one clause.
- **"reducer" is rakaia's own coinage.** Nobody else uses it for this. The closest
  named equivalents are Marten's **multi-stream projection** (a view aggregated
  across streams) and Akka's **grouped handler**. rakaia's reducer is genuinely a
  bit different from both — it runs once per stage over *committed projections
  via the reader*, not over a slice of events — so a rename would trade a unique
  word for an inexact one. **Keep it**, but note in the glossary that Marten users
  will look for "multi-stream projection" and Axon users for "saga".
- **rakaia has no word for the write side, and the obvious word is taken.** Every
  other framework names this: Axon *command handler*, Eventide *handler +
  consumer*, Marten *append modes*, EventStoreDB *emit*. rakaia's `producer` means
  fencing. If a framework-tier write-side concept ever lands, the vocabulary
  question has to be settled first.
- **Nobody names "fold order between families".** Axon comes closest and its
  answer is a *prohibition* ("not possible to order event handlers belonging to
  different Event Processors") plus a warning that ordering is coupling. This is
  weak support for making it a first-class rakaia concept — and good support for
  the Q1 recommendation of expressing it as a stage inside one replay, which is
  the arrangement Axon *does* support.

---

## What NOT to build

Each of these was considered and rejected. The reason matters more than the
verdict, because a future adopter will propose them again.

1. **An edge / graph primitive** (`register_edge`, an edge event type, an edge
   reducer). One adopter; the pattern is one `register(..., match_field="relation",
   stage=1)` call; the only generalisable part (`op="update"` as "annotate, never
   materialise") already exists and the adopter isn't using it. Their ADR-0008 got
   this right in 2026-07 and nothing has changed since.

2. **A correction-fold primitive** (`register_correction`, `overrides=`). A
   correction fold is a stage-1 handler. What is special about corrections is the
   *precedence*, and precedence is already spelled `stage=`. A dedicated
   registration kwarg would be a second, weaker spelling of an existing concept.

3. **A "discriminated authority" mechanism** (declarative precedence, a
   non-last-write-wins registration flag). The consumer's implementation is:

   ```python
   if current is not None and current.value in _TERMINAL:
       return []
   ```

   Four lines in a handler body, reading a projection through the reader. A
   library concept for this would be a configuration language for `if`. The rule
   is *domain* knowledge (which status values are user-authored terminals), and
   domain knowledge in a general library is exactly what ADR-0008 warns against.

4. **A "thin event" / attribute-fold concept.** There is nothing thin about the
   *fold* — it is a normal handler over a normal event. Thinness is a property of
   what the producer chose to emit. Building a read-side concept for it would name
   the wrong end of the pipe.

5. **A `Producer` class / producer registry / emit-decision framework.** See Q2:
   the rule that motivates it (ADR-0021) is not mechanically enforceable by its
   authors' own account, has zero conforming implementations on the branch under
   review, and is contradicted by the one live producer that faces the same
   decision. Abstracting now would canonise an unvalidated pattern — and the name
   is already spent on fencing.

6. **A cross-replay fold-ordering API** (`replay_families([...])`,
   `run_in_order([...])`). It is a `for` loop over `(stream, registry)` pairs. The
   version with real value — one composed registry where family precedence *is*
   the stage — needs no new API at all, only `merge_replay` and a doc page.

7. **A rakaia-side "one fact, one derivation" conflict check.** Tempting: their
   ADR-0018 found the same column computed by a stage-1 fold and a stage-2 reducer
   under *different rules*, live, in two different gates. A registry that knew both
   writers targeted `Tf_6_1_1.project_status` could in principle warn. Rejected for
   now — a handler's target column is only knowable from its returned `Effect`s,
   i.e. at replay time, not registration time, so the check would be a runtime
   warning on a legitimate pattern (multi-owner projections are explicitly
   supported; `op="update"` exists to serve them). Listed below as an open
   question rather than a rejection, because the failure it would have caught was
   real and expensive.

---

## Open questions / what would change the answer

- **A second adopter of edges.** This is ADR-0008's own stated trigger and the
  single cleanest thing that would flip candidate #1. Two real call sites would
  make the shape designable; one still makes it guesswork.

- **Does the composed-registry route actually work at their scale?** The Q1
  recommendation (one registry, families as stages, `merge_replay(ENVELOPE_TS)`)
  is sound on paper and **untested by anyone** — `merge_replay` has zero call
  sites in the consumer. A prototype rebuild of TF611 + corrections + status +
  derivations as one merged, staged replay is the experiment. If it fails on
  `HandlerGapError`, memory, or the `seq`-semantics change, the answer to Q1
  becomes "the sequential-replay convention is the design, so document *that*
  instead".

- **Do multi-owner projections need a declared owner-per-column?** ADR-0018's
  defect (two live derivations of one fact, disagreeing at the edges, each wired
  into a different gate) is the most expensive bug in the consumer's whole ADR
  record, and it is a *composition* failure, not a handler-type failure. If a
  second consumer hits the same class, "who owns this column" may deserve to be
  declarable. That would be a new kind of registration metadata, not a new handler
  type.

- **Is `append_if_changed` general enough?** The consumer needed a three-way
  classification (content-changed / status-only / no-change) and got a `bool`.
  Whether the primitive should return a verdict rather than a boolean is worth
  asking before a second consumer rebuilds it a second time.

- **Does ADR-0021 survive contact with its own codebase?** The rule has one
  spike-branch implementation and one live counter-example. When
  `spike/priority-fractional-rank` lands (or doesn't), and when
  `bind_project_edge` is either reconciled with the rule or explicitly excepted
  from it, there will be real evidence. Revisit Q2 then, not before. A recorded
  decision is not evidence of a working pattern.

---

## Docs build

`uv run zensical build` with this file present: **`No issues found`, build clean.**
An un-navigated page under `docs/` neither breaks nor warns — zensical uses an
explicit `nav`, so a file absent from it is simply not published. This file is
**not** added to
`zensical.toml`'s `nav`, deliberately — research notes are not part of the
published manual, and adding one would set a precedent for the nav that a future
`docs/research/` directory of ten files would not survive.
