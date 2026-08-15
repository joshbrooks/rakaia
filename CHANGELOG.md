# Changelog

All notable changes to Rakaia are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/).

New here? The [guided tour](docs/whats-new.md) walks these capabilities with a
runnable demo for each.

## [Unreleased]

Everything below has landed on `main` since the initial `0.1.0` groundwork and
is not yet tagged in a release.

### Added

- **A declared public API, and a contract for it.**
  [`docs/public-api.md`](docs/public-api.md) sets out three tiers: **Tier 1**
  (`rakaia.__all__` + the new `django_rakaia.__all__`) which does not change
  without a major bump and an `UPGRADING.md` entry; **Tier 2**, the ORM models
  and schema — usable, deliberately *not* exported, and free to change in a
  minor release; and **Tier 3**, everything else. It also says plainly to depend
  on rakaia with an **upper bound** (`>=0.2,<0.3`), because on a pre-1.0 library
  an unbounded `>=` admits every future breaking change and takes it silently on
  the next lockfile refresh.

  `django_rakaia` previously exported **nothing** — 44 lines of docstring naming
  a "Public API" that was not importable — so every consumer import had to name
  an internal module, pinning the module *layout* rather than the surface. It now
  exports 34 names, resolved lazily (PEP 562) so importing the package still does
  not pull in the ORM, which is what made eager exports impossible and is now
  pinned by a test. Both surfaces are pinned name-by-name, so changing one is a
  deliberate act rather than a diff.

  The examples and docs were the main offender and are converted: **72
  submodule imports** now use the package root.


- **`StreamServerStore` — the protocol-server store surface, named.** `create_app`
  was typed against the concrete in-memory `StreamStore`, so nothing else could
  back the protocol server. The new protocol (exported from `rakaia`) covers
  the full surface the server actually calls — the protocol lifecycle methods
  it declares itself plus the framework read/write methods inherited from
  `WritableStore` — and `create_app` is typed against it.

- **`DjangoStreamStore` now backs a protocol server.** It implements the whole
  `StreamServerStore` surface — producer epoch/seq fencing, stream close, the TTL
  sliding window, long-poll and response formatting — so `rakaia.create_app` can
  serve the Durable Streams protocol directly off the database. Both stores are
  held to one shared conformance suite (`tests/server_store_contract.py`), and
  the fencing rules live in a single pure module (`rakaia.producer`) that both
  call, so the two cannot drift apart.

  It also holds the payloads the protocol allows but a JSON column does not.
  A stream may declare **any** content type; `text/plain`, `text/csv` and
  binary bodies are stored verbatim (base64 when not valid UTF-8) and read back
  byte for byte, recorded by a new `StreamEvent.payload_encoding` column.
  Streams with no declared content type keep storing parsed JSON exactly as
  before, which is what `replay()`, the admin and the channel-layer signals
  read. In JSON mode a top-level array is flattened one level on append, as the
  protocol specifies and the in-memory store already did — `[a, b]` is two
  messages, not one message that is an array.

  This closes what `tests/store_contract.py` previously called "genuine,
  permanent architectural divergences": conflict detection on `create`, stream
  close, Stream-Seq and producer fencing were all documented as concerns the
  durable store would never model. It models them now. The only remaining
  backend-specific behaviour is the offset *format*, which the protocol leaves
  open (§6).

- **Named store failures.** A store now raises one of `StreamNotFound`,
  `StreamConfigConflict`, `SequenceConflict`, `ContentTypeMismatch`,
  `InvalidJson`, `EmptyJsonArray` or `InvalidOffset` (all exported from
  `rakaia`) instead of a bare `ValueError`/`KeyError`, and the ASGI server maps
  them to a status by type via `rakaia.handler.STORE_FAILURE_STATUS` —
  resolved along the MRO, so a backend that specializes a failure inherits its
  status rather than falling through to a 500. Previously the server picked
  the status by matching English in `str(e)`, so rewording a message in
  `store.py` silently turned a 4xx into an unhandled 500 — and any other store
  implementation had to reproduce five exact strings to behave the same. Each
  failure subclasses the builtin it replaced, so existing `except ValueError` /
  `except KeyError` code and tests are unaffected. A new failure type without a
  status now fails the suite rather than 500ing at runtime.

- **`append_event` and `fold_events` — the two rituals every consumer retypes.**
  Appending an enveloped event (JSON-encode with `DjangoJSONEncoder`,
  create-the-stream-if-missing, wrap label/actor/`event_ts` in `AppendOptions`)
  and folding a batch live (seed a scratch in-memory `StreamStore`, replay it
  through a registry with a reader bound) were hand-written at nearly every
  durable call site — ~37 and 11 copies respectively in the first production
  consumer, whose own module carried the warning that motivates this: *"a second
  write path which re-implements the envelope is a path no gate covers."* Both
  now ship from `django_rakaia.envelope`, pinned byte-for-byte against the
  longhand they replace — with one deliberate departure: `append_event` omits
  `metadata["user"]` when no actor is passed, rather than writing `None`.
  `merge_provenance` layers ambient under explicit, so the copies in the wild
  clobber the actor `ProvenanceMiddleware` stamped on the request whenever a
  call site doesn't repeat it, and `envelope_actor` then falls back to the
  payload's owner FK. Upstreamed as its ADR-0020 anticipated.

  The store contract also gained the property that makes the create-if-missing
  shorthand safe: a redundant `create()` on a populated stream preserves its
  messages and does not rewind its offsets. Both stores already satisfied it;
  nothing had said so, and `test_create_is_idempotent` would have passed a
  `create()` that truncated. With it pinned, the `has()`/`create()` dance is
  provably redundant — `registry.py`'s three copies are now a single `create()`.

- **One registration log instead of three.** The meta-stream mechanism —
  create the stream, read back what is recorded, append if new — was written
  three times in `registry.py` (handlers, reducers, upcasters), alongside six
  module-level identity functions in hand-mirrored pairs: one building a tuple
  from the object, one rebuilding the same tuple from stored JSON, with nothing
  checking that they agreed. The tuples were then read back **positionally**
  (`ident[4]` for a handler's dotted path, `ident[2]` for a reducer's), and two
  of the builders carried comments warning editors to append new fields at the
  end so those indices stayed valid.

  `rakaia.registration_log.RegistrationLog` owns the mechanism once, and each
  record type owns its own `identity` / `to_payload` / `identity_from_payload` —
  so the round trip is a property of the type rather than an agreement between
  two functions, and `modules()` reads `dotted_path` by name. No behaviour
  change; `registry.py` loses 89 lines and every positional index lookup, and
  the log is now testable against a bare in-memory store with no decorators,
  no source hashing and no import machinery.

- **`PreloadedProjectionReader` — bulk-fetch a verification sweep.**
  `diff_effects_against_rows` does one `reader.get` per effect (one round-trip
  each), which is thousands of round-trips on a full reconcile. Pass the same
  effect batch to `django_rakaia.verification.PreloadedProjectionReader(effects,
  using=...)` and it fetches every lookup up front — one query per `(model,
  lookup-shape)` group — then serves each `get` from an in-memory snapshot;
  lookups outside the batch (or relation-spanning ones) fall back to a live,
  memoised read. A point-in-time snapshot: for read-only verification, not live
  staged replay. Upstreamed from the Partisipa migration's `PreloadReader`
  workaround. → [`docs/projection-cookbook.md`](docs/projection-cookbook.md).

- **`assert_no_live_writes` — the rebuild gate's write-side guard** (ADR 0003).
  `django_rakaia.hermeticity` has documented this as the mirror of
  `deny_database_access` since ADR 0003 landed, but only shipped the read half;
  the write half lived in the first consumer's tree and every adopter would have
  re-derived it. It now ships here: wrap a from-scratch rebuild in
  `assert_no_live_writes(*models, using="default")` and any change to the live
  database's row counts raises `LiveWriteLeaked` naming the drift. The leak it
  exists to catch is a `post_save`/`pre_save` receiver saving without a `using=`
  — which Postgres' `session_replication_role = replica` does *not* disable, so
  a rebuild can silently mutate production while reporting itself green.
  Unlike the read guard (an `execute_wrapper`, so it cannot be armed where any
  legitimate query to the alias happens), counting rows tolerates reads — so this
  is the guard that can wrap a whole rebuild whose event log lives on the
  guarded alias.

- **Handler hermeticity guard** (P1, [ADR 0003](docs/adr/0003-handler-hermeticity.md)).
  `django_rakaia.hermeticity.deny_database_access(*aliases)` raises
  `AmbientDatabaseAccess` on any query to the named aliases — the read-side
  mirror of the rebuild gate's write-side `assert_no_live_writes`. Wrap the
  handler-dispatch region of a from-scratch rebuild in
  `deny_database_access("default")` and an ambient `Model.objects` read inside a
  handler (which would make a green rebuild lie) becomes a loud failure instead
  of a silent determinism leak. Read the log from an in-memory `StreamStore` (or
  another alias) so the event source doesn't trip it.

- **Machine-resolution transitions for `reconcile_by_key`** (#32). A retire that
  soft-deletes stale rows can now notify. Opt in with
  `reconcile_by_key(..., transition_kind="alert_transition")`: the executor
  captures the identities of the rows the open-guarded retire actually flipped
  (NULL→set) and returns them in `ApplyReport.retire_flips`, and the replay
  orchestrator emits one `external` transition per real resolution — skipped and
  counted on replay by default, delivered under `include_external=True`, so a
  rebuild never re-spams. `Executor.apply` now returns an `ApplyReport` (was
  `None`). → [`docs/alerts-projection.md`](docs/alerts-projection.md).

- **Versioned event handlers, upcasters & replay** (#3). Register handlers
  against a sequence range so old events run through the business logic that was
  correct *when they happened*; replay is idempotent and detects handler drift.
  Schema changes are absorbed by upcasters applied on read.
  → [`docs/versioned-handlers.md`](docs/versioned-handlers.md),
  [`examples/orders`](examples/orders/) (`just orders-demo`, #4).

- **Projections & fan-out with `reconcile_children`** (#6). Project one event
  into many rows without orphans — upserts plus a reconcile delete in a single
  transaction, so a shrinking collection prunes its dropped children.
  → [`docs/projections-and-fan-out.md`](docs/projections-and-fan-out.md).

- **Durable `DjangoStreamStore`** (#6). A database-backed store selected by the
  `RAKAIA_STORE = "durable"` setting, so the event log survives across processes
  and `manage.py replay <stream>` works outside the emitting process.
  → [Adopting the durable store](docs/django-integration.md#adopting-the-durable-store).

- **Delete effects & migration tooling** (#6). An `Effect(op="delete", …)` op and
  a dry-run path (`CollectingExecutor`) for previewing exactly what a replay
  would write before committing.
  → [`docs/dry-run-and-executors.md`](docs/dry-run-and-executors.md).

- **Durable Streams conformance suite in CI** (#10). Rakaia is checked against
  the upstream, language-agnostic `@durable-streams/server-conformance-tests`
  suite as a non-blocking CI job; run locally with `just conformance`. Closes
  several protocol gaps (TTL sliding window, HEAD TTL/Expires-At, offset=now
  long-poll, SSE data→control pairing, CORS `If-None-Match`).
  → [`conformance/README.md`](conformance/README.md).

- **`reconcile_tree` & `reconcile_aggregate` in core** (#17). Orphan-safe
  siblings of `reconcile_children` for unbounded nested repeaters and for grouped
  rollup summaries.
  → [`docs/projections-and-fan-out.md`](docs/projections-and-fan-out.md),
  [`docs/tree-reconcile.md`](docs/tree-reconcile.md).

- **Opt-in `skip_unchanged` executor** (#18). `DjangoExecutor(skip_unchanged=True)`
  writes only changed columns, avoiding no-op `UPDATE`s that churn `auto_now`
  fields, `post_save` signals, and replication when re-materialising large
  collections.
  → [`docs/dry-run-and-executors.md`](docs/dry-run-and-executors.md).

- **Staged replay in core** (#19). Handlers declare a `stage=`; replay runs
  stages in order and hands later stages a read-only projection reader, so a
  form can resolve references produced by another form — deterministic and
  self-healing, no backfills.
  → [`docs/staged-replay.md`](docs/staged-replay.md).

- **Adoption spike: `formkit-ninja`** (#6). A worked example proving a rakaia
  replay reproduces `formkit-ninja`'s direct `to_model()` rows byte-identically,
  while adding time-correct history.
  → [`examples/formkit_submissions`](examples/formkit_submissions/) (`just formkit-demo`).

- **Guided "What's new" tour, dry-run/executors reference, and a `just demo`
  recipe** that runs the scripted demos end-to-end.
  → [`docs/whats-new.md`](docs/whats-new.md).

### Changed

- **One producer response table instead of three.** The five-arm `isinstance`
  ladder over `ProducerValidationResult` — duplicate → 204, stale epoch → 403,
  bad epoch opening → 400, sequence gap → 409, closed stream → 409, each with
  its own headers — was written out three times inside `_handle_append`, the
  largest function in the largest module. The exception half of the same mapping
  had already been deepened into a single `_status_for` lookup; the union-typed
  half never followed. `producer_response(result, *, producer_epoch, offset,
  stream_closed)` now sits beside `_status_for`, and the three call sites pass
  what differs rather than restating the table. `_handle_append` drops from 244
  to 164 lines, and the status/header mapping is testable as a table — no ASGI
  round trip, no live store.

- **`replay` and `merge_replay` share one staging pipeline.** Reading one stream
  in order and k-way-merging several by an order key are genuinely different
  jobs — but everything *after* that was written twice: registry defaulting, a
  byte-identical `_upcaster_drift` closure, a `_ReplayCtx` with the same seven
  fields, the `any(stage > 0) or has_reducers()` test, the reader-required
  check, the stage-pass loop, and `events_processed`. Around sixty lines, in two
  control-flow shapes that had already drifted apart, so any change to staging
  semantics had to be made twice and correctly in both.

  `build_pipeline()` / `run_passes()` now own that; the two entry points are
  event *sources* that hand over `(seq, match_str, event)` tuples. `replay`
  drops 129 → 102 lines and `merge_replay` 144 → 119. Staging is now testable on
  a fabricated list of events — no store, no bytes, no upcasters — which is what
  the new tests use.

  `replay`'s single-stage path deliberately keeps its own streaming loop rather
  than going through `run_passes`: decoding lazily means a malformed event at
  offset N fails *after* the first N-1 have been dispatched, which is the
  partial-progress behaviour replay had before staging existed.

### Fixed

- **A handler can take an injected dependency without losing drift detection.**
  A stage-0 handler is called `fn(event)`, so a dependency had nowhere to go, and
  both routes out were broken. `functools.partial` was rejected outright by
  `hash_function_source` — with a message claiming the handler was not in an
  importable source file, which was untrue — pushing callers to a closure
  factory, which was accepted and silently harmful: its `dotted_path` carries
  `<locals>` so `rehydrate()` cannot restore it, and its `source_hash` covers the
  four-line wrapper rather than the function holding the logic. Rewriting the
  wrapped function entirely produced an **identical hash**, so drift detection —
  the feature that exists to catch exactly that edit — was blind, with the
  library reporting success throughout. `partial` is now unwrapped for both, and
  a *persisting* registry warns when a registration is not importable. Found in
  the first production consumer, which had four handlers in this state and a
  comment explaining why it had chosen the closure.


- **The two rebuild guards compose in either nesting order** (#101).
  `assert_no_live_writes` counts rows on the alias `deny_database_access`
  forbids reading, so nesting them the "wrong" way round made the closing
  `COUNT(*)` trip the read guard — and the `AmbientDatabaseAccess` it raised
  said *"replay touched the 'default' database directly"*, blaming the rebuild
  for a query the guard itself issued. In a tool whose only job is to say where
  a leak is, that sends the reader after one that does not exist. The guard now
  suspends rakaia's own deny wrappers while taking its counts — checking for a
  leak is bookkeeping, not the rebuild reading live data. A consumer's own
  `execute_wrapper` is left armed, and a genuine leak is still caught in either
  order.

- **`fold_events` no longer defaults to another project's vocabulary** (#100).
  `SCRATCH_PATH` was `"produce/submission"` — domain language from the first
  consumer, sitting in the generic Django integration. The value is arbitrary
  (the store is in-memory and discarded per call) but *load-bearing*, because a
  registry's `event_match` has to name it, so every other consumer was
  registering handlers against a stranger's stream naming. It is now
  `"_scratch/fold"`, namespaced so it cannot collide with a consumer's paths,
  and the docstrings say plainly that the value is arbitrary but must match
  `event_match`.

- **A payload timestamp now compares equal to the column it was encoded from**
  (#83). `canonical_value` had no temporal normalizer, so a `DateTimeField`
  projection reported a difference on every replay and never stopped: the event
  encoder truncates a datetime to **milliseconds** while the database stores
  microseconds, so a stored `…05.123456` never equalled the log's `…05.123`. A
  `DateField` was worse — its payload is the string `"2026-01-02"` and the
  column reads back as a `date`, which are never equal at all. Because
  `canonical_value` is shared with `DjangoExecutor(skip_unchanged=True)`, this
  also meant every replay re-`UPDATE`d every row carrying a timestamp, churning
  `auto_now` fields, `post_save` receivers and replication for a value that had
  not changed.

  `normalize_temporal` joins `DEFAULT_NORMALIZERS`, covering `DateTimeField`,
  `DateField` and `TimeField`. **Both sides truncate to milliseconds** —
  parsing the payload alone is not enough, since a parsed `…05.123` still would
  not equal a stored `…05.123456`. The accepted cost is that a genuine
  sub-millisecond change reads as unchanged; sub-millisecond precision cannot
  survive the log in the first place. It is a comparison device only: the column
  keeps its full precision. Note `DateTimeField` subclasses `DateField` in
  Django, so the checks are ordered — otherwise every timestamp would be
  truncated to its calendar date.

- **`RAKAIA_STORE` no longer fails open.** `get_store()` returned the
  *in-memory* store for any backend string that wasn't exactly `"durable"`, so a
  one-character typo (`"durrable"`, a stray space from a `.env` file, `"Durable"`)
  selected a process-local dict while the deployment believed it was durable:
  appends succeeded, nothing warned, and the entire event log vanished on the
  next restart. An unknown backend now raises `ImproperlyConfigured` naming the
  valid choices, and a refused backend is not cached, so correcting the setting
  works without a restart. A new system check reports the same problem at
  startup (`rakaia.E001`) rather than on the first append — which in a worker
  process could be hours in — and `"memory"` with `DEBUG = False` warns
  (`rakaia.W001`). ADR 0002 named this gap ("`RAKAIA_STORE` swaps stores by
  string with no interface check"); the first production consumer had written
  its own check downstream to catch it.

- **A verification sweep no longer certifies a population of zero.**
  `diff_effects_against_rows(...).ok` answers "did anything disagree?", which is
  vacuously `True` when nothing was compared — so an empty effect list reported
  a clean bill of health with nothing behind it. The ways that happens are all
  ordinary: a store on the wrong backend, a replay over a renamed stream path,
  an `event_match` filter that stopped matching, a registry that failed to
  autodiscover. `DiffReport` now carries a three-state `verdict`
  (`GREEN`/`RED`/`VACUOUS`) plus `compared` and `certified`, and
  `raise_if_diff()` raises the new `VacuousVerification` rather than passing.
  Failures are checked **before** vacuity, so a real failure is never hidden
  behind "empty"; pass `allow_empty=True` where a zero population is genuinely
  expected. `ok` keeps its original meaning, so existing callers are unaffected.
  Upstreamed from the first production consumer, which named this **the vacuous
  green** and had built the same guard downstream.
  → [`docs/projection-cookbook.md`](docs/projection-cookbook.md)

- **`@stream_model` now records the ambient envelope.** The decorator wrote
  `StreamEvent` rows directly rather than through a store, so `metadata` was
  always `{}` and `event_ts` always `NULL` — on the most-used append path in the
  Django integration. Two consequences: `ProvenanceMiddleware`, which exists to
  stamp actor and URL onto envelopes, could not reach the appends it was built
  for, and `history.envelope_actor` silently fell back to the payload's owner FK,
  answering "who owns this" in place of "who saved this". `event_ts` being NULL
  also forced `merge_replay` onto transport time, the ordering trap ADR 0002
  item 5 closed. The decorator now calls `merge_provenance` and sets `event_ts`
  exactly as both stores do; a fan-out to several streams shares one envelope,
  since it is one event. Appends outside a `provenance()` block are unchanged.

- **A bulk append now reaches live subscribers** (#82). `append_many` persists
  with `bulk_create`, which does not fire `post_save` — and on the durable store
  publication *was* a `post_save` receiver on `StreamEntry`, so every bulk append
  was silently invisible to SSE subscribers. The docstring's promise that it is
  "semantically identical to calling `append` once per item" was true of the rows
  written and false of the subscribers reached; nothing caught it because that
  semantic was never in the interface. The wire frame now has a single definition
  (`channels_signals.broadcast_entries`) which both the receiver and the store
  call, and `DjangoStreamStore` publishes its own appends rather than relying on
  a signal that its own write strategy bypasses. Restoring the signal by saving
  rows one at a time would have undone the reason `append_many` exists.

- **`append` now honours producer options on both stores.** `StreamStore.append`
  validated producer epoch/seq inline and recognised the idempotent
  close-duplicate; `DjangoStreamStore.append` ignored `options.producer_id`
  entirely — while its docstring claimed "outcomes, all now matching the
  in-memory store". Nothing caught it because every producer test routed through
  `append_with_producer`, but `WritableStore.append` is public and its
  `AppendOptions` carries those fields, so a consumer calling it directly got
  adapter-dependent behaviour.

  The whole admission sequence — closed → content-type → producer fencing →
  Stream-Seq — now lives in one pure module, `rakaia.append_decision`, which both
  stores ask. The ordering is the subtle part and was previously encoded only as
  the order of if-statements in two separate methods: fencing runs **before**
  Stream-Seq, because a retried append carries the same `Stream-Seq` it did the
  first time, so checking Stream-Seq first would raise `SequenceConflict` on
  exactly the retry that fencing exists to absorb. Seven new cases in
  `tests/server_store_contract.py` hold both stores to the same verdicts, and the
  rules themselves are now unit-testable as a table with no store, no database
  and no async.

- **A broken demo now fails instead of printing ✗ and exiting 0.** Seven checks
  across three examples computed a verdict, styled it red on failure, wrote it to
  stdout — and returned success. `just demo` reported a clean run on a library
  that had stopped upholding the guarantee the demo exists to show. Every check
  now raises `CommandError`, so "it ran" and "it was right" are the same
  outcome. A new `just demos` runs all eleven demos, and CI runs it: `examples/`
  was outside `testpaths` *and* outside `ruff check src/ tests/`, so nothing
  there was executed or linted by CI at all — which is how it drifted far enough
  to need a repair pass. `ruff` now covers `examples/` too.

- **`StreamServerStore.get` is typed, and the contract asserts it.** It was
  declared `-> Any`, and the conformance suite only checked `hasattr` for the six
  fields a server reads — which a backend's own row object satisfies too. So when
  `DjangoStreamStore.get` changed from returning its ORM `Stream` row to a
  `rakaia.types.Stream` metadata snapshot, nothing noticed, and the first
  downstream consumer broke on it. The snapshot is the right shape (a protocol
  server is async and reads these fields outside the `run_sync` bridge, so
  anything lazy would fail there); the mistake was that nothing said so. `get` is
  now `-> Stream | None` and two contract cases assert the type and its inertness
  on both backends. A new [`UPGRADING.md`](UPGRADING.md) documents this and the
  other breaks since `5e4a6e3`, with the exact edits.

- **A store now refuses an offset it did not issue.** Both stores accepted the
  other's offset format and resolved it to an unrelated position instead of
  failing: `int("0_5")` is `5` in Python, so the durable store read the wrong
  window, while the in-memory store's lexicographic comparison put a plain
  integer above every offset it emits, so a resume returned nothing and
  reported the client up to date. Either way a resume silently skipped
  messages. Both now raise `InvalidOffset` (400). `VALID_OFFSET_PATTERN` cannot
  make this call — offsets are opaque, not uniform (§6), so only the issuing
  store knows its own.

- **A non-dict event payload crashed the channel-layer signal.** `post_save` on
  `StreamEvent` called `.get()` on `data` unconditionally, so an append
  carrying a JSON array, string or number raised `AttributeError` — failing the
  append *after* the write had landed.
- **The offset pattern rejected the durable store's own offsets.**
  `VALID_OFFSET_PATTERN` accepted only the in-memory store's compound
  `{seq}_{byte}` form, so once the durable store backed the protocol server
  every resume read (`GET ?offset=…`) 400'd on an offset the server had just
  issued. It now accepts a plain integer too. The protocol makes offsets opaque,
  not identically formatted (§6), so this is a syntactic guard against junk in a
  URL and nothing more — deciding whether a token is *this* store's offset is
  the store's job, and each now raises `InvalidOffset` for the other's (400)
  rather than reading from whatever position it parses to.

- **Channel group names broke on any stream id containing a slash.** The
  sanitizer replaced colons only, which sufficed while ids looked like
  `user:1:projects`; a protocol path is `/orders`, and the slash raised
  `TypeError` from inside `post_save` — an append over HTTP failed at the
  broadcast, after the write had landed. Anything outside the channel layer's
  allowed set is now replaced, and over-long names are truncated.

- **Stream-Seq was compared as text, not as a number.** The header reached the
  store unparsed, so the conflict check `opts.seq <= stream.last_seq` compared
  strings: sending `Stream-Seq: 10` after `9` was rejected with a 409, because
  `"10" < "9"` lexicographically. Every producer broke on reaching double
  digits. The header is now parsed strictly (400 on a non-integer, matching the
  producer headers) and both fields are typed `int | None`.

- **`@stream_model` no longer appends phantom events for fixture loads** (#80).
  `handle_post_save` now honours Django's `raw` kwarg, so `manage.py loaddata`
  and `serialized_rollback=True` test restores no longer write one bogus
  `create`/`update` event per fixture row (multiplying on every restore), and
  can no longer crash mid-`loaddata` on a raw instance whose foreign-key rows
  are not loaded yet.

- **Event payloads are JSON-encoded end to end** (#80). A payload containing a
  `UUID`, `datetime`, or `Decimal` used to raise `TypeError` at insert time —
  from inside the consumer's `post_save`, i.e. crashing the very save being
  audited — forcing every transformer to pre-stringify. `StreamEvent.data` and
  `.metadata` now use `DjangoJSONEncoder`, and `create_stream_event` encodes the
  payload before the insert so the in-memory event carries the same primitives
  the row does — the SSE fan-out broadcasts that in-memory object, and a raw
  `UUID` there fails just as hard (msgpack under `channels_redis`, `json.dumps`
  under the SSE view). Consumers can now hand model field values straight
  through. Python-side only: the accompanying migration (`0006`) is a no-op on
  the schema.

- **Channels SSE receivers honour `raw`** (#80). `handle_stream_event_created`
  and `handle_stream_entry_created` no longer broadcast phantom frames for rows
  restored by `loaddata`/`serialized_rollback`, nor dereference `instance.stream`
  / `instance.event` mid-load when those parent rows are not restored yet.

### Removed

- **BREAKING — `django_rakaia.protocol_views` is gone.** It was a second,
  partial implementation of the Durable Streams protocol that did not use
  `DjangoStreamStore`, and it disagreed with the real one on nearly everything
  the protocol specifies: it routed by verb-in-path (`/streams/x/append`)
  rather than by HTTP method, returned newline-delimited JSON rather than a JSON
  array, dropped the envelope, and had no producer fencing, close, TTL,
  long-poll, ETag or CORS at all. It also carried two live defects — a
  handler-issued offset passed validation and then silently read the wrong
  window, and `?offset=now` returned the entire stream instead of nothing.

  Serve the protocol over the database by mounting `rakaia.create_app` — the
  same implementation the standalone server runs — via
  `django_rakaia.integration.get_asgi_app()` in `asgi.py`. It is an ASGI app,
  not a Django view, so it does not go in `urls.py`.
  → [`docs/django-integration.md`](docs/django-integration.md#protocol-http-api).

### Removed

- **The `Translatable` demo has left the library.** `django_rakaia` shipped a
  translations model and manager, an admin, an HTMX dashboard, JSON endpoints, a
  translations SSE feed and three templates. That is demo domain, not library
  surface — its `langcode` choices were hard-coded to `tet`/`pt`/`id` — and
  because the model sat in `0001_initial`, **every** consumer got a
  `django_rakaia_translatable` table whether they used it or not. It has moved
  to `examples/polyglot`, which was the only thing that ever used it (and whose
  own `signals.py` already carried the argument: *"we don't decorate the
  library's `Translatable` model itself — that would push demo concerns into the
  library"*).

  Migration `0008` **drops the table**; dump it first if you have rows.
  [`UPGRADING.md`](UPGRADING.md) carries the model definition to paste into your
  own app, and the removed URL list. The stream dashboard, the stream SSE
  endpoint and everything else under `/streams/` are unaffected — nothing in the
  core imported anything from the translations feature. One incidental routing
  change: a stream literally named `translations` now resolves to the stream
  detail page.

### Changed

- **BREAKING — `DjangoStreamStore.append` and `.append_many` return
  `AppendResult`,** not the `StreamEntry` row (and a list of them). This is what
  lets one protocol server implementation run on either store. The message is at
  `result.message`; a closed stream reports `result.stream_closed` instead of
  writing. Reach for the ORM rows via `read()` or the models directly.

- **BREAKING — `DjangoStreamStore.create` validates its configuration.** It
  records `content_type` / `ttl_seconds` / `expires_at` / `closed`, so a
  re-`create` with a *different* configuration now raises `StreamConfigConflict`
  where it previously ignored the mismatch silently. Re-creating with matching
  configuration is still idempotent.

  Both changes need the accompanying migration (`0007`), which adds the
  lifecycle columns to `Stream` and the `StreamProducer` table.

- **BREAKING — `AppendOptions.seq` and `Stream.last_seq` are `int | None`,** not
  `str | None`. See the Stream-Seq fix under Fixed.

- **BREAKING — `StreamServerStore` requires `run_sync`.** The protocol server
  is async but most of the store surface is synchronous, and how that sync work
  runs is the store's business: the in-memory store calls straight through,
  while `DjangoStreamStore` hands it to a thread (the ORM refuses async-context
  access). The server now routes every sync store call through
  `store.run_sync(fn, *args)`, so a third-party `StreamServerStore`
  implementation must provide it — subclassing either shipped store, or
  defining the one-line pass-through, satisfies the contract.

- **`@stream_model` takes `on_delete=` and `delete_to_dataclass=`** (#80), for
  soft-delete models. Under `pgtrigger.SoftDelete` a `DELETE` becomes `UPDATE
  is_active=false` and the row survives, but Django still fires `post_delete` —
  so the stream recorded a hard delete that never happened, with a stale
  pre-delete payload. Use `on_delete="update"` to emit the update that actually
  occurred, with `delete_to_dataclass=` supplying the post-delete payload — the
  trigger performs the flip inside the database, so `post_delete` is the *only*
  signal Django fires and it is the one place the soft delete can be caught.
  `on_delete=None` registers no `post_delete` receiver at all, for models that
  soft-delete in Python or whose deletes are not worth streaming. The default
  stays `on_delete="delete"`. →
  [`docs/django-integration.md`](docs/django-integration.md).

- **`skip_unchanged` compares through the field's canonical form, not raw `!=`**
  (P4). The executor's opt-in skip path now normalises both the stored value and
  the effect's `defaults` via the shared `canonical_value` (the same UUID/Decimal
  normalizer `diff_effects_against_rows` uses), so a value the column would round
  or re-type — a JSON `float` for a `DecimalField`, a UUID string for a
  `UUIDField` — is no longer counted as a change. Without this, replaying such a
  log rewrote the row on every pass, defeating the optimisation. With the default
  normalizers, "unchanged" now means the same thing in the migration diff and on
  the write path. (The skip path always uses `DEFAULT_NORMALIZERS`; a
  `diff_effects_against_rows` call given a *custom* `normalizers=` set is on its
  own — `DjangoExecutor` has no hook to match it.)

### Spikes / prototypes

Exploratory examples validating specific adoption stories (not production APIs):

- Streams-native audit log to retire `pghistory` (#12) —
  [`examples/partisipa_history`](examples/partisipa_history/) (`just partisipa-history-demo`).
- Staged replay for late-arriving cross-form links (#8) —
  [`examples/partisipa_staged`](examples/partisipa_staged/) (`just partisipa-demo`).
- Close-precondition state machine / guarded transition (#13) —
  [`examples/partisipa_close`](examples/partisipa_close/) (`just partisipa-close-demo`).
- Multi-stream merge replay across SF/TF/FF pipelines (#14) —
  [`examples/partisipa_merge`](examples/partisipa_merge/) (`just partisipa-merge-demo`).
- Tree-reconcile for unbounded nested repeaters (#16) —
  [`examples/partisipa_repeaters`](examples/partisipa_repeaters/) (`just partisipa-tree-demo`).

## [0.1.0]

- Initial Durable Streams protocol server (zero-dependency ASGI app) and the
  `django_rakaia` integration: normalized stream models, the `@stream_model`
  decorator, Channels-based SSE broadcasting, and the admin interface.
  → [`examples/chat`](examples/chat/).
