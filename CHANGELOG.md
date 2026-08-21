# Changelog

All notable changes to Rakaia are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project follows
[Semantic Versioning](https://semver.org/).

New here? The [guided tour](docs/whats-new.md) walks these capabilities with a
runnable demo for each.

## [Unreleased]

### Added

- **`DjangoExecutor(batch_updates=True)` collapses a fanned-out `Update` into one
  statement.** A handler that fans one change across many rows emits one `Update`
  per row — on purpose, so a verification pass can diff them one at a time — and
  pays one statement per row. Saving a form with eight repeating rows runs nine
  identical `UPDATE`s.

  *Collapses:* consecutive updates on one model, each matching a single field by
  equality on a non-null value, all writing the same plain values — a string,
  bytes, an integer, a boolean, `None`, or a `models.TextChoices` /
  `IntegerChoices` member over one of those.

  *May not:* anything else is applied one statement at a time, exactly as with the
  flag off — an expression such as `F("total") + 1`, a composite or traversing
  lookup, a lookup matching `NULL`, an unhashable value such as a JSON dict, or a
  value of any other type, including a `Decimal`, a `float`, and a date or
  datetime. Declining costs a statement, never a wrong row.

  *Worth it for:* a replay, reconcile or backfill. Each statement not issued is
  worth about 0.4 ms (Postgres, and mostly query-building rather than network — the
  same benchmark against in-memory SQLite still saves 0.13 ms), and the bound is
  the number of tables rather than the number of rows. On the shapes the consumer
  of #199 emits that is 1.3x for a typical form save, 4.7x for their worst one, and
  20x for a reconcile over 500 rows. See `docs/dry-run-and-executors.md`.

  **Off by default.** The rows come out the same either way, and that is checked
  by running the same effects down both paths and comparing every column — not
  argued. But the rule deciding what may collapse was wrong four times, and each
  time it wrote wrong data rather than raising, so a consumer opts in knowingly
  and a suspected write anomaly stays bisectable against it.

- **`rebuild_and_verify()` — one call for "can the log rebuild this, and does it
  match?"** Getting a trustworthy answer previously meant composing six
  interfaces in the right order — move the log off the database being guarded,
  arm `assert_no_live_writes` outside and `deny_database_access` inside, record
  the effects while still applying them (so a stage > 0 handler can read what
  stage 0 wrote), replay, build a `PreloadedProjectionReader`, diff — and then
  separately remembering the part written down nowhere: *a pass means nothing
  unless the guards were actually armed*. `hermeticity.py` asked the caller to
  check that by hand; ADR 0003 records the first production consumer leaving the
  read guard unwired for months.

  The new call does the composition and checks its own work. It trips the read
  guard on purpose and raises `GuardNotArmed` if nothing happens, so a green
  verdict cannot be obtained with the guard off. It refuses a from-scratch claim
  it cannot honour — `ScratchAliasNotEmpty` when the disposable alias still holds
  rows from an earlier run, which a stage > 0 handler would otherwise read as if
  the log had produced them. It returns the same `DiffReport`, so `verdict` still
  separates "nothing disagreed" from "nothing was compared". `live_models` is
  required rather than defaulted to empty: defaulting it would disarm the write
  guard without saying so, which is the failure this exists to stop.

  The individual pieces stay exported. This is the shortest route to a
  trustworthy answer, not the only permitted one.

- **`diff_effects_against_rows(effects, preload=True)` — the bulk verification
  path as one option.** Avoiding one query per row used to mean building a
  `PreloadedProjectionReader` with your effects and then passing *the same*
  effects to the diff, with nothing enforcing the "same". Get it wrong and you
  got a report rather than an error: one resting partly on a snapshot of a
  different batch, or — handing a generator to both — on nothing at all. The flag
  builds the reader from the list the diff is already given, so there is no second
  list to keep in step, and `using=` names the alias that reader reads from.

  The bulk fetch is a point-in-time snapshot, so the flag is for read-only
  verification and stays off during live staged replay, where the rows change as
  the replay writes them. `PreloadedProjectionReader` remains exported for a
  caller that needs it standalone, but handing one back to the diff now has to
  cover that call's effects — `PreloadMismatch`, a new exported error, instead of
  a half-snapshot answer — and `reader=` together with `preload=`/`using=` is a
  `TypeError` rather than one of them quietly winning. `rebuild_and_verify()`
  keeps its single bulk read and no longer composes the reader itself.

- **`DjangoExecutor(normalizers=...)`, and `Normalizer` as an exported name.**
  Deciding whether a stored value differs from the one the log carries is one
  question, and two paths asked it: `diff_effects_against_rows` to check that
  replaying reproduces the rows already there, and
  `DjangoExecutor(skip_unchanged=True)` to decide whether a write is a no-op it
  can skip. Only the first could be given a custom normalizer set, so a consumer
  with a domain-specific rule — a currency rounding convention, say — had a diff
  that honoured it and a skip path that did not, and no argument that would fix
  it: the diff would certify a row as unchanged while the executor rewrote it on
  every replay. Both now take `normalizers=`, both default to
  `DEFAULT_NORMALIZERS`, and handing the same sequence to each makes "unchanged"
  mean one thing. `Normalizer` is exported because it is the element type of a
  public parameter and could otherwise only be named by importing a submodule.

### Changed

- **A single append reads the stream row once, not twice.** Every
  `DjangoStreamStore.append` did two `SELECT`s of the same row: an unlocked one
  before the transaction opened, purely so that an expired stream would still be
  reaped when the write went on to report `StreamNotFound` — the reap is a
  delete, and the rollback that reports the 404 would otherwise undo it — and
  then the locked one the write actually decides against. The expiry now leaves
  the transaction as a signal and is reaped on the far side of the rollback, so
  the locked read does both jobs.

  A steady-state append is **7 statements, down from 8** — `BEGIN`, the locked
  stream read, the event `INSERT`, the locked high-water read, its `UPDATE`, the
  entry `INSERT`, `COMMIT` — and each one of them now does something. The
  reporter of #202 measured 13 against 0.2.0 and named three statements that
  looked redundant; the other two, a high-water read repeated either side of a
  get-or-create and a `MAX(offset)` scan beside the watermark it duplicates, both
  went in #175 and are already absent from a steady-state append. The
  set of statements is now pinned per append shape by
  `tests/test_django_rakaia/test_append_query_cost.py`, because none of this
  changes an observable outcome and so nothing else in the suite notices when a
  statement comes back.

  The first append to a path still costs four more: the high-water row has to be
  created, and seeded from a `MAX(offset)` scan, because `high = 0` means either
  "new path" or "install upgraded across migration 0005" and only the entry table
  can tell those apart. That is once per path, ever.

- **The value-equality rule moved to `django_rakaia.canonicalisation`.**
  `canonical_value`, `Normalizer`, `DEFAULT_NORMALIZERS` and the three
  `normalize_*` functions were defined in `verification.py`, which meant the write
  path imported the verify path to reach them. They now live in a module both
  import and neither owns.

  **No import needs to change.** `django_rakaia.canonical_value` and
  `from django_rakaia.verification import canonical_value` both still work — the
  exported *names* are Tier 1, the module defining them is not. One thing does
  improve: reaching `canonical_value` no longer pulls in the projection reader or
  the effect types, so the laziness `django_rakaia.__init__` exists for now holds
  one level further down.

- **A durable append no longer scans the entry table to find its place.** Every
  append recomputed the stream's high offset with an aggregate over all of its
  entries, and read the offset high-water row twice — once to create it, once to
  lock it. Allocation is now two queries: one locked read, one write. Reading the
  stream head drops the same scan — `Stream.current_offset` is one query, and
  `DjangoStreamStore.get_current_offset` two, the extra one being its own
  expiry check.

  The high-water row is authoritative once it has been advanced, so the scan
  survives for the single case that needs it: a watermark still at zero, which
  means either a brand-new stream or an install upgraded across migration `0005`
  (it creates the table without backfilling it). That runs once per stream and
  seeds the row for every allocation after it. No migration is required and no
  offset changes.

  **Worth knowing if you write `StreamEntry` rows directly.** An entry inserted
  without going through `Stream.get_next_offset_block` no longer advances the
  head, where the old aggregate would have absorbed it. The row is still stored
  and still returned by a read — but it sits *out of order*: with a watermark at
  1 and a hand-written entry at offset 99, the head stays at 1, the next
  allocations are 2, 3, 4 …, and a subscriber resuming past 99 never sees them.
  Allocation eventually reaches 99 and collides with the unique constraint.

  Nothing in rakaia writes entries that way, and `StreamEntry` is a Tier 2 read
  surface (see `docs/public-api.md`), so this is a narrowing of what the log
  tolerates rather than a break. A consumer inserting its own rows should
  allocate through the stream.

## [0.2.0] - 2026-08-15

The first release that describes the library. `0.1.0` was the initial
groundwork; everything below landed on `main` afterwards and, until now, was
reachable only by pinning a git revision — which is what every consumer was
doing, because the version number could not tell them what they had.

Several entries are breaking. See [UPGRADING.md](UPGRADING.md) for what to do
about each; it is organised by release from this version on.

**Known limitation.** Rakaia passes the Durable Streams conformance suite except
the stream-forking family — `Stream-Forked-From`, `Stream-Fork-Offset` and
`Stream-Fork-Sub-Offset`, 56 tests. Forking is not implemented. The gap is
baselined in `conformance/expected-failures.txt` so a real regression stays
distinguishable from it, and tracked in
[#61](https://github.com/joshbrooks/rakaia/issues/61). Everything else in the
protocol is covered by both stores.

### Added

- **`ApplyReport` says how much a batch actually wrote.** `skip_unchanged`
  computes exactly which columns differ and then threw the answer away, so a
  replay that rewrote every row and one that wrote nothing reported the same
  thing — converged state, and silence. The question the option exists to answer,
  *how much write churn did this cause*, could only be reached by counting
  queries around the call. `ApplyReport` now carries `upserts_created`,
  `upserts_written` and `upserts_skipped`, with
  `written + skipped == <number of upsert effects>` and `written - created` the
  number of updates. Scoped to `update_or_create` on purpose: that is where
  `skip_unchanged` is wired, because `update` already issues one UPDATE that does
  not advance `auto_now`, and deletes and retires are counted by their own
  effects. `InMemoryProjections` reports them too — it writes unconditionally, so
  its `skipped` is always 0, which is also what `DjangoExecutor` reports with the
  option off — and `tests/executor_contract.py` pins them for every executor.
  Additive fields with defaults; nothing breaks.

- **`rakaia.InMemoryProjections`, and shared contracts for the executor and reader
  seams.** Rakaia had four ways to apply effects and four ways to read
  projections back, and — unlike the store seam, which has two adapters and two
  shared conformance suites — nothing checked that any of them agreed. Rebuild
  verification rests entirely on `DjangoProjectionReader` and
  `PreloadedProjectionReader` giving identical answers, and a disagreement there
  would not have failed; it would have reported a clean rebuild that was not.

  Two suites close that: `tests/executor_contract.py` pins the batch semantics a
  handler author relies on (three ordered passes — every write, then every
  delete, then every retire, so convergence never depends on emission order; one
  `RefResolver` per `apply()`; `check_disjoint_defaults` before the first write;
  `retire_flips` only for retires that opted in), and
  `tests/projection_reader_contract.py` pins the reader surface, **including
  `model_label` being positional-only** — a signature divergence `isinstance`
  cannot see. `Executor` is now `@runtime_checkable`, matching every protocol in
  `protocols.py`.

  `InMemoryProjections` is the one in-memory implementation both halves are bound
  to: an `Executor` *and* a `ProjectionReader` over dict-backed tables, with all
  five ops, real `Ref` resolution against synthetic primary keys, and
  `__in`/`__isnull` matching. It replaces two half-implementations — a
  reader-only one in the replay tests and a writer-only one in
  `examples/multi_owner` — and the three example `Refs` classes are now
  `DjangoProjectionReader`, which they were strictly weaker copies of (no
  `using=` alias, so none of them could take part in rebuild verification).

- **`rakaia.seed_stream` — putting events into a stream is one call.** Getting a
  handful of events into a stream took four lines (create, loop, `json.dumps`,
  `.encode()`) and rakaia never shipped a way to do it, so the test suite carried
  six hand-rolled copies in three different shapes and the examples twelve more,
  four of them wrapped in a locally-defined `_append`. Setup in the worst test ran
  to sixty-eight lines before the first assertion.

  `seed_stream(path, events, store=..., encoder=...)` builds an in-memory
  `StreamStore` when no store is given, accepts any `WritableStore` when one is,
  and returns it either way. Payloads may be dicts or pre-encoded bytes; the
  label/metadata/`event_ts` envelope is **per event** — pair a payload with an
  `AppendOptions` — because a batch-level label is not what the callers needed.
  Creation is idempotent and non-destructive, so seeding an existing path appends
  and no caller needs a `has()` guard.

  The `encoder` parameter is the point: `django_rakaia.envelope.append_event` and
  `fold_events` are now built on it, passing `DjangoJSONEncoder` in, so there is
  exactly one `json.dumps` rule in the codebase rather than the drifting second
  copy `envelope.py` warns about. `append_event`'s output is unchanged, byte for
  byte, and a test pins that.

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

- **BREAKING — one `Effect` with an `op=` becomes four types, one per
  operation.** `Effect` carried thirteen fields, ten of which were meaningless on
  any given `op`, and needed five runtime `ValueError` checks to police the
  combinations that shouldn't have been writable at all. It is now `Upsert`,
  `Update`, `Delete` and `Retire`, sharing a `RowEffect` base (`model_label` and
  `lookup`, now both required), with `Effect` kept as the union of the four so a
  `-> list[Effect]` annotation still reads the same. Three of the five checks
  vanish because the field no longer exists on the wrong variant; the other two
  are modelled away — a delete's two alternative row-sparing mechanisms become
  one `spare` field with two shapes (`Exclude` or `SpareKeys`), and the
  `transition_kind`/`transition_key_fields` pair becomes one `Transition`. **The
  cross-field validation in `Effect.__post_init__` is now empty**; the only rule
  left is `Transition` rejecting empty `key_fields`, inside a two-field object
  rather than across a thirteen-field one. `EffectOp` is gone.
  → [UPGRADING.md](UPGRADING.md#effect-is-now-four-types-one-per-operation).

- **BREAKING — `external` effects leave the Effect family.** An `ExternalEffect`
  (an email, a webhook) names no row and no executor ever applied one — replay
  filtered them out before the executor, then either counted and dropped them or,
  under `include_external=True`, handed them to the *database* executor mixed
  into a write batch, which is why an example executor needed an `op="external"`
  branch. Now `replay()` collects them into **`ReplayResult.external:
  list[ExternalEffect]`** and returns them, replacing the
  `external_effects_skipped` count with the effects themselves. The
  `include_external=` parameter is gone from `replay()`, `merge_replay()` and
  `replay_stream`, along with `--include-external` on `manage.py replay`. A
  rebuild still delivers nothing by itself, but a caller who *wants* the
  transitions now has enough to send them.

- **BREAKING — `rakaia.types.Stream` no longer carries `messages`.** It is the
  metadata a store's `get()` returns, and the durable store documented the field
  as permanently empty — a lie on one of the two backends. The in-memory store
  keeps its messages in its own storage; read a stream with `read()`.

- **`diff_effects_against_rows(ops=...)` is now `kinds=`**, taking effect classes
  instead of op strings and defaulting to `(Upsert, Update)`. Callers on the
  default need no change.

- **One uv version, so the lockfile stops rewriting itself.** CI pinned the
  `setup-uv` action but not uv itself, so it installed whatever was latest and
  reserialised `uv.lock` in the newer style — a version-bump PR arrived carrying
  a few hundred lines of unrelated dependency-marker churn that nobody had asked
  for. Every workflow now pins the same uv, and `[tool.uv] required-version`
  refuses a local uv outside that range rather than quietly rewriting the lock
  under you.

- **One writer for the enveloped event** (#131). "Write an event into the Django
  models" was implemented twice — once in `DjangoStreamStore._write`, once in
  `create_stream_event`, the `@stream_model` decorator's door — and the two
  copies had drifted. The store records a labelless append under a stable
  `"append"` sentinel that its reader inverts back to "no label"; the decorator
  wrote the label through raw, so an event from that door could never be
  recognised as labelless. Each also called its own offset helper and resolved
  `metadata` its own way. `django_rakaia.envelope`'s module docstring already
  warned that a second copy of the envelope "produces events that replay
  differently from every other event in the same stream, and no test anywhere is
  looking at the difference" — this was that copy.
  Both doors now write through `write_enveloped_event`, which owns the sentinel,
  the ambient-`provenance()` merge, `{}` rather than NULL metadata, `event_ts`
  passthrough, and locking offset allocation. It takes a *list* of streams, so
  `@stream_model`'s fan-out is still one event with one envelope shared by N
  entries. `tests/test_django_rakaia/test_envelope_writer.py` drives both doors
  with the same envelope and compares the rows.
  Behaviour is unchanged except that a `@stream_model` event with an empty
  action string is now stored as `"append"` rather than `""` — the same shape
  the store has always written, and the same thing both read back as.

- **One content-routing rule, one registration record** (#132). Handlers and
  upcasters each answered the same question — *what string does this
  registration's glob get tested against?* — in their own method, the second
  carrying a comment saying it mirrored the first. `reset()` and `rehydrate()`
  were written out twice; the "a frozenset serialises as a sorted list" rule
  three times; and `identity` / `to_payload` / `identity_from_payload` once per
  record type, nine methods that had to agree pairwise with nothing checking
  that they did. Content routing is now one function both registries call, the
  meta-stream mechanics are one base class both registries inherit, and each
  record type declares its persisted fields once and derives the round trip from
  the declaration. `rakaia.registration_log` made the same move one level down;
  its module docstring says why. Routing behaviour is unchanged, and pinned
  first by tests that put identical inputs through both registries — including
  the two edges that carry the weight: a missing key routes as `""` and does not
  match, and a content-routed registration matched with no event raises.
  Content routing for upcasters (ADR 0002 item 3) is untouched; this is where
  that decision lands rather than a walk-back of it.

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

- **Type checking is a CI gate, not a report** (#143). `pyright src/` ran with
  `continue-on-error: true`, so its 34 errors were printed and discarded on
  every green build. That mattered more after the `Effect` split: the guarantee
  that an invalid effect is *unconstructible* is enforced by the type system and
  by nothing else, so a checker CI ignores is a guarantee CI does not hold. The
  step now fails the build.

  Getting to zero needed no new dependency and no loosened settings. Django's
  synthesised attributes — reverse accessors, the implicit `id` primary key —
  are declared on `Stream`, `StreamEvent` and `StreamEntry` under
  `if TYPE_CHECKING`, using the `RelatedManager` stub `django-types` already
  ships; `annotate()` aliases, which no stub can know about because the query
  invents them, are named in a `Protocol` beside the query that produces them.
  Neither construction runs. The old admin idiom of assigning
  `.short_description` onto a method is now `@admin.display(description=…)`,
  which is what Django has documented since 3.2.

  Two of the errors were worth having: `HandlerRegistry.reducers_for_stage` took
  `stage: int` while replay's non-staged pass passed `None`, working only
  because the comparison inside it never matched — the parameter is now
  `int | None` with the rule stated; and `@stream_model`'s check that
  `to_dataclass` returned a dataclass accepted a dataclass *class*, which then
  failed obscurely inside `asdict` instead of with the clear message every other
  wrong return already got.

### Fixed

- **The fault-injection endpoint is no longer reachable on a deployed server**
  (#130). `/_test/inject-error` was matched before any stream routing, with no
  gate of any kind. An unauthenticated `POST` of
  `{"path": "orders", "status": 500, "count": 999999}` made every subsequent
  request to that path fail, indefinitely, for every client — a denial of
  service any anonymous caller could aim at any stream. Harmless while the
  server only ever ran on a laptop; not once the package is published and
  someone deploys it.
  The endpoint is now off unless `RAKAIA_ENABLE_FAULT_INJECTION` is set to a
  truthy value, via a new `ServerOptions.enable_fault_injection` that reads it
  the same way `long_poll_timeout` reads `LONG_POLL_TIMEOUT`. The environment
  variable is what matters: `uvicorn rakaia:app` builds the module-level app
  with default `ServerOptions()`, so a constructor argument alone could never
  reach it. While off the path is not routed at all and answers `404` like any
  other unknown path — deliberately not `403`, which would confirm that the
  endpoint exists. Nothing needs the flag today: the upstream conformance suite
  never calls the endpoint, so `conformance/run.sh` does not set it and the
  baseline in `conformance/expected-failures.txt` is unchanged.
  Six of `InjectedFault`'s ten fields were also stored and never read —
  `delayMs`, `dropConnection`, `truncateBodyBytes`, `corruptBody`, `jitterMs`
  and `injectSseEvent` — so a caller asking for a 200ms delay got no delay and
  no error either. They are gone; `count`, `status`, `retryAfter` and `method`
  are the four that ever did anything.

- **A handler registered somewhere other than where it is defined comes back
  from a restart** (#117). `rehydrate()` restores registrations by re-importing
  modules and letting their `@register_handler` decorators run again, and it
  worked out which modules to import by chopping the last segment off the
  function's dotted path. That assumed the decorator ran in the module the
  function was defined in. Binding a dependency with
  `functools.partial(fn, **deps)` — the documented way to do it — breaks that
  assumption by design: the partial is built in your app's `handlers.py` while
  `fn` lives in a shared module, so rakaia imported the shared module, no
  decorator ran, and the handler was simply absent after a restart with nothing
  logged. The same line mis-derived a handler defined as a method, whose
  qualname `pkg.mod.Class.method` chopped down to the *class*. A registration
  now records `registered_in` — the module of the frame that registered it —
  and that is what `rehydrate()` imports; `dotted_path` keeps meaning "where the
  logic is" for drift reporting. Meta-streams written before the field existed
  fall back to the old derivation, which is correct for exactly the cases that
  used to work, so upgrading does not re-append an existing log.
- **The dashboard views refuse an offset the durable store never issued** (#122).
  Two Django views parsed a client-supplied offset with bare `int()`, outside
  any store's ownership check: `after_offset` on the JSON events endpoint, and
  the `Last-Event-ID` header the browser sends when an `EventSource` reconnects.
  Python reads `_` as a digit separator, so the in-memory store's compound
  `{seq}_{byte}` offset — `int("0_5")` is `5` — resolved to an unrelated
  position: the JSON API returned the wrong window with a `200`, and the SSE
  endpoint resumed in the wrong place. An offset that did not parse at all fell
  into `except ValueError: 0` and silently replayed the entire stream to a
  client that believed it was resuming. Both now parse through the durable
  store's own strict check and answer `400`; failing a connection is visible,
  and neither wrong answer was.

  That check has moved to `django_rakaia.offsets` alongside `format_offset`,
  which also settles a layering inversion: `Stream.current_offset` used to
  import `format_offset` back out of the store module for its own property, and
  duplicated `DjangoStreamStore.get_current_offset`'s watermark query verbatim.
  The store now delegates to the model property; all it adds is the expiry
  check and the absent-stream `None`.
- **A closed stream answers the same way on both stores, whichever append door
  you use** (#119). `append_with_producer` on the durable store ran its own
  admission sequence — producer fencing *before* the closed check, and never
  reading who closed the stream. A producer whose sequence had also drifted was
  told about the gap instead of being told the stream was closed, and a producer
  retrying its own closing append was never recognised as a duplicate; the
  in-memory store got both right. Both doors on both stores now ask
  `rakaia.append_decision`, the one shared admission sequence, and the shared
  store contract drives the combination that was missing, which is why the drift
  had gone unseen. A refusal on a closed stream now always carries a producer
  result — `ProducerDuplicate` for a retry of the closing tuple, otherwise
  `ProducerStreamClosed` — which is the same 409 (or 204) on the wire as before,
  since the server already synthesised the closed result when the store gave it
  nothing.


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

- **`Stream-Seq` is an opaque string, compared byte-wise**
  ([#137](https://github.com/joshbrooks/rakaia/issues/137)). Rakaia spent part
  of this cycle parsing the header as a decimal integer, comparing it
  numerically, and returning `400 Bad Request` for anything that was not one.
  The protocol specifies none of that: `Stream-Seq` values are opaque strings
  that **MUST** compare using simple byte-wise lexicographic ordering, so any
  value is well-formed and a ULID is as valid as a digit string.

  **If you send unpadded decimals, pad them.** Byte-wise, `"10"` sorts below
  `"9"`, so `Stream-Seq: 10` after `9` is a `409 Conflict` — correctly. Send
  `"09"` then `"10"`, at whatever fixed width your writer will reach; rakaia's
  own offsets zero-pad for exactly this reason. Nothing else in the header's
  behaviour changed, and a writer already padding or using a ULID is unaffected.

  Seven conformance tests were failing on the numeric comparison. They pass now,
  leaving the stream-forking family as the only known gap.

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

- **Three helpers nobody was using are gone.** All three were added after
  `0.1.0` and never appeared in a tagged release, so **no upgrade note is
  needed** and no released code can break: there is nothing to migrate from.

  - `send_sse_event` — an internal helper for writing a Server-Sent Event that
    nothing in the library ever called. It was also the unsafe twin of the code
    the protocol server actually uses: the real one normalises carriage returns
    so a payload cannot forge an event boundary, and this copy did not.
  - `dispatch_external` — routed the "external" effects rakaia deliberately
    never applies (email, webhooks) to per-kind handlers. Its own documentation
    described the alternative: a two-line loop in your own code. The
    `multi_owner` example now writes that loop.
  - `recover_peak_snapshot` — recovered a record's most complete historical
    snapshot after a legacy blank save. Once the audit rows exist this is a
    one-line scan over them, and each application wants a slightly different
    version, so the `partisipa_history` example keeps its own — as it always
    did.

  `check_disjoint_defaults`, in the same module as one of these, stays: it runs
  on every effect apply.

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

- **BREAKING — `AppendOptions.seq` and `Stream.last_seq` are `str | None`,** and
  the durable `Stream.last_seq` column is text rather than an integer. See the
  `Stream-Seq` entry under Fixed. Needs migration `0009`, which widens the
  column; the values it held were decimal digits and survive unchanged.

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
