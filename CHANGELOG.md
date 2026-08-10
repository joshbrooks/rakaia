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

- **Named store failures.** A store now raises one of `StreamNotFound`,
  `StreamConfigConflict`, `SequenceConflict`, `ContentTypeMismatch`,
  `InvalidJson` or `EmptyJsonArray` (all exported from `rakaia`) instead of a
  bare `ValueError`/`KeyError`, and the ASGI server maps them to a status by
  type via `rakaia.handler.STORE_FAILURE_STATUS`. Previously the server picked
  the status by matching English in `str(e)`, so rewording a message in
  `store.py` silently turned a 4xx into an unhandled 500 — and any other store
  implementation had to reproduce five exact strings to behave the same. Each
  failure subclasses the builtin it replaced, so existing `except ValueError` /
  `except KeyError` code and tests are unaffected. A new failure type without a
  status now fails the suite rather than 500ing at runtime.

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

### Fixed

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

### Changed

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
