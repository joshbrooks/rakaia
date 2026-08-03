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
