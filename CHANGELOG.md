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

- **Adoption spike: `formkit-ninja`** (#6). A worked example proving a rakaia
  replay reproduces `formkit-ninja`'s direct `to_model()` rows byte-identically,
  while adding time-correct history.
  → [`examples/formkit_submissions`](examples/formkit_submissions/) (`just formkit-demo`).

- **Guided "What's new" tour, dry-run/executors reference, and a `just demo`
  recipe** that runs the scripted demos end-to-end.
  → [`docs/whats-new.md`](docs/whats-new.md).

### Spikes / prototypes

Exploratory examples validating specific adoption stories (not production APIs):

- Streams-native audit log to retire `pghistory` (#12) —
  [`examples/partisipa_history`](examples/partisipa_history/) (`just partisipa-history-demo`).
- Staged replay for late-arriving cross-form links (#8) —
  [`examples/partisipa_staged`](examples/partisipa_staged/) (`just partisipa-demo`).
- Close-precondition state machine / guarded transition (#13) —
  [`examples/partisipa_close`](examples/partisipa_close/) (`just partisipa-close-demo`).

## [0.1.0]

- Initial Durable Streams protocol server (zero-dependency ASGI app) and the
  `django_rakaia` integration: normalized stream models, the `@stream_model`
  decorator, Channels-based SSE broadcasting, and the admin interface.
  → [`examples/chat`](examples/chat/).
