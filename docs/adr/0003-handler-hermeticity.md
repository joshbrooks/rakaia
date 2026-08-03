# ADR 0003 — Handlers are hermetic: reads go through the injected reader

- **Status:** Accepted
- **Date:** 2026-07-31
- **Deciders:** rakaia maintainers
- **Related:** [`staged-replay.md`](../staged-replay.md),
  [`dry-run-and-executors.md`](../dry-run-and-executors.md),
  `django_rakaia.hermeticity` (`deny_database_access`), the `using=` seam
  (#68 item 2, [`test_using_seam.py`](../../tests/test_django_rakaia/test_using_seam.py)),
  Partisipa `assert_no_live_writes`

## Context

A rakaia handler is a **pure function of the event**: stage 0 is
`event -> Effect`, a stage > 0 handler is `event, reader -> Effect`. Two of the
system's load-bearing guarantees rest on that purity:

1. **Replay determinism.** Re-running a range reproduces the same materialized
   state, because every fact a handler used came from the event or from a
   `reader` that only ever reads committed projections (themselves a pure
   function of the log).
2. **Disposable-DB verification.** A from-scratch rebuild replays the log into a
   throwaway database (`DjangoExecutor(using="rebuild")` +
   `DjangoProjectionReader(using="rebuild")`) and diffs the result against live.
   A green diff is only meaningful if the rebuild consulted **nothing but the
   log** — otherwise it is quietly reading the very state it claims to
   reconstruct.

Both guarantees are violated the moment a handler — or a helper it calls —
reaches for an **ambient default-DB manager** (`SomeModel.objects.filter(...)`)
instead of the injected `reader`. The `.objects` manager binds to the `default`
connection regardless of the alias the rebuild targets, so:

- the handler's output depends on live DB state, not the log (non-deterministic
  across rebuilds); and
- the rebuild silently consults production, so its "clean rebuild" verdict is
  not actually log-only.

This is not hypothetical. A shared projection helper computed a nullable-FK
"drop it if the target row is gone" check with
`fk.related_model.objects.filter(pk=val).exists()` — an un-aliased read on
`default` — from inside a stage-0 projection that the rebuild gate runs under
`using="rebuild"`. The dangling-FK decision was therefore made against live
data, not the disposable DB. It was benign only because the reference tables it
happened to touch were mirrored into the rebuild DB; a nullable FK to any
*projected* (rebuilt-from-scratch) table would have diverged silently.

The rebuild gate already guards the **write** side — `assert_no_live_writes`
asserts the default DB's row counts are unchanged across a rebuild, turning a
leaked `post_save` into a loud failure. But a stray **read** changes no row
count, so the write-side guard cannot see it. Reads need their own guard.

## Decision

1. **A handler reads only through the injected `reader`** (and the alias it
   carries), never through an ambient model manager. Any datum a stage > 0
   handler needs from another projection is fetched with
   `reader.get(...)` / `reader.filter(...)` / `reader.query(...)`. A stage-0
   handler that needs to consult existing state is **the wrong stage** — promote
   the check to a stage that has a reader.

2. **Helpers a handler calls inherit the rule.** "Pure function of the event"
   is transitive: a projection helper (`row_defaults`, a status derivation, a
   dangling-FK check) must take the alias/reader from its caller rather than
   reach for `.objects`. If a helper genuinely cannot be made hermetic, it does
   not belong on the replay path.

3. **Hermeticity is enforced, not trusted.** `django_rakaia` ships
   `deny_database_access(*aliases)` — a context manager that installs a Django
   `execute_wrapper` raising `AmbientDatabaseAccess` on any statement issued to
   the named aliases. Wrap the handler-dispatch region of a rebuild in
   `deny_database_access("default")`; an ambient read then fails loudly instead
   of leaking. It is the read-side mirror of `assert_no_live_writes`.

4. **The event source is held to the same standard.** A hermetic rebuild reads
   its log from a store that does not touch the denied alias — an in-memory
   `StreamStore`, or a store on a different alias. A `DjangoStreamStore` on
   `default` would (correctly) trip the guard: if the only copy of the log lives
   in the database you are proving you can reconstruct, the proof is circular.

## Consequences

- **A whole class of silent divergence becomes a test failure.** The dangling-FK
  leak above would have been caught the first time the gate ran under
  `deny_database_access`, instead of lurking until a projected-table FK made it
  bite.
- **Stage boundaries carry real weight.** "Does this handler need to read
  existing state?" now has a mechanical answer: if yes, it is stage > 0 and
  takes the reader; a stage-0 handler that reads the DB is a bug the guard
  reports.
- **Write guard + read guard together certify a rebuild.** `assert_no_live_writes`
  (no leaked writes) and `deny_database_access` (no leaked reads) make
  "reconstructed from the log alone" a property you assert in CI, not a claim a
  reviewer signs off on by eye.
- **Cost is negligible.** The guard is an `execute_wrapper` that raises; it adds
  nothing when no query is issued to a denied alias, and short-circuits the
  first one that is.
