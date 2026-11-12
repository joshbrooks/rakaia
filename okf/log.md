# Directory Update Log

## 2026-11-12

* **Creation**: [partisipa_pipeline](examples/partisipa-pipeline.md) — one suku's forms through a
  six-step pipeline, saved one at a time and then rebuilt and compared with live; the code behind
  the new explainer pages in `docs/explainers/`.
* **Creation**: [sqlite_replay](examples/sqlite-replay.md) — replay an order stream into a
  SQLite file with `SqliteExecutor`, no Django (#368).
* **Update**: [Effects & executors](concepts/effects-and-executors.md) — `SqliteExecutor` and
  `SqliteProjectionReader`.
* **Update**: [Protocol & streams](concepts/protocol-and-streams.md) — `TaggedReads` and
  `read_tagged`, asking a store for the events carrying given tags, and the
  `tagged_reads_contract` suite (#365).
* **Update**: [Protocol & streams](concepts/protocol-and-streams.md) — `check_tagged_read` and
  `pick_tagged` exported from `rakaia` (#365).

## 2026-11-11 (store author kit)

* **Update**: [Protocol & streams](concepts/protocol-and-streams.md) — `plan_appends`,
  `plan_initial_write`, `StreamFacts`, `AppendPlan`, `AppendVerdict`, `PlannedWrite`,
  `page_of`, `check_page_size`, and the `rakaia.testing` contract suites, now importable
  from the installed package.
* **Update**: [Effects & executors](concepts/effects-and-executors.md) — `apply_effects`,
  `UpsertOutcome`, `RowAccessor`, `ApplyReport` and `OnDriftPolicy` for writing an executor.

## 2026-11-11

* **Update**: [Django integration](concepts/django-integration.md) — `RAKAIA_STORE` accepts
  the dotted import path of a store of your own, and `rakaia.E003` reports one that does not
  import (#364).
* **Update**: [Django integration](concepts/django-integration.md) — `RAKAIA_JSONL_ROOT`,
  `RAKAIA_JSONL_SEGMENT_SIZE`, `RAKAIA_JSONL_FSYNC` and `RAKAIA_ENABLE_SSE`, which the code
  read but the bundle never named.
* **Update**: [Protocol & streams](concepts/protocol-and-streams.md) —
  `RAKAIA_ENABLE_FAULT_INJECTION`. A test now fails when a setting or management command is
  missing from `okf/`.

## 2026-11-01

* **Creation**: [consumer_rebuild](examples/consumer-rebuild.md) — a consumer meets
  a stream deleted and recreated (`replaced` / `Consumed.needs_rebuild`), a transient
  apply error that records nothing and is retried, and a pass cut short that converges
  on the next run.
* **New**: [two_workers](examples/two-workers.md) — two worker processes under one consumer name:
  the second is seen waiting on the cursor row in `pg_stat_activity` and applies nothing, a commit
  behind the saved position raises `CursorMovedBackwards`, and a resume reads five events out of a
  205-event log. The lock half needs Postgres; on SQLite the demo prints
  `skipped: SQLite has no row locks`.
* **Update**: [polyglot](examples/polyglot.md) — `migrate_stream` keeping offsets and
  adopting the source's stamp (`adopt_generation`) so a consumer resumes, and
  renumbering so it stops as `replaced` (`just migrate-consumer-demo`).
* **Update**: [orders](examples/orders.md) — the impure-handler warning,
  `RAKAIA_STRICT_HANDLER_FINGERPRINTS`, and drift through a `functools.partial` that a
  closure misses (`just hermeticity-demo`).
* **Update**: [partisipa_close](examples/partisipa-close.md) — an accepted close now `Retire`s the
  suku's open finance lines, stamped with the close event's own timestamp rather than the replay
  clock; the demo replays the same stream under a clock shifted six years and asserts identical
  stamps, then replays a deliberately `now()`-stamped variant to show the check bites.
* **Update**: [Effects & executors](concepts/effects-and-executors.md) — `Retire`'s stamp is
  demonstrated end to end by `partisipa_close`: replay under a shifted clock writes the same values.
* **Update**: [Consuming & outcomes](concepts/consuming-and-outcomes.md) and
  [Django integration](concepts/django-integration.md) — the new example, and
  `migrate_stream` as the copy that makes a saved position valid or stops the consumer.
* **Update**: [Consuming & outcomes](concepts/consuming-and-outcomes.md) — `two_workers` now
  demonstrates the cursor row lock and the forward-only commit, not only `partisipa_intake`.
* **Update**: [Versioned handlers & replay](concepts/versioned-handlers-and-replay.md) —
  partial vs closure fingerprints.

## 2026-10-31

* **Update**: [Protocol layer & streams](concepts/protocol-and-streams.md) —
  `StreamStore.adopt_generation`, and `get_generation` beside it in the method
  list: a migration that kept every offset keeps the source's stamp, so a
  consumer that had run before resumes instead of stopping as `replaced`.
* **Update**: [Consuming & outcomes](concepts/consuming-and-outcomes.md) — the
  same migration carries the saved cursor's stamp match forward.

## 2026-10-30

* **Update**: [Consuming & outcomes](concepts/consuming-and-outcomes.md) — the lock covers only
  `DjangoConsumerCursorStore`, a pass is one transaction over the whole backlog, and the savepoint
  is on the cursor alias only.
* **Update**: [Protocol layer & streams](concepts/protocol-and-streams.md) — a file stream with no
  recorded `segment_size` still in its first segment is refused when its entries do not fit the
  size you pass.

## 2026-10-29

* **Update**: [Protocol layer & streams](concepts/protocol-and-streams.md) — `JsonlStreamStore`
  records `segment_size` per stream and raises the new exported `SegmentSizeMismatch` when opened
  with another; deletes and creates take the stream lock (GitHub #346).
* **Update**: [Django integration](concepts/django-integration.md) — `rebuild_and_verify`
  counts rows that share a key instead of collapsing them, so a second live row behind one
  upsert key now fails the check.
* **Update**: [Django integration](concepts/django-integration.md) — a `@stream_model` save that
  fans out to several streams checks them all first, so one closed stream refuses the whole save
  on the memory and file stores too, leaving none written.
* **Update**: [Consuming & outcomes](concepts/consuming-and-outcomes.md) — two workers under
  one consumer name take turns (`DjangoConsumer.run` locks the cursor row for the pass), and
  `commit_cursor` raises the new `CursorMovedBackwards` for an offset behind the saved one.

* **Update**: [Consuming & outcomes](concepts/consuming-and-outcomes.md) — `prune_outcomes`
  keeps the latest record for each consumer, stream and subject, so pruning cannot turn a
  skipped event into one that reads as succeeded.

* **Update**: [Consuming & outcomes](concepts/consuming-and-outcomes.md) — the manual
  `poll_consumer` + `commit_cursor` path can detect `replaced` too: `Poll.generation` carries the
  stream's stamp and `commit_cursor(..., generation=)` saves it. `register_transient_errors` is
  gone; `django_consumer` / `DjangoConsumer` pass Django's `OperationalError` and
  `InterfaceError`, and core `consume` treats nothing as transient unless told. A file-backed
  stream from before stamps is given one the first time it is asked for.

* **Update**: [Consuming & outcomes](concepts/consuming-and-outcomes.md) — a consumer whose
  stream went backwards (`rewound`) or was deleted and recreated or expired (`replaced`) now
  applies nothing and keeps its cursor (`Consumed.needs_rebuild`); the stream stamp
  (`get_generation`, the `generation` column, migration `0014`) is saved beside the cursor.
  A dropped connection or deadlock stops the pass (`Consumed.transient`) instead of being
  recorded as a failed event.
* **Update**: [Django integration](concepts/django-integration.md) — `rebuild_and_verify`
  diffs the rebuilt table against the live table both ways over `live_models`, and a
  `RowDiff` reports which side holds a row the other lacks.
* **Update**: [Django integration](concepts/django-integration.md) — a `@stream_model` save
  appends through the store `RAKAIA_STORE` names instead of writing database rows directly,
  so a closed stream refuses it with `StreamClosed`; check `rakaia.W003` warns when a streamed
  model meets the in-memory store.
* **Update**: [Django integration](concepts/django-integration.md) and
  [Versioned handlers & replay](concepts/versioned-handlers-and-replay.md) — handler
  fingerprints cover helpers and constants (`v2:` prefix); `RAKAIA_STRICT_HANDLER_FINGERPRINTS`
  turns the impure-handler warning into an error.

## 2026-10-27

* **Update**: [Effects & executors](concepts/effects-and-executors.md) — `CollectingExecutor`
  runs the same collision and `Ref` checks as a real write and reports upsert and retire-flip
  counts as a write onto empty tables would. `InMemoryProjections` raises `MultipleRowsMatched`
  for an upsert lookup matching several rows and orders flipped rows by value; `MultipleRowsMatched` is a `RakaiaError` with reason code
  `multiple_rows_matched`.

* **Update**: [Protocol layer & streams](concepts/protocol-and-streams.md) and
  [Django integration](concepts/django-integration.md) — the protocol server takes an
  access hook, an allowed-origins list, a body limit (`413`) and an SSE timer, with
  `RAKAIA_AUTHORIZE`, `RAKAIA_ALLOWED_ORIGINS`, `RAKAIA_MAX_BODY_BYTES` and
  `RAKAIA_SSE_MAX_SECONDS` under Django; an SSE handler ends when its client leaves; the
  dashboard's live feed needs a login.

## 2026-10-14

* **Update**: [Protocol layer & streams](concepts/protocol-and-streams.md) and
  [Django integration](concepts/django-integration.md) — permanent streams apply on every store
  (`permanent=` on `StreamStore` and `JsonlStreamStore`; `RAKAIA_PERMANENT_STREAMS` reaches
  whichever backend `get_store()` builds), and `delete(path, force=True)` on all three.

* **Update**: [Event envelope & provenance](concepts/event-envelope-and-provenance.md) — every
  store stamps an append that sets no `event_ts` (the durable store used to leave it null), and
  tags and correlation ids are checked only on an append that will be written.

* **Update**: [Event envelope & provenance](concepts/event-envelope-and-provenance.md) — a
  batch's unstamped items share one stamp, and a stream's initial `PUT` body records no ambient
  provenance or correlation id on any store. [Django integration](concepts/django-integration.md)
  — every store's `delete()` is refused on a permanent store without `force=True`, not only the
  durable one's.

## 2026-10-06

* **Update**: [Event envelope & provenance](concepts/event-envelope-and-provenance.md) — a tag
  or correlation id containing a NUL character or invalid UTF-8 is refused, like an empty or
  over-long one.

## 2026-10-02

* **Update**: [Event envelope & provenance](concepts/event-envelope-and-provenance.md) —
  `AppendOptions`/`StreamMessage` gain `tags` and `correlation_id`, `AppendResult` gains
  `event_id`, and `provenance(correlation=...)` supplies an ambient correlation id.
* **Update**: [Django integration](concepts/django-integration.md) — the `StreamEventTag` table
  and `correlation_id` column (migration `0013`), the `tagged()`/`correlated()` lookups, the
  `api/events/` endpoint, the admin filters, and `append_event`'s new arguments and result.
* **Update**: [Versioned handlers & replay](concepts/versioned-handlers-and-replay.md) — the
  engine module is now `src/rakaia/replaying.py`. `from rakaia import replay` is unchanged; the
  module path `rakaia.replay` is gone, so the function is no longer shadowed by a module of the
  same name.

## 2026-09-23

* **Creation**: [formkit_emission](examples/formkit-emission.md) — formkit-ninja's own
  `emit`/`wire` decomposition appended to a rakaia log, and the check that an absent key
  and an explicit null come back as different events. The durable store is held to the
  same property by `TestAnAbsentKeyIsNotANullOne`; the demo's own store keeps payloads as
  opaque bytes and could not lose it.
* **Update**: [Django integration](concepts/django-integration.md) — `RAKAIA_PERMANENT_STREAMS`
  now refuses `DjangoStreamStore.delete()` from Python unless it is called with
  `force=True`. The switch previously refused only a client's protocol DELETE, which left
  the hand-run management command — the likeliest way to lose a stream — outside the guard
  it looked like it covered.

## 2026-09-22

* **Creation**: [stream_coverage](examples/stream-coverage.md) — a stream that fell behind its
  table, found by `check_stream_coverage` and repaired.
* **Update**: [Django integration](concepts/django-integration.md) — `stream_coverage` and the
  command, demonstrated by the new example.
* **Update**: [Protocol layer & streams](concepts/protocol-and-streams.md) — paged catch-up
  reads (`ServerOptions(read_page_size=…)`, `read(limit=…)`), the two permanent-stream
  refusals (`ExpiryNotAllowed`, `DeleteNotAllowed`), a delete that takes the stream's own
  events, and the stamp that now agrees with an event's position.
* **Update**: [Django integration](concepts/django-integration.md) — the settings and
  operator commands the bundle had never listed: `RAKAIA_READ_PAGE_SIZE`,
  `RAKAIA_PERMANENT_STREAMS`, `manage.py prune_orphan_events`, and the stream lock order a
  model save now shares with a protocol append.
* **Note**: everything in the two entries above shipped in 0.6.0, 0.6.1 and 0.7.0, and none
  of it reached this bundle before its release. Only examples are gated — a change that adds
  a setting or a command without adding an example walks past the test that would have
  caught it.

## 2026-09-08

* **Creation**: [Consuming a stream & failure records](concepts/consuming-and-outcomes.md) — a seventh
  concept group, for the loop that reads a stream and the record of what it could not apply
  (`poll`/`consume`, `Consumer`, `Outcome` and the three outcome stores, the published reason codes,
  the retention command and the screen).
* **Update**: [Effects & executors](concepts/effects-and-executors.md) — `RecordingExecutor`.
* **Creation**: [partisipa_intake](examples/partisipa-intake.md) — the worked example of the whole
  consuming loop (added with the example itself, logged here now).
* **Note**: a test now fails if an `examples/` directory has no page in this bundle, and the names
  in these pages are swept for resolution along with the rest of the documentation. Until today
  nothing checked either, and this log had not been written to since the bundle was created.

## 2026-07-28

* **Creation**: Rakaia knowledge bundle ([index](index.md)) — initial OKF catalog of rakaia's concepts and examples.
* **Creation**: [Protocol layer & streams](concepts/protocol-and-streams.md), [Versioned handlers & replay](concepts/versioned-handlers-and-replay.md), [Effects & executors](concepts/effects-and-executors.md), [Projections & fan-out](concepts/projections-and-fan-out.md), [Event envelope & provenance](concepts/event-envelope-and-provenance.md), [Django integration](concepts/django-integration.md) — the six concept groups.
* **Creation**: 13 example concepts under [examples/](examples/index.md), each cross-linked to the concepts it demonstrates; demos run green are marked `verified`.
