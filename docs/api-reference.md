---
icon: lucide/list
---

# Python API reference

Every name the two packages export, with its signature. This page is generated
from `rakaia.__all__` and `django_rakaia.__all__`, so it lists exactly what is
importable and nothing else.

For *what these names promise* — which are stable, which may change, and how to
pin a version — see [the public API](public-api.md). This page is the index; that
page is the contract.

!!! note "Generated file"

    Do not edit by hand. Run `just api-reference` and commit the result.


## Running a server

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `app` | `rakaia` | `(scope: 'Scope', receive: 'Receive', send: 'Send') -> 'None'` | — |
| `create_app` | `rakaia` | `(store: 'StreamServerStore \| None' = None, options: 'ServerOptions \| None' = None) -> 'Any'` | Create a plain ASGI application implementing the Durable Streams protocol. |
| `get_asgi_app` | `django_rakaia` | `(options: rakaia.handler.ServerOptions \| None = None) -> object` | Get the Rakaia ASGI application configured with the store `RAKAIA_STORE` names. |
| `ServerOptions` | `rakaia` | `(long_poll_timeout: 'float' = 3.0, cursor_options: 'CursorOptions' = <factory>, enable_fault_injection: 'bool' = <factory>) -> None` | Configuration for the ASGI handler. |
| `get_store` | `django_rakaia` | `() -> Any` | Get the configured stream store. |

## Reading and writing streams

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `Stream` | `rakaia` | `(path: 'str', content_type: 'str \| None' = None, current_offset: 'str' = '0000000000000000_0000000000000000', last_seq: 'str \| None' = None, ttl_seconds: 'int \| None' = None, expires_at: 'str \| None' = None, created_at: 'float' = 0.0, last_activity_at: 'float' = 0.0, producers: 'dict[str, ProducerState]' = <factory>, closed: 'bool' = False, closed_by: 'ClosedBy \| None' = None) -> None` | Stream metadata. |
| `StreamStore` | `rakaia` | `() -> 'None'` | In-memory store for durable streams. |
| `DjangoStreamStore` | `django_rakaia` | `(*, using: 'str \| None' = None) -> 'None'` | A durable store backed by the django_rakaia ORM models. |
| `ReadableStore` | `rakaia` | `(*args, **kwargs)` | A store `replay()` can read events from. |
| `WritableStore` | `rakaia` | `(*args, **kwargs)` | A store the event-sourcing framework both writes to and reads from. |
| `StreamServerStore` | `rakaia` | `(*args, **kwargs)` | A store that can back a Durable Streams **protocol server**. |
| `append_event` | `django_rakaia` | `(store: 'WritableStore', stream_path: 'str', payload: 'dict[str, Any]', *, label: 'str', actor: 'Any' = None, event_ts: 'float \| None' = None) -> 'None'` | Append one enveloped event to ``stream_path``, creating the stream if absent. |
| `append_if_changed` | `rakaia` | `(store: 'Any', path: 'str', data: 'bytes', *, current: 'Any', options: 'Any' = None, snapshot_of: 'Callable[[dict[str, Any]], Any] \| None' = None) -> 'bool'` | Append `data` to `path` only if its snapshot differs from `current`. |
| `seed_stream` | `rakaia` | `(path: 'str', events: 'Iterable[SeedEvent]' = (), *, store: '_S \| None' = None, encoder: 'type[json.JSONEncoder] \| None' = None) -> '_S \| StreamStore'` | Create ``path`` and append ``events`` to it, in list order. |
| `create_stream_event` | `django_rakaia` | `(stream_paths: str \| list[str] \| collections.abc.Callable[[django.db.models.base.Model], str \| list[str]], to_dataclass: collections.abc.Callable[[django.db.models.base.Model], typing.Any], instance: django.db.models.base.Model, action: str, using: str \| None = None) -> django_rakaia.models.StreamEvent` | Create a stream event for the given model instance. |
| `AppendOptions` | `rakaia` | `(seq: 'str \| None' = None, content_type: 'str \| None' = None, producer_id: 'str \| None' = None, producer_epoch: 'int \| None' = None, producer_seq: 'int \| None' = None, close: 'bool' = False, label: 'str' = '', metadata: 'dict \| None' = None, event_ts: 'float \| None' = None) -> None` | Options for append operations. |
| `AppendResult` | `rakaia` | `(message: 'StreamMessage \| None' = None, producer_result: 'ProducerValidationResult \| None' = None, stream_closed: 'bool' = False) -> None` | Result of an append operation. |
| `CloseResult` | `rakaia` | `(final_offset: 'str' = '', already_closed: 'bool' = False, producer_result: 'ProducerValidationResult \| None' = None) -> None` | Result of a close operation. |
| `ClosedBy` | `rakaia` | `(producer_id: 'str', epoch: 'int', seq: 'int') -> None` | Tracks which producer tuple closed a stream (for idempotent close). |
| `StreamMessage` | `rakaia` | `(data: 'bytes', offset: 'str', timestamp: 'float', event_ts: 'float \| None' = None, label: 'str' = '', metadata: 'dict \| None' = None) -> None` | A single message in a stream. |
| `Poll` | `rakaia` | `(messages: 'list[StreamMessage]', cursor: 'str \| None', status: 'PollStatus') -> None` | The result of polling a stream from a cursor. |
| `PollStatus` | `rakaia` | — | — |
| `poll` | `rakaia` | `(store: 'CursorStore', path: 'str', cursor: 'str \| None') -> 'Poll'` | Read `path` forward from `cursor`, detecting a rewound log. |
| `poll_consumer` | `django_rakaia` | `(store: 'CursorStore', consumer_id: 'str', stream_path: 'str') -> 'Poll'` | Load this consumer's cursor and poll `stream_path` for the delta. |

## Rebuilding tables (replay)

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `replay` | `rakaia` | `(store: 'ReadableStore', stream_path: 'str', executor: 'Executor', *, handler_registry: 'HandlerRegistry \| None' = None, upcaster_registry: 'UpcasterRegistry \| None' = None, start_seq: 'int' = 0, end_seq: 'int \| None' = None, event_match: 'str \| None' = None, on_drift: 'OnDriftPolicy' = 'warn', reader: 'ProjectionReader \| None' = None) -> 'ReplayResult'` | Replay events in `stream_path` from `start_seq` (inclusive) to `end_seq` (exclusive) through the registered handlers, applying produced effects via `executor`. |
| `replay_stream` | `django_rakaia` | `(stream_path: 'str', *, executor: 'Executor \| None' = None, reader: 'ProjectionReader \| None' = None, handler_registry: 'HandlerRegistry \| None' = None, upcaster_registry: 'UpcasterRegistry \| None' = None, start_seq: 'int' = 0, end_seq: 'int \| None' = None, event_match: 'str \| None' = None, on_drift: 'OnDriftPolicy' = 'warn', store: 'ReadableStore \| None' = None) -> 'ReplayResult'` | Replay ``stream_path`` through the Django executor + reader by default. |
| `merge_replay` | `rakaia` | `(store: 'ReadableStore', stream_paths: 'list[str]', executor: 'Executor', *, order_key: 'str \| _EnvelopeTs' = 'ts', handler_registry: 'HandlerRegistry \| None' = None, upcaster_registry: 'UpcasterRegistry \| None' = None, event_match: 'str \| None' = None, on_drift: 'OnDriftPolicy' = 'warn', reader: 'ProjectionReader \| None' = None) -> 'ReplayResult'` | Replay several streams merged into one deterministic total order. |
| `ReplayResult` | `rakaia` | `(events_processed: 'int' = 0, effects_applied: 'int' = 0, external: 'list[ExternalEffect]' = <factory>, warnings: 'list[str]' = <factory>, drift_detected: 'list[str]' = <factory>) -> None` | Summary of a single replay() invocation. |
| `fold_events` | `django_rakaia` | `(events: 'Sequence[dict[str, Any]]', registry: 'HandlerRegistry', *, reader: 'ProjectionReader \| None' = None, executor: 'Any' = None, label: 'str' = 'import', scratch_path: 'str' = '_scratch/fold') -> 'None'` | Project ``events`` now, through the handlers a replay would use. |
| `project_latest` | `rakaia` | `(messages: 'Sequence[StreamMessage]', model_label: 'str', *, subject_of: 'Callable[[dict[str, Any]], Any]', defaults_of: 'Callable[[StreamMessage, dict[str, Any]], dict[str, Any]]', subject_field: 'str' = 'subject', tombstone_labels: 'Sequence[str]' = ('delete',)) -> 'list[Effect]'` | Project each subject's **latest** snapshot into one row — the current-state read model behind an event log. |

## Describing changes (effects)

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `Effect` | `rakaia` | — | — |
| `AnyEffect` | `rakaia` | — | — |
| `Upsert` | `rakaia` | `(model_label: 'str', lookup: 'dict[str, Any]', defaults: 'dict[str, Any] \| None' = None, produces: 'str \| None' = None) -> None` | Create the row matching ``lookup``, or update it in place — the replay-safe write. Re-applying converges to the same row. |
| `Update` | `rakaia` | `(model_label: 'str', lookup: 'dict[str, Any]', defaults: 'dict[str, Any] \| None' = None) -> None` | Update-if-exists: the rows matching ``lookup`` are updated in place and **never** inserted (a no-op when nothing matches or ``defaults`` is empty). |
| `Delete` | `rakaia` | `(model_label: 'str', lookup: 'dict[str, Any]', spare: 'Exclude \| SpareKeys \| None' = None) -> None` | Hard-delete the rows matching ``lookup``, minus any ``spare``. |
| `Retire` | `rakaia` | `(model_label: 'str', lookup: 'dict[str, Any]', patch: 'dict[str, Any]', spare: 'SpareKeys \| None' = None, transition: 'Transition \| None' = None) -> None` | Soft-delete: the rows matching ``lookup`` (minus ``spare``) are UPDATEd with ``patch`` instead of DELETEd, e.g. ``{"resolved_at": <event ts>}``. |
| `Exclude` | `rakaia` | `(lookup: 'dict[str, Any]') -> None` | Spare rows from a delete via a single flat lookup. |
| `RowEffect` | `rakaia` | `(model_label: 'str', lookup: 'dict[str, Any]') -> None` | The row identity every database effect shares. |
| `ExternalEffect` | `rakaia` | `(kind: 'str', payload: 'dict[str, Any]') -> None` | An application-level side effect: an email, a webhook, a payment. |
| `Ref` | `rakaia` | `(produces: 'str', field: 'str' = 'pk') -> None` | A batch-local reference to a row a *sibling* effect materialises. |
| `RefResolver` | `rakaia` | `() -> 'None'` | Resolves `Ref` placeholders against rows produced earlier in one batch. |
| `UnresolvedRefError` | `rakaia` | — | A `Ref` names a `produces` id no earlier effect in the batch produced (a forward reference or a typo). |
| `Transition` | `rakaia` | `(kind: 'str', key_fields: 'tuple[str, ...]') -> None` | A retire's opt-in request for per-flip notifications. |
| `TouchedSubject` | `rakaia` | `(model_label: 'str', lookup: 'dict[str, Any]') -> None` | One subject a replay pass's per-event handlers wrote: the target model and the ``lookup`` identifying the affected row(s), taken straight from the applied Effect. |

## Registering rules (handlers, reducers, upcasters)

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `register_handler` | `rakaia` | `(name: 'str', event_match: 'str \| Iterable[str]', effective_from: 'int', effective_to: 'int \| None' = None, *, match_field: 'str \| None' = None, stage: 'int' = 0, registry: 'HandlerRegistry \| None' = None) -> 'Callable[[Callable[..., Any]], Callable[..., Any]]'` | Decorator that registers a handler version with the default registry. |
| `register_reducer` | `rakaia` | `(name: 'str', stage: 'int', *, registry: 'HandlerRegistry \| None' = None) -> 'Callable[[Callable[..., Any]], Callable[..., Any]]'` | Decorator that registers a per-stage reduce step with the default registry. |
| `register_simple` | `rakaia` | `(name: 'str', event_match: 'str \| Iterable[str]', *, match_field: 'str \| None' = None, stage: 'int' = 0, registry: 'HandlerRegistry \| None' = None) -> 'Callable[[Callable[..., Any]], Callable[..., Any]]'` | Register an always-on handler — the common "just project" case. |
| `register_upcaster` | `rakaia` | `(event_match: 'str', from_version: 'int', *, match_field: 'str \| None' = None, registry: 'UpcasterRegistry \| None' = None) -> 'Callable[[Callable[[dict[str, Any]], dict[str, Any]]], Callable[[dict[str, Any]], dict[str, Any]]]'` | Decorator that registers an upcaster from `from_version` to `from_version+1`. |
| `upcast` | `rakaia` | `(event: 'dict[str, Any]', event_match: 'str', *, registry: 'UpcasterRegistry \| None' = None) -> 'dict[str, Any]'` | Normalise `event` to the current schema for `event_match`. |
| `HandlerRegistry` | `rakaia` | `(store: 'StreamStore \| None' = None, *, stream_path: 'str' = '__rakaia__:handlers') -> 'None'` | Registry of versioned handlers, optionally backed by a meta-stream. |
| `UpcasterRegistry` | `rakaia` | `(store: 'StreamStore \| None' = None, *, stream_path: 'str' = '__rakaia__:upcasters') -> 'None'` | Registry of schema-version upcasters, keyed by (event_match, from_version, match_field). |
| `HandlerVersion` | `rakaia` | `(name: 'str', event_match: 'str \| frozenset[str]', effective_from: 'int', effective_to: 'int \| None', fn: 'Callable[..., Any]', dotted_path: 'str', source_hash: 'str', match_field: 'str \| None' = None, stage: 'int' = 0, registered_in: 'str \| None' = None) -> None` | One registered version of a handler. |
| `ReducerVersion` | `rakaia` | `(name: 'str', stage: 'int', fn: 'Callable[..., Any]', dotted_path: 'str', source_hash: 'str', registered_in: 'str \| None' = None) -> None` | A per-stage reduce step: recompute an aggregate once from the projections. |
| `UpcasterVersion` | `rakaia` | `(event_match: 'str', from_version: 'int', fn: 'Callable[[dict[str, Any]], dict[str, Any]]', dotted_path: 'str', source_hash: 'str', match_field: 'str \| None' = None, registered_in: 'str \| None' = None) -> None` | One registered upcaster: transforms event from `from_version` to next. |
| `get_default_registry` | `rakaia` | `() -> 'HandlerRegistry'` | Return the process-wide default handler registry. |
| `get_default_upcaster_registry` | `rakaia` | `() -> 'UpcasterRegistry'` | Return the process-wide default upcaster registry. |
| `reset_default_registries` | `rakaia` | `() -> 'None'` | Reset both process-wide default registries (handlers + upcasters). |
| `check_disjoint_defaults` | `rakaia` | `(effects: 'Iterable[Effect]') -> 'None'` | Raise EffectCollisionError if two write effects targeting the same (model_label, lookup) row share any key in their `defaults`. |

## Applying changes (executors and readers)

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `Executor` | `rakaia` | `(*args, **kwargs)` | Applies a batch of effects to durable storage. |
| `CollectingExecutor` | `rakaia` | `() -> 'None'` | An Executor that records effects instead of applying them. |
| `DjangoExecutor` | `django_rakaia` | `(*, skip_unchanged: 'bool' = False, using: 'str \| None' = None, normalizers: 'Sequence[Normalizer] \| None' = None, batch_updates: 'bool' = False) -> 'None'` | Apply Effects via Django's ORM. |
| `InMemoryProjections` | `rakaia` | `() -> 'None'` | An in-memory `Executor` **and** `ProjectionReader` over dict-backed tables. |
| `ProjectionReader` | `rakaia` | `(*args, **kwargs)` | Read-only view over materialised projections. |
| `DjangoProjectionReader` | `django_rakaia` | `(*, using: 'str \| None' = None) -> 'None'` | Read-only projection accessor over `apps.get_model(...).objects`. |
| `PreloadedProjectionReader` | `django_rakaia` | `(effects: 'Iterable[Effect]', *, using: 'str \| None' = None) -> 'None'` | A :class:`DjangoProjectionReader` that bulk-fetches, up front, the rows a batch of effects will look up — so each :meth:`get` serves from an in-memory snapshot instead of issuing one ``SELECT``… |
| `ModelStreamReader` | `django_rakaia` | `(*, queryset_for: 'Callable[[str], QuerySet[Any]]', order_by: 'str', to_payload: 'Callable[[Any], dict[str, Any]]', chunk_size: 'int' = 1000) -> 'None'` | A read-only adapter that satisfies the subset of the `StreamStore` interface that `rakaia.replay.replay` uses. |

## Rehearsing a rebuild safely

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `rebuild_and_verify` | `django_rakaia` | `(stream_path: 'str', *, into: 'str', live_models: 'Sequence[type[Model]]', source: 'ReadableStore \| None' = None, live_using: 'str' = 'default', registry: 'HandlerRegistry \| None' = None, upcaster_registry: 'UpcasterRegistry \| None' = None, normalizers: 'Sequence[Normalizer] \| None' = None, event_match: 'str \| None' = None, on_drift: 'OnDriftPolicy' = 'warn') -> 'DiffReport'` | Rebuild ``stream_path`` into ``into`` under both guards and diff the result against the live rows. |
| `GuardNotArmed` | `django_rakaia` | — | The read guard did not fire on a deliberate ambient query, so a green verdict from this run would be unsupported. |
| `ScratchAliasNotEmpty` | `django_rakaia` | — | The ``into`` alias already holds rows for a model being rebuilt. |
| `deny_database_access` | `django_rakaia` | `(*aliases: 'str') -> 'Iterator[None]'` | Raise :class:`AmbientDatabaseAccess` on any query to ``aliases`` in the block. |
| `assert_no_live_writes` | `django_rakaia` | `(*models: 'type[Model]', using: 'str' = 'default') -> 'Iterator[None]'` | Assert the ``using`` row counts of ``models`` are unchanged across the block. |
| `AmbientDatabaseAccess` | `django_rakaia` | — | A guarded connection alias was queried inside ``deny_database_access`` — a handler (or a helper it calls) read the database directly instead of through the injected reader. |
| `LiveWriteLeaked` | `django_rakaia` | — | A guarded model's row count changed inside ``assert_no_live_writes`` — a rebuild mutated the live database it was only supposed to reconstruct. |
| `diff_effects_against_rows` | `django_rakaia` | `(effects: 'Iterable[Effect]', *, reader: 'DjangoProjectionReader \| None' = None, preload: 'bool' = False, using: 'str \| None' = None, normalizers: 'Sequence[Normalizer] \| None' = None, kinds: 'tuple[type, ...]' = (<class 'rakaia.effects.Upsert'>, <class 'rakaia.effects.Update'>)) -> 'DiffReport'` | Diff each write effect's ``defaults`` against its live projection row. |
| `DiffReport` | `django_rakaia` | `(rows: 'list[RowDiff]') -> None` | Aggregate result of :func:`diff_effects_against_rows`. |
| `RowDiff` | `django_rakaia` | `(model_label: 'str', lookup: 'dict[str, Any]', missing: 'bool', field_diffs: 'list[FieldDiff]' = <factory>) -> None` | The verification outcome for one write effect's target row. |
| `FieldDiff` | `django_rakaia` | `(field: 'str', expected: 'Any', actual: 'Any') -> None` | One field whose stored value disagrees with the effect's ``defaults``. |
| `snapshots_equal` | `rakaia` | `(a: 'Any', b: 'Any') -> 'bool'` | Whether two decoded snapshots are equal. |
| `GREEN` | `django_rakaia` | — | — |
| `RED` | `django_rakaia` | — | — |
| `VACUOUS` | `django_rakaia` | — | — |
| `VacuousVerification` | `django_rakaia` | `(report: 'DiffReport') -> 'None'` | Raised by :meth:`DiffReport.raise_if_diff` when nothing was compared. |
| `VerificationError` | `django_rakaia` | `(report: 'DiffReport') -> 'None'` | Raised by :meth:`DiffReport.raise_if_diff` when a projection disagrees. |
| `PreloadMismatch` | `django_rakaia` | — | A :class:`PreloadedProjectionReader` was handed to :func:`diff_effects_against_rows` together with effects its bulk fetch does not cover. |

## Audit trails and provenance

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `provenance` | `rakaia` | `(**fields: 'Any') -> 'Iterator[None]'` | Set ambient envelope metadata for appends within this block. |
| `get_provenance` | `rakaia` | `() -> 'dict[str, Any]'` | The current ambient provenance (a copy; empty dict if none is set). |
| `ProvenanceMiddleware` | `django_rakaia` | `(get_response: 'Callable[[Any], Any]') -> 'None'` | Stamp the acting user + request path onto envelope metadata per request. |
| `envelope_actor` | `rakaia` | `(msg: 'StreamMessage', event: 'dict[str, Any]', *, owner_key: 'str' = 'user_id') -> 'Any'` | The acting user: the envelope's ``metadata['user']`` (the editor), falling back to the payload's own owner FK (``event[owner_key]``) when there is no request-context actor (bulk import, management… |
| `label_marker` | `rakaia` | `(label: 'str') -> 'str'` | Map an envelope label to the `/history` diff marker `+` / `~` / `-`. |
| `materialize_history` | `django_rakaia` | `(store: 'Any', path: 'str', model_label: 'str', *, subject_of: 'Callable[[dict[str, Any]], Any]', defaults_of: 'Callable[[Any, dict[str, Any]], dict[str, Any]]', subject_field: 'str' = 'subject', version_field: 'str' = 'version', version_of: 'Callable[[Any], Any] \| None' = None, executor: 'Any \| None' = None) -> 'list[Effect]'` | Read `path` and materialise its `/history` audit rows into `model_label`. |
| `history_effects` | `rakaia` | `(messages: 'Sequence[StreamMessage]', model_label: 'str', *, subject_of: 'Callable[[dict[str, Any]], Any]', defaults_of: 'Callable[[StreamMessage, dict[str, Any]], dict[str, Any]]', subject_field: 'str' = 'subject', version_field: 'str' = 'version', version_of: 'Callable[[StreamMessage], Any] \| None' = None) -> 'list[Effect]'` | One idempotent audit-row upsert per event in `messages`. |
| `ENVELOPE_TS` | `rakaia` | — | — |

## Keeping child rows in step

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `reconcile_children` | `rakaia` | `(model_label: 'str', parent_lookup: 'dict[str, Any]', child_key: 'str', items: 'Sequence[Any]', defaults_fn: 'Callable[[Any], dict[str, Any]]') -> 'list[Effect]'` | Return Effects that materialise `items` as child rows without orphans. |
| `reconcile_tree` | `rakaia` | `(model_label: 'str', scope_lookup: 'dict[str, Any]', node_key: 'str', nodes: 'Sequence[Any]', id_fn: 'Callable[[Any], Any]', defaults_fn: 'Callable[[Any], dict[str, Any]]') -> 'list[Effect]'` | Materialise an unbounded nested tree without orphans at any depth. |
| `reconcile_aggregate` | `rakaia` | `(model_label: 'str', scope_lookup: 'dict[str, Any]', group_key: 'str', groups: 'Mapping[Any, dict[str, Any]]', *, owns: 'Sequence[str] \| None' = None, retire_filter: 'dict[str, Any] \| None' = None, allow_full_clear: 'bool' = False) -> 'list[Effect]'` | Materialise one recomputed aggregate row per group, without stale rows. |
| `reconcile_by_key` | `rakaia` | `(model_label: 'str', scope: 'dict[str, Any]', key_fields: 'tuple[str, ...]', items: 'Sequence[Any]', key_fn: 'Callable[[Any], dict[str, Any]]', defaults_fn: 'Callable[[Any], dict[str, Any]]', *, retire_filter: 'dict[str, Any] \| None' = None, retire: "Literal['delete'] \| dict[str, Any]" = 'delete', transition_kind: 'str \| None' = None) -> 'list[Effect]'` | Reconcile a set of rows keyed by a *composite natural key*. |

## Consumer cursors

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `CursorStore` | `rakaia` | `(*args, **kwargs)` | A `ReadableStore` that also exposes its current head offset — what a subscriber (`poll()`) needs to detect new messages and a rewound log. |
| `CursorOptions` | `rakaia` | `(interval_seconds: 'int' = 20, epoch: 'float' = 1728432000.0) -> None` | Configuration for cursor calculation. |
| `calculate_cursor` | `rakaia` | `(options: 'CursorOptions \| None' = None) -> 'str'` | Calculate the current cursor value based on time intervals. |
| `generate_response_cursor` | `rakaia` | `(client_cursor: 'str \| None', options: 'CursorOptions \| None' = None) -> 'str'` | Generate a cursor for a response, ensuring monotonic progression. |
| `commit_cursor` | `django_rakaia` | `(consumer_id: 'str', stream_path: 'str', offset: 'str') -> 'None'` | Persist `offset` as the consumer's watermark for `stream_path`. |
| `load_cursor` | `django_rakaia` | `(consumer_id: 'str', stream_path: 'str') -> 'str \| None'` | The consumer's last committed offset for `stream_path`, or None. |

## Producer fencing

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `ProducerState` | `rakaia` | `(epoch: 'int', last_seq: 'int', last_updated: 'float') -> None` | Producer state for idempotent writes. Tracks epoch and sequence number per producer ID for deduplication. |
| `ProducerAccepted` | `rakaia` | `(status: "Literal['accepted']" = 'accepted', is_new: 'bool' = False, producer_id: 'str' = '', proposed_state: 'ProducerState \| None' = None) -> None` | Producer validation: append accepted. |
| `ProducerDuplicate` | `rakaia` | `(status: "Literal['duplicate']" = 'duplicate', last_seq: 'int' = 0) -> None` | Producer validation: duplicate append (idempotent success). |
| `ProducerStaleEpoch` | `rakaia` | `(status: "Literal['stale_epoch']" = 'stale_epoch', current_epoch: 'int' = 0) -> None` | Producer validation: stale epoch (zombie fencing). |
| `ProducerSequenceGap` | `rakaia` | `(status: "Literal['sequence_gap']" = 'sequence_gap', expected_seq: 'int' = 0, received_seq: 'int' = 0) -> None` | Producer validation: sequence gap detected. |
| `ProducerInvalidEpochSeq` | `rakaia` | `(status: "Literal['invalid_epoch_seq']" = 'invalid_epoch_seq') -> None` | Producer validation: new epoch must start at seq=0. |
| `ProducerStreamClosed` | `rakaia` | `(status: "Literal['stream_closed']" = 'stream_closed') -> None` | Producer validation: stream is already closed. |
| `ProducerValidationResult` | `rakaia` | — | — |

## Django model integration

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `stream_model` | `django_rakaia` | `(stream_paths: str \| collections.abc.Callable[[django.db.models.base.Model], str] \| list[str] \| collections.abc.Callable[[django.db.models.base.Model], list[str]], to_dataclass: collections.abc.Callable[[django.db.models.base.Model], typing.Any], on_delete: Optional[Literal['delete', 'update']] = 'delete', delete_to_dataclass: collections.abc.Callable[[django.db.models.base.Model], typing.Any] \| None = None) -> collections.abc.Callable[[type[django.db.models.base.Model]], type[django.db.models.base.Model]]` | Decorator to automatically stream Django model changes to stream events. |
| `register_stream_event_admin` | `django_rakaia` | `(event_model_class)` | Register a concrete StreamEvent subclass with the admin. |
| `canonical_value` | `django_rakaia` | `(model: 'type', field_name: 'str', value: 'Any', normalizers: 'tuple[Normalizer, ...]' = (<function normalize_uuid>, <function normalize_decimal>, <function normalize_temporal>)) -> 'Any'` | Coerce ``value`` into the comparable form the column stores. |
| `DEFAULT_NORMALIZERS` | `django_rakaia` | — | — |
| `Normalizer` | `django_rakaia` | — | — |

## Constants

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `__version__` | `rakaia` | — | — |
| `HANDLERS_META_STREAM` | `rakaia` | — | — |
| `REDUCERS_META_STREAM` | `rakaia` | — | — |
| `UPCASTERS_META_STREAM` | `rakaia` | — | — |
| `SCRATCH_PATH` | `django_rakaia` | — | — |

## Errors

| Name | Import from | Signature | What it does |
|---|---|---|---|
| `StreamError` | `rakaia` | — | Base for store failures a protocol server maps to a status. |
| `StreamNotFound` | `rakaia` | — | The stream does not exist, or has expired. |
| `SequenceConflict` | `rakaia` | — | An append's `Stream-Seq` is not above the stream's last seq. |
| `ContentTypeMismatch` | `rakaia` | — | An append's content type disagrees with the stream's. |
| `InvalidJson` | `rakaia` | — | A JSON-mode payload did not parse. |
| `InvalidOffset` | `rakaia` | — | An offset is syntactically valid but not one this store can read. |
| `ForeignOffset` | `rakaia` | — | An offset was used where its format does not belong — passed to a store that did not issue it, or compared against one from another store. |
| `EmptyJsonArray` | `rakaia` | — | A JSON-mode append carried an empty array. |
| `SpareKeys` | `rakaia` | `(keys: 'list[dict[str, Any]]') -> None` | Spare rows from a delete or retire by composite natural key. |
| `StreamConfigConflict` | `rakaia` | — | A create names an existing stream with a different configuration. |
| `HandlerDriftError` | `rakaia` | — | A handler/upcaster's source body differs from its registered hash. |
| `HandlerGapError` | `rakaia` | — | No registered version of a handler covers the requested sequence. |
| `HandlerOverlapError` | `rakaia` | — | Two versions of the same handler claim overlapping sequence ranges. |
| `EffectCollisionError` | `rakaia` | — | Two sibling effects target the same (model_label, lookup) row with overlapping keys in `defaults`, violating the disjoint-defaults invariant. |
| `DuplicateProducesError` | `rakaia` | — | Two effects in one batch declare the same `produces` id. The id would silently bind to the second producer's row, orphaning the first — always a bug, so it is rejected rather than resolved to the… |
| `UpcasterChainError` | `rakaia` | — | Cannot upcast: missing or ambiguous link in the upcaster chain. |
| `UpcasterConflictError` | `rakaia` | — | Two upcasters were registered for the same (event_match, from_version). |

---

## Appendix — coverage

136 exported names across 14 sections. 120 carry a docstring; 16 do not and show `—` above.

Undocumented: `AnyEffect`, `DEFAULT_NORMALIZERS`, `ENVELOPE_TS`, `Effect`, `GREEN`, `HANDLERS_META_STREAM`, `Normalizer`, `PollStatus`, `ProducerValidationResult`, `RED`, `REDUCERS_META_STREAM`, `SCRATCH_PATH`, `UPCASTERS_META_STREAM`, `VACUOUS`, `__version__`, `app`.
