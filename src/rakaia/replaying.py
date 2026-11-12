"""
Replay orchestrator: re-derive materialized state from a stream by running
versioned handlers and an effect executor.

This module is `rakaia.replaying`, not `rakaia.replay`, so that the package
attribute `rakaia.replay` is only ever the function (#161).

Per event the pipeline is:
    1. Load the raw event bytes from the stream
    2. Decode as JSON, upcast to current schema via UpcasterRegistry
    3. Resolve handlers via HandlerRegistry.resolve(event_match, seq, event, stage)
    4. Call each handler -> collect effects
    5. Split off the ExternalEffects into ReplayResult.external
    6. executor.apply(the database effects)

When every handler is stage 0 (the default), replay is a single streaming pass
over the range — one event decoded and applied at a time. When handlers span
more than one stage, replay decodes the range once and runs the pipeline as one
pass per stage in ascending order: the whole range through stage 0, then stage
1, and so on, so a stage > 0 handler (called `fn(event, reader)`) can read the
projections earlier stages committed. A stage may also declare **reducers**
(`register_reducer(name, stage, fn)`) that run once, after that stage's per-event
handlers, to recompute an aggregate from the committed projections via the
reader. A reducer declaring a second parameter (`fn(reader, touched)`) also
receives the `TouchedSubject`s the pass's handlers wrote, so one reducer can
scope its recompute to what changed (incremental) or to everything (full
rebuild). See `register_handler(stage=...)` and `register_reducer(...)`.

**How far a replay gets before failing** depends on how much it decodes ahead:

* a single unstaged pass over one stream decodes one event at a time, so the
  events *before* a malformed one are dispatched and applied;
* a staged replay decodes the whole range up front and applies nothing;
* `merge_replay` cannot know the merged order until every stream is read, so it
  applies nothing, staged or not. See `EventSource`.

Re-running the same range twice produces identical materialized state because
every database effect is idempotent (an `Upsert` converges to the same row).
"""

from __future__ import annotations

import inspect
import json
from collections.abc import Iterable, Iterator
from dataclasses import dataclass, field
from typing import Any

from .batching import _drain, _StageBuffer
from .drift import DriftLedger

# `HandlerDriftError` and `OnDriftPolicy` live in `rakaia.drift` and are
# re-exported here because callers import them from this module.
from .drift import HandlerDriftError as HandlerDriftError
from .drift import OnDriftPolicy as OnDriftPolicy
from .effects import (
    AnyEffect,
    ApplyReport,
    Delete,
    Effect,
    Executor,
    ExternalEffect,
    Retire,
    Update,
    Upsert,
    transition_payload,
)
from .errors import RakaiaError
from .protocols import ProjectionReader, ReadableStore
from .registry import (
    HandlerRegistry,
    HandlerVersion,
    UpcasterRegistry,
    get_default_registry,
    get_default_upcaster_registry,
)

# =============================================================================
# Errors
# =============================================================================
#
# Each also subclasses `ValueError`, so a caller already catching `ValueError`
# around a merge or a decode keeps catching it.


class MissingReaderError(RakaiaError, ValueError):
    """A staged replay was asked to run with no projection reader."""

    code = "missing_reader"


class UndecodableEventError(RakaiaError, ValueError):
    """An event's stored bytes are not decodable JSON."""

    code = "undecodable_event"


class MergeKeyError(RakaiaError, ValueError):
    """A merge cannot order its events: the order key is missing from an event,
    or its values are not mutually comparable across them."""

    code = "merge_key"


class _EnvelopeTs:
    """Sentinel `order_key` selecting the first-class envelope timestamp."""

    __slots__ = ()

    def __repr__(self) -> str:
        return "ENVELOPE_TS"


ENVELOPE_TS = _EnvelopeTs()
"""Pass as ``merge_replay(order_key=ENVELOPE_TS)`` to merge on the **envelope**
timestamp (`StreamMessage.event_ts` — the producer's logical event time,
defaulting to append time) rather than a field read out of the payload body.
This is the first-class, unambiguous merge key (see the ADR-0002 determinism
contract); a plain string `order_key` still reads the decoded payload."""


@dataclass
class ReplayResult:
    """Summary of a single replay() invocation."""

    events_processed: int = 0
    effects_applied: int = 0
    external: list[ExternalEffect] = field(default_factory=list)
    """The application-level effects this replay produced, in the order the
    handlers emitted them (plus one per row a notifying retire flipped).

    They are **returned, not applied** — no executor ever sees an external
    effect. A rebuild therefore never re-sends an email by accident, and a
    caller that *does* want them delivered walks this list itself."""
    drift: DriftLedger = field(default_factory=DriftLedger)
    """The drift ledger this replay checked its handlers, reducers and upcasters
    against — the one object that knows what drifted, what it has already
    reported, and what it hashed."""

    @property
    def warnings(self) -> list[str]:
        """The warnings this replay emitted, read from the ledger."""
        return self.drift.warnings

    @property
    def drift_detected(self) -> list[str]:
        """Names of the drifted rules, deduplicated, in the order first seen."""
        return self.drift.drifted


@dataclass(frozen=True)
class TouchedSubject:
    """One subject a replay pass's per-event handlers wrote: the target model
    and the ``lookup`` identifying the affected row(s), taken straight from the
    applied Effect.

    A staged-replay reducer that declares a second parameter receives the
    deterministic, deduplicated tuple of these — the subjects changed *in this
    pass* — so it can scope its recompute (e.g. only the touched groups on an
    incremental forward replay, everything on a full rebuild). A one-argument
    ``fn(reader)`` reducer is unaffected. See `register_reducer`.
    """

    model_label: str
    lookup: dict[str, Any]


@dataclass
class _ReplayCtx:
    """Bundle of the invariants the pipeline helpers need, shared by `replay()`
    (single stream) and `merge_replay()` (several streams merged)."""

    handlers: HandlerRegistry
    executor: Executor
    reader: ProjectionReader | None
    result: ReplayResult
    # Only accumulate touched subjects when a reducer opts in (a second parameter).
    track_touched: bool = False
    touched: list[TouchedSubject] = field(default_factory=list)
    _touched_seen: set = field(default_factory=set)
    # Set for a pass whose handlers take no reader, so its effects can be
    # batched; None applies per event. See `_StageBuffer`.
    buffer: _StageBuffer | None = None
    upcasters: Any = None

    @property
    def drift(self) -> DriftLedger:
        """The ledger owning every drift question this replay asks: the policy,
        the warn-once memory and the hash memo. Scoped to the replay, so a
        long-lived process that reloads code between replays still detects the
        drift that reload introduced."""
        return self.result.drift


#: One event ready for dispatch: its sequence, the routing subject to match
#: handlers against, and the decoded + upcasted payload. Whether the seq is an
#: offset in one stream or a position in a merged order is the source's business.
PipelineEvent = tuple[int, str, dict]

#: Every event a replay will dispatch, in dispatch order.
#:
#: An `Iterable`, not a `Sequence`, because how much of the range a replay
#: decodes before the first handler runs is observable. A source consumed one
#: event at a time leaves the events before a malformed one already applied; a
#: source materialised in full applies nothing. `replay()` yields a generator;
#: `merge_replay()` has no choice but a list, because the merged order is not
#: known until every stream has been read.
EventSource = Iterable[PipelineEvent]


def build_pipeline(
    *,
    handler_registry: HandlerRegistry | None,
    upcaster_registry: UpcasterRegistry | None,
    executor: Executor,
    reader: ProjectionReader | None,
    on_drift: OnDriftPolicy,
) -> _ReplayCtx:
    """Assemble everything a replay needs that does not depend on its event source.

    Shared by `replay()` and `merge_replay()`. The returned context carries its
    own `upcasters` and `drift` ledger so a source can decode and upcast events
    without re-deriving either.
    """
    handlers = handler_registry or get_default_registry()

    return _ReplayCtx(
        handlers=handlers,
        executor=executor,
        reader=reader,
        result=ReplayResult(drift=DriftLedger(on_drift=on_drift)),
        track_touched=_wants_touched_reducer(handlers),
        upcasters=upcaster_registry or get_default_upcaster_registry(),
    )


def is_staged(ctx: _ReplayCtx) -> bool:
    """Whether this replay needs more than one pass over the events.

    True when any handler declares a stage above 0, or any reducer is
    registered — a reducer runs once after its stage's per-event handlers, which
    is a second pass even with only stage-0 handlers.
    """
    return any(s > 0 for s in ctx.handlers.stages()) or ctx.handlers.has_reducers()


def require_reader(ctx: _ReplayCtx, what: str = "Replay") -> None:
    """Refuse a staged replay with no reader, naming the caller.

    A stage > 0 handler is called `fn(event, reader)` and a reducer `fn(reader)`,
    so without one they cannot resolve what earlier stages materialised. Failing
    here beats failing inside the first handler that dereferences `None`.
    """
    if is_staged(ctx) and ctx.reader is None:
        raise MissingReaderError(
            f"{what} has stage > 0 handlers or reducers but no reader was "
            f"provided; pass reader= so they can read earlier stages' projections."
        )


def run_passes(
    ctx: _ReplayCtx, events: EventSource, *, what: str = "Replay"
) -> ReplayResult:
    """Run `events` through every stage, in order, and return the result.

    One pass per stage in ascending order: that stage's per-event handlers over
    **every** event, then its reducers once. An unstaged replay is the same shape
    with a single `None` pass (`_run_stage_reducers(ctx, None)` is a no-op when
    nothing is registered).

    **Decodes ahead only as far as it must** (see `EventSource`). One pass
    consumes the source as it arrives, so a lazy source dispatches everything up
    to a malformed event before failing. A staged replay has to see every event
    before the second pass starts, so it materialises the source first and a
    malformed event anywhere applies nothing. `events_processed` counts each event
    once however many stages ran.

    The events are already decoded and upcasted; what differs between `replay()`
    and `merge_replay()` happens before this, in the source.
    """
    require_reader(ctx, what)
    staged = is_staged(ctx)
    source: EventSource = list(events) if staged else events
    passes: list[int | None] = list(ctx.handlers.stages()) if staged else [None]
    for stage in passes:
        # Stage 0 handlers get no reader, so nothing in the pass can observe a
        # write that has not landed and its effects can be batched. A stage > 0
        # handler is handed a reader that goes straight to storage, so those
        # passes apply per event: a buffered write would be silently invisible.
        ctx.buffer = _make_buffer(ctx) if stage in (None, 0) else None
        dispatched = 0
        try:
            for seq, match_str, event in source:
                _dispatch_event(ctx, seq, match_str, event, stage)
                dispatched += 1
        finally:
            # In a `finally` so a lazy source that raises partway still leaves
            # the events before it applied (see `EventSource`).
            _drain(ctx)
        ctx.result.events_processed = dispatched
        # Reducers read the projections, so the `_drain` above must come first.
        _run_stage_reducers(ctx, stage)
    return ctx.result


def _reducer_wants_touched(fn: Any) -> bool:
    """Whether a reducer opts in to the touched-subjects arg by declaring a
    second positional parameter. A plain ``fn(reader)`` returns False.
    """
    try:
        sig = inspect.signature(fn)
    except (TypeError, ValueError):
        return False
    positional = 0
    for p in sig.parameters.values():
        if p.kind is inspect.Parameter.VAR_POSITIONAL:
            return True
        if p.kind in (
            inspect.Parameter.POSITIONAL_ONLY,
            inspect.Parameter.POSITIONAL_OR_KEYWORD,
        ):
            positional += 1
    return positional >= 2


def _wants_touched_reducer(handlers: HandlerRegistry) -> bool:
    """Whether any registered reducer opts in, which decides if we track."""
    return handlers.has_reducers() and any(
        _reducer_wants_touched(r.fn) for r in handlers.all_reducers()
    )


def _record_touched(ctx: _ReplayCtx, effects: list[Effect]) -> None:
    # A subject is a database effect's (model_label, lookup). Dedup in
    # first-seen order so the set is a pure function of the events; an unhashable
    # lookup value skips dedup.
    for e in effects:
        try:
            key = (e.model_label, tuple(sorted(e.lookup.items())))
        except TypeError:
            key = None
        if key is not None:
            if key in ctx._touched_seen:
                continue
            ctx._touched_seen.add(key)
        ctx.touched.append(
            TouchedSubject(model_label=e.model_label, lookup=dict(e.lookup))
        )


def _synth_transitions(report: ApplyReport | None) -> list[ExternalEffect]:
    # One external transition per row a retire actually flipped. `report` is
    # typed `ApplyReport | None`, not `object`, so renaming `retire_flips` is a
    # type error rather than a silent "synthesise nothing".
    if report is None:
        return []
    out: list[ExternalEffect] = []
    for eff, rows in report.retire_flips:
        if eff.transition is None:
            continue
        patch = eff.patch or {}
        # Bound here because the `is None` check narrows `eff.transition` in
        # this scope only, not inside the generator.
        kind = eff.transition.kind
        out.extend(
            ExternalEffect(kind=kind, payload=transition_payload(patch, identity))
            for identity in rows
        )
    return out


def _make_buffer(ctx: _ReplayCtx) -> _StageBuffer:
    """A buffer for one pass, wired to this replay's executor and result.

    The buffer owns the ``apply()`` call, so the transitions a batch's retires
    flipped come back through the sink, which keeps the external list in
    handler-emission order.
    """
    return _StageBuffer(
        ctx.executor,
        lambda report: ctx.result.external.extend(_synth_transitions(report)),
    )


def _apply_effects(ctx: _ReplayCtx, effects: list[AnyEffect]) -> None:
    """Route a batch: external effects to the result, the rest to the executor.

    The split is by type, so no executor needs an external branch.
    """
    to_apply: list[Effect] = []
    for e in effects:
        if isinstance(e, ExternalEffect):
            ctx.result.external.append(e)
        else:
            to_apply.append(e)
    if not to_apply:
        return
    ctx.result.effects_applied += len(to_apply)
    if ctx.buffer is not None:
        # Counted above, not at flush time, so the number is the same whether
        # or not this pass batches.
        ctx.buffer.add(to_apply)
        return
    report = ctx.executor.apply(to_apply)
    ctx.result.external.extend(_synth_transitions(report))


def _dispatch_event(
    ctx: _ReplayCtx, seq: int, match_str: str, upcasted: dict, stage: int | None
) -> None:
    versions = ctx.handlers.resolve(match_str, seq, event=upcasted, stage=stage)
    all_effects: list[AnyEffect] = []
    for version in versions:
        ctx.drift.check(
            kind="handler",
            name=version.name,
            stored_hash=version.hash_to_check,
            fn=version.fn,
        )
        all_effects.extend(_call_handler(version, upcasted, ctx.reader))
    if ctx.track_touched:
        _record_touched(
            ctx, [e for e in all_effects if not isinstance(e, ExternalEffect)]
        )
    _apply_effects(ctx, all_effects)


def _run_stage_reducers(ctx: _ReplayCtx, stage: int | None) -> None:
    # Reducers run once per stage, after its per-event handlers have committed.
    # A reducer declaring a second parameter also receives the subjects touched
    # so far (cumulative across stages); reducer *outputs* are not recorded as
    # touched.
    for reducer in ctx.handlers.reducers_for_stage(stage):
        ctx.drift.check(
            kind="reducer",
            name=reducer.name,
            stored_hash=reducer.source_hash,
            fn=reducer.fn,
        )
        if _reducer_wants_touched(reducer.fn):
            out = reducer.fn(ctx.reader, tuple(ctx.touched))
        else:
            out = reducer.fn(ctx.reader)
        _apply_effects(ctx, _normalize_effects(out))


def replay(
    store: ReadableStore,
    stream_path: str,
    executor: Executor,
    *,
    handler_registry: HandlerRegistry | None = None,
    upcaster_registry: UpcasterRegistry | None = None,
    start_seq: int = 0,
    end_seq: int | None = None,
    event_match: str | None = None,
    on_drift: OnDriftPolicy = "warn",
    reader: ProjectionReader | None = None,
) -> ReplayResult:
    """
    Replay events in `stream_path` from `start_seq` (inclusive) to `end_seq`
    (exclusive) through the registered handlers, applying produced effects
    via `executor`.

    Args:
        store: The store holding the events (in-memory or durable).
        stream_path: Path of the event stream to replay.
        executor: Effect executor (e.g. DjangoExecutor).
        handler_registry: HandlerRegistry to dispatch through; defaults to the
            process-wide registry.
        upcaster_registry: UpcasterRegistry to apply; defaults to the
            process-wide registry.
        start_seq: First event index to replay (0-based, inclusive).
        end_seq: One past the last event index to replay (None = to head).
        event_match: Match string passed to the registries; defaults to
            stream_path.
        on_drift: 'warn' (default) emits a warning and continues if a
            handler's, reducer's or upcaster's source has changed since
            registration; 'raise' raises HandlerDriftError.
        reader: A read-only projection reader forwarded to every stage > 0
            handler (as its second argument) and to every reducer. Required when
            any handler declares stage > 0 or any reducer is registered; unused
            for single-stage replay.

    Staged replay: when handlers span more than one stage (see
    `register_handler(stage=...)`), the whole range runs through stage 0, then
    stage 1, and so on. Stage 0 handlers are called `fn(event)`; stage > 0
    handlers `fn(event, reader)`, so they can resolve facts earlier stages
    materialised. After a stage's handlers, its reducers (`register_reducer`) run
    once as `fn(reader)`, or `fn(reader, touched)` for a reducer that opts in to
    the tuple of `TouchedSubject`s the pass wrote.

    How far it gets before failing: a single pass decodes one event at a time, so
    a malformed event at offset N raises `UndecodableEventError` with the first N
    already applied. A staged replay decodes up front and applies nothing. See
    `EventSource`.
    """
    return replay_numbered_from(
        store,
        stream_path,
        executor,
        handler_registry=handler_registry,
        upcaster_registry=upcaster_registry,
        start_seq=start_seq,
        end_seq=end_seq,
        event_match=event_match,
        on_drift=on_drift,
        reader=reader,
    )


def replay_numbered_from(
    store: ReadableStore,
    stream_path: str,
    executor: Executor,
    *,
    first_seq: int = 0,
    handler_registry: HandlerRegistry | None = None,
    upcaster_registry: UpcasterRegistry | None = None,
    start_seq: int = 0,
    end_seq: int | None = None,
    event_match: str | None = None,
    on_drift: OnDriftPolicy = "warn",
    reader: ProjectionReader | None = None,
) -> ReplayResult:
    """`replay()`, with the stream's first message numbered ``first_seq``.

    For a stream that holds only part of another stream's history, such as the
    scratch stream `django_rakaia.fold_events` builds: handlers are chosen by
    ``seq``, so the events must carry the position they have in the real stream.
    ``start_seq`` and ``end_seq`` count in the same numbering.
    """
    match_str = event_match if event_match is not None else stream_path
    messages, _ = store.read(stream_path)

    ctx = build_pipeline(
        handler_registry=handler_registry,
        upcaster_registry=upcaster_registry,
        executor=executor,
        reader=reader,
        on_drift=on_drift,
    )

    def _decoded() -> Iterator[PipelineEvent]:
        # A lazy `EventSource`: one event decoded and upcasted at a time.
        for seq, msg in enumerate(messages, start=first_seq):
            if end_seq is not None and seq >= end_seq:
                break
            if seq < start_seq:
                continue
            # Resolved per event so content-routed (match_field) upcasters can
            # key off the event body.
            yield (
                seq,
                match_str,
                ctx.upcasters.upcast_to_current(
                    _decode_event(msg.data, stream_path, seq),
                    match_str,
                    drift=ctx.drift,
                ),
            )

    return run_passes(ctx, _decoded(), what="Replay")


def merge_replay(
    store: ReadableStore,
    stream_paths: list[str],
    executor: Executor,
    *,
    order_key: str | _EnvelopeTs = "ts",
    handler_registry: HandlerRegistry | None = None,
    upcaster_registry: UpcasterRegistry | None = None,
    event_match: str | None = None,
    on_drift: OnDriftPolicy = "warn",
    reader: ProjectionReader | None = None,
) -> ReplayResult:
    """
    Replay several streams merged into one deterministic total order.

    Every event is decoded, upcast, and tagged with ``(sort_value, stream_path,
    offset)``, then the merged sequence is that tuple sorted: equal order-key
    values across streams break by stream path then offset, so the order is
    **independent of the order `stream_paths` is passed**. The merged events then
    run through the same staged handler + reducer pipeline as `replay()`.

    Args:
        store: The store holding the events.
        stream_paths: The streams to merge, each read in full.
        executor: Effect executor.
        order_key: What to merge on. Two sources, deliberately distinct:

            - a **string** (default ``"ts"``) reads a field out of the **decoded
              payload body** (``event[order_key]``), not the envelope timestamp.
              A missing key raises `MergeKeyError`.
            - the ``ENVELOPE_TS`` sentinel reads the **envelope** timestamp
              (``StreamMessage.event_ts``). Prefer this: it does not need the
              producer to duplicate a timestamp into the payload. A message with
              no ``event_ts`` (only a hand-built one) raises `MergeKeyError`.
        handler_registry / upcaster_registry: default to the process-wide ones.
        event_match: Match string for handler routing + upcasting. Default None
            uses each event's source stream path; pass a value to route every
            merged event by one match string.
        on_drift / reader: as for `replay()`. A reader is required when any
            handler declares stage > 0 or any reducer exists.

    Note: merged `seq` is the event's position in the *merged* order, not a
    per-stream one. Handlers routed by content (`match_field`) or over open ranges
    (`effective_to=None`) are unaffected, but a handler with a **closed** seq range
    sized to a single stream raises `HandlerGapError` under merge.

    Raises `MergeKeyError` when an event lacks the requested order key or the
    order-key values aren't mutually comparable. Duplicate `stream_paths` raises a
    plain `ValueError`, checked before any event is read, as an argument fault;
    `MergeKeyError` is itself a `ValueError`, so one `except ValueError` catches
    all three.

    How far it gets before failing: **nothing is applied unless everything
    decodes**, because a merge reads every stream to know the order. See
    `EventSource`.
    """
    if len(set(stream_paths)) != len(stream_paths):
        raise ValueError(
            f"merge_replay received duplicate stream paths ({stream_paths}); "
            f"each stream would be processed twice."
        )
    ctx = build_pipeline(
        handler_registry=handler_registry,
        upcaster_registry=upcaster_registry,
        executor=executor,
        reader=reader,
        on_drift=on_drift,
    )

    tagged: list[tuple[tuple[Any, str, int], str, dict]] = []
    for path in stream_paths:
        match_str = event_match if event_match is not None else path
        messages, _ = store.read(path)
        for offset, msg in enumerate(messages):
            upcasted = ctx.upcasters.upcast_to_current(
                _decode_event(msg.data, path, offset),
                match_str,
                drift=ctx.drift,
            )
            if order_key is ENVELOPE_TS:
                if msg.event_ts is None:
                    raise MergeKeyError(
                        f"Event at offset={offset} in stream={path!r} has no "
                        f"envelope event_ts; cannot merge on ENVELOPE_TS. (A store "
                        f"always sets it — is this a hand-built StreamMessage?)"
                    )
                sort_value: Any = msg.event_ts
            else:
                if order_key not in upcasted:
                    raise MergeKeyError(
                        f"Event at offset={offset} in stream={path!r} has no "
                        f"order_key={order_key!r} in its payload; cannot merge "
                        f"deterministically."
                    )
                sort_value = upcasted[order_key]
            tagged.append(((sort_value, path, offset), match_str, upcasted))

    try:
        tagged.sort(key=lambda item: item[0])
    except TypeError as exc:
        raise MergeKeyError(
            f"Cannot merge deterministically: order_key={order_key!r} values are "
            f"not mutually comparable across events (mixed types or None?): {exc}"
        ) from exc

    # seq is the position in the merged order. A list rather than a lazy
    # `EventSource` is forced: the sort has already read every stream.
    merged: list[PipelineEvent] = [
        (seq, m, ev) for seq, (_, m, ev) in enumerate(tagged)
    ]
    return run_passes(ctx, merged, what="merge_replay")


def _decode_event(data: bytes, stream_path: str, seq: int) -> dict:
    try:
        return json.loads(data)
    except (ValueError, UnicodeDecodeError) as exc:
        raise UndecodableEventError(
            f"Cannot decode event at seq={seq} in stream={stream_path!r} as JSON: {exc}"
        ) from exc


def _call_handler(
    version: HandlerVersion,
    event: dict,
    reader: ProjectionReader | None,
) -> Iterable[AnyEffect]:
    # Stage > 0 handlers take (event, reader); stage 0 takes (event).
    result = version.fn(event, reader) if version.stage > 0 else version.fn(event)
    return _normalize_effects(result)


def _normalize_effects(result: object) -> list[AnyEffect]:
    """Coerce a handler/reducer return (None | one effect | iterable) to a list."""
    if result is None:
        return []
    if isinstance(result, (Upsert, Update, Delete, Retire, ExternalEffect)):
        return [result]
    return list(result)  # type: ignore[arg-type]
