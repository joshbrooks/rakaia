"""
Replay orchestrator: re-derive materialized state from a stream by running
versioned handlers and an effect executor.

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

Re-running the same range twice produces identical materialized state because
every database effect is idempotent (an `Upsert` converges to the same row).
"""

from __future__ import annotations

import inspect
import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from functools import partial
from typing import Any, Literal

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
)
from .protocols import ProjectionReader, ReadableStore
from .registry import (
    HandlerDriftError,
    HandlerRegistry,
    HandlerVersion,
    ReducerVersion,
    UpcasterRegistry,
    UpcasterVersion,
    get_default_registry,
    get_default_upcaster_registry,
)
from .source_hash import hash_function_source

_log = logging.getLogger("rakaia.replay")

OnDriftPolicy = Literal["warn", "raise"]


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
    warnings: list[str] = field(default_factory=list)
    drift_detected: list[str] = field(default_factory=list)


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
    on_drift: OnDriftPolicy
    result: ReplayResult
    # Only accumulate touched subjects when a reducer actually opts in (a second
    # parameter), so the common path pays nothing.
    track_touched: bool = False
    touched: list[TouchedSubject] = field(default_factory=list)
    _touched_seen: set = field(default_factory=set)
    # Carried so an event source can decode and upcast without re-deriving the
    # registry or rebuilding the drift callback.
    upcasters: Any = None
    drift_callback: Any = None
    # Source hashes computed during this replay, keyed by the callable.
    #
    # Drift is a property of a *registration*, not of an event: a handler's
    # source cannot change while its own replay is running. Checking it per
    # event meant `inspect.getsource` + SHA-256 for every (event x version) —
    # measured at ~86% of total replay time on a 2000-event stream (#156).
    #
    # Scoped to the replay rather than cached globally on purpose. A long-lived
    # process that reloads code between replays must still be able to detect the
    # drift that reload introduced.
    source_hashes: dict[Any, str] = field(default_factory=dict)
    # Registrations already reported as drifted, so the warning is emitted once
    # rather than once per event. `result.drift_detected` already dedupes by
    # name; `result.warnings` and the log did not.
    drift_reported: set = field(default_factory=set)


#: One event ready for dispatch: its sequence, the routing subject to match
#: handlers against, and the decoded + upcasted payload. Whether the seq is an
#: offset in one stream or a position in a merged order is the *source's*
#: business, not the pipeline's.
PipelineEvent = tuple[int, str, dict]


def build_pipeline(
    *,
    handler_registry: HandlerRegistry | None,
    upcaster_registry: UpcasterRegistry | None,
    executor: Executor,
    reader: ProjectionReader | None,
    on_drift: OnDriftPolicy,
) -> _ReplayCtx:
    """Assemble everything a replay needs that does not depend on its event source.

    Registry defaulting, the `ReplayResult`, and the upcaster-drift callback were
    written out identically in `replay()` and `merge_replay()`. So was the
    `_ReplayCtx` construction — the same seven fields, in two places, where
    adding an eighth meant remembering both.

    The returned context carries its own `upcasters` and `drift_callback` so a
    source can decode and upcast events without re-deriving either.
    """
    handlers = handler_registry or get_default_registry()
    result = ReplayResult()

    ctx = _ReplayCtx(
        handlers=handlers,
        executor=executor,
        reader=reader,
        on_drift=on_drift,
        result=result,
        track_touched=_wants_touched_reducer(handlers),
        upcasters=upcaster_registry or get_default_upcaster_registry(),
    )

    def _upcaster_drift(uv: UpcasterVersion, live_hash: str) -> None:
        _record_drift(
            kind="upcaster",
            name=uv.dotted_path,
            stored_hash=uv.source_hash,
            live_hash=live_hash,
            on_drift=on_drift,
            result=result,
        )

    ctx.drift_callback = _upcaster_drift
    return ctx


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
        raise ValueError(
            f"{what} has stage > 0 handlers or reducers but no reader was "
            f"provided; pass reader= so they can read earlier stages' projections."
        )


def run_passes(
    ctx: _ReplayCtx, events: Sequence[PipelineEvent], *, what: str = "Replay"
) -> ReplayResult:
    """Run `events` through every stage, in order, and return the result.

    One pass per stage in ascending order: that stage's per-event handlers over
    **every** event, then its reducers once. An unstaged replay is the same
    shape with a single `None` pass — `_run_stage_reducers(ctx, None)` is a no-op
    when nothing is registered, so the two cases need no branch.

    The events are already decoded and upcasted. What differs between `replay()`
    and `merge_replay()` — reading one stream in order versus k-way-merging
    several by an order key — happens before this, in the source. This is the
    part that was written twice.
    """
    require_reader(ctx, what)
    passes: list[int | None] = list(ctx.handlers.stages()) if is_staged(ctx) else [None]
    for stage in passes:
        for seq, match_str, event in events:
            _dispatch_event(ctx, seq, match_str, event, stage)
        _run_stage_reducers(ctx, stage)
    ctx.result.events_processed = len(events)
    return ctx.result


def _reducer_wants_touched(fn: Any) -> bool:
    """Whether a reducer opts in to the touched-subjects arg by declaring a
    second positional parameter — mirroring how a stage > 0 handler opts in to
    the reader. A plain ``fn(reader)`` returns False and is called with one arg,
    so existing reducers are unchanged.
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
    """Whether any registered reducer opts in — decides if we bother tracking."""
    return handlers.has_reducers() and any(
        _reducer_wants_touched(r.fn) for r in handlers.all_reducers()
    )


def _record_touched(ctx: _ReplayCtx, effects: list[Effect]) -> None:
    # A subject is a database effect's (model_label, lookup); externals never
    # reach here. Dedup in first-seen (event) order so the set is a pure
    # function of the events. A non-hashable lookup value is rare and simply
    # skips dedup — order still deterministic.
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
    # Turn the executor's retire-flip report into one external transition per
    # row a retire actually flipped. `report` may be None (executors that don't
    # observe flips) — treat that as nothing to synthesise. Typed as
    # `ApplyReport | None` (not `object`) so a rename of `retire_flips` is a
    # type error rather than a silent "synthesise nothing" regression.
    if report is None:
        return []
    out: list[ExternalEffect] = []
    for eff, rows in report.retire_flips:
        if eff.transition is None:
            continue
        patch = eff.patch or {}
        for identity in rows:
            out.append(
                ExternalEffect(
                    kind=eff.transition.kind,
                    # Spread patch first so the orchestrator's own `key`/`state`
                    # always win: reconcile_by_key is a generic primitive, and a
                    # patch column named `key` or `state` must not clobber the
                    # row identity or the transition state.
                    payload={**patch, "key": dict(identity), "state": "resolved"},
                )
            )
    return out


def _apply_effects(ctx: _ReplayCtx, effects: list[AnyEffect]) -> None:
    """Route a batch: external effects to the result, the rest to the executor.

    The split is by *type*, so no executor ever needs an external branch and no
    caller has to ask for one to be skipped.
    """
    to_apply: list[Effect] = []
    for e in effects:
        if isinstance(e, ExternalEffect):
            ctx.result.external.append(e)
        else:
            to_apply.append(e)
    if not to_apply:
        return
    report = ctx.executor.apply(to_apply)
    ctx.result.effects_applied += len(to_apply)
    # A retire that flipped rows (a real machine resolution) yields external
    # transitions — collect them alongside the handler-emitted ones.
    ctx.result.external.extend(_synth_transitions(report))


def _dispatch_event(
    ctx: _ReplayCtx, seq: int, match_str: str, upcasted: dict, stage: int | None
) -> None:
    versions = ctx.handlers.resolve(match_str, seq, event=upcasted, stage=stage)
    all_effects: list[AnyEffect] = []
    for version in versions:
        _check_handler_drift(version, ctx)
        all_effects.extend(_call_handler(version, upcasted, ctx.reader))
    if ctx.track_touched:
        _record_touched(
            ctx, [e for e in all_effects if not isinstance(e, ExternalEffect)]
        )
    _apply_effects(ctx, all_effects)


def _run_stage_reducers(ctx: _ReplayCtx, stage: int | None) -> None:
    # Reducers run once per stage, after that stage's per-event handlers have
    # committed, reading the accumulated projections via the reader. A None
    # stage (single, non-staged pass) has no reducers. A reducer declaring a
    # second parameter also receives the subjects the pass's handlers touched so
    # far (cumulative across stages already run this pass); reducer *outputs* are
    # not themselves recorded as touched.
    for reducer in ctx.handlers.reducers_for_stage(stage):
        _check_reducer_drift(reducer, ctx)
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
            handler's source has changed since registration; 'raise' raises
            HandlerDriftError. (Drift detection itself lands in Step 6.)
        reader: A read-only projection reader forwarded to every stage > 0
            handler (as its second argument) and to every reducer. Required when
            any handler declares stage > 0 or any reducer is registered; unused
            for single-stage replay.

    Staged replay: when handlers span more than one stage (see
    `register_handler(stage=...)`), replay runs the whole event range through
    stage 0, then stage 1, and so on. Stage 0 handlers are called `fn(event)`;
    stage > 0 handlers are called `fn(event, reader)` so they can resolve facts
    earlier stages materialised (a form linking to a reference entity another
    form created, regardless of arrival order). After a stage's per-event
    handlers, its reducers (`register_reducer`) run once as `fn(reader)` — or
    `fn(reader, touched)` for a reducer that opts in to the tuple of
    `TouchedSubject`s the pass's handlers wrote — to recompute aggregates. With
    only stage 0 handlers and no reducers, replay is a single pass, exactly as
    before.
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

    def _decode_upcast(seq: int, data: bytes) -> dict:
        # Target version is resolved per event so content-routed upcasters
        # (match_field) can key off the event body, not just the stream path.
        return ctx.upcasters.upcast_to_current(
            _decode_event(data, stream_path, seq),
            match_str,
            drift_callback=ctx.drift_callback,
            hasher=partial(live_source_hash, ctx),
        )

    def _in_range(seq: int) -> bool:
        return seq >= start_seq and (end_seq is None or seq < end_seq)

    if not is_staged(ctx):
        # Single stage keeps its own streaming loop rather than going through
        # `run_passes`. Not a micro-optimisation: decoding lazily means a
        # malformed event at offset N fails *after* the first N-1 have been
        # dispatched, which is the partial-progress behaviour replay had before
        # staging existed. Materialising first would move that failure earlier.
        for seq, msg in enumerate(messages):
            if end_seq is not None and seq >= end_seq:
                break
            if not _in_range(seq):
                continue
            _dispatch_event(ctx, seq, match_str, _decode_upcast(seq, msg.data), None)
            ctx.result.events_processed += 1
        return ctx.result

    require_reader(ctx, "Replay")
    decoded: list[PipelineEvent] = []
    for seq, msg in enumerate(messages):
        if end_seq is not None and seq >= end_seq:
            break
        if _in_range(seq):
            decoded.append((seq, match_str, _decode_upcast(seq, msg.data)))
    return run_passes(ctx, decoded, what="Replay")


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

    Each stream is already ordered, so this is a k-way merge; the design point is
    the **order key**. Every event is decoded, upcast, and tagged with
    ``(sort_value, stream_path, offset)``, then the merged sequence is that tuple
    sorted — so equal order-key values across streams break by stream path then
    offset, giving an order that is a pure function of the streams and is
    **independent of the order `stream_paths` is passed**. The merged events then
    run through exactly the same staged handler + reducer pipeline as `replay()`.

    Args:
        store: The store holding the events.
        stream_paths: The streams to merge, each read in full.
        executor: Effect executor.
        order_key: What to merge on. **Two sources, deliberately distinct:**

            - a **string** (default ``"ts"``) reads a field out of the **decoded
              payload body** (``event[order_key]``). This is *not* the transport
              or envelope timestamp — it is whatever field the producer wrote into
              the JSON. A missing key raises a ValueError.
            - the ``ENVELOPE_TS`` sentinel reads the first-class **envelope**
              timestamp (``StreamMessage.event_ts`` — the producer's logical event
              time, defaulting to append time). Prefer this: it is unambiguous and
              does not require the producer to duplicate a timestamp into the
              payload. A message with no ``event_ts`` (only a hand-built one; a
              store always sets it) raises a ValueError.
        handler_registry / upcaster_registry: default to the process-wide ones.
        event_match: Match string for handler routing + upcasting. Default None
            uses each event's **source stream path** as its match string, so
            content-routed handlers (`match_field`) and per-stream upcasters both
            work; pass a value to route every merged event by one match string.
        on_drift / reader: as for `replay()`. A reader is required when any
            handler declares stage > 0 or any reducer exists.

    Note: merged `seq` is the event's position in the *merged* order (0-based),
    not a per-stream one, so handlers routed by content (`match_field`) or
    versioned over open ranges (`effective_to=None`) are unaffected, but a
    handler with a **closed** seq range sized to a single stream will raise
    `HandlerGapError` under merge (the merged range is longer). Version merged
    handlers by content or open ranges.

    Raises `ValueError` on duplicate `stream_paths`, when an event lacks the
    requested order key (payload field, or `event_ts` under `ENVELOPE_TS`), or
    when the order-key values aren't mutually comparable across events.
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

    # Decode + upcast every event of every stream, tag with the sort key.
    tagged: list[tuple[tuple[Any, str, int], str, dict]] = []
    for path in stream_paths:
        match_str = event_match if event_match is not None else path
        messages, _ = store.read(path)
        for offset, msg in enumerate(messages):
            # per-event target so content-routed (match_field) upcasters resolve
            upcasted = ctx.upcasters.upcast_to_current(
                _decode_event(msg.data, path, offset),
                match_str,
                drift_callback=ctx.drift_callback,
                hasher=partial(live_source_hash, ctx),
            )
            if order_key is ENVELOPE_TS:
                if msg.event_ts is None:
                    raise ValueError(
                        f"Event at offset={offset} in stream={path!r} has no "
                        f"envelope event_ts; cannot merge on ENVELOPE_TS. (A store "
                        f"always sets it — is this a hand-built StreamMessage?)"
                    )
                sort_value: Any = msg.event_ts
            else:
                if order_key not in upcasted:
                    raise ValueError(
                        f"Event at offset={offset} in stream={path!r} has no "
                        f"order_key={order_key!r} in its payload; cannot merge "
                        f"deterministically."
                    )
                sort_value = upcasted[order_key]
            tagged.append(((sort_value, path, offset), match_str, upcasted))

    try:
        tagged.sort(key=lambda item: item[0])
    except TypeError as exc:
        raise ValueError(
            f"Cannot merge deterministically: order_key={order_key!r} values are "
            f"not mutually comparable across events (mixed types or None?): {exc}"
        ) from exc

    # seq is the event's position in the merged order, not its offset in any
    # one stream — which is exactly why the pipeline takes seq per event.
    merged: list[PipelineEvent] = [
        (seq, m, ev) for seq, (_, m, ev) in enumerate(tagged)
    ]
    return run_passes(ctx, merged, what="merge_replay")


def live_source_hash(ctx: _ReplayCtx, fn: Any) -> str:
    """The source hash of `fn`, computed at most once per replay.

    The expensive part of a drift check — `inspect.getsource` and a SHA-256 —
    depends only on the callable, so it is memoised here. The *comparison* stays
    per registration, because two registrations may legitimately share a
    function while storing different hashes.
    """
    cached = ctx.source_hashes.get(fn)
    if cached is None:
        cached = hash_function_source(fn)
        ctx.source_hashes[fn] = cached
    return cached


def _check_handler_drift(version: HandlerVersion, ctx: _ReplayCtx) -> None:
    live_hash = live_source_hash(ctx, version.fn)
    if live_hash == version.source_hash:
        return
    if ("handler", version.name) in ctx.drift_reported:
        return
    ctx.drift_reported.add(("handler", version.name))
    _record_drift(
        kind="handler",
        name=version.name,
        stored_hash=version.source_hash,
        live_hash=live_hash,
        on_drift=ctx.on_drift,
        result=ctx.result,
    )


def _check_reducer_drift(reducer: ReducerVersion, ctx: _ReplayCtx) -> None:
    live_hash = live_source_hash(ctx, reducer.fn)
    if live_hash == reducer.source_hash:
        return
    if ("reducer", reducer.name) in ctx.drift_reported:
        return
    ctx.drift_reported.add(("reducer", reducer.name))
    _record_drift(
        kind="reducer",
        name=reducer.name,
        stored_hash=reducer.source_hash,
        live_hash=live_hash,
        on_drift=ctx.on_drift,
        result=ctx.result,
    )


def _record_drift(
    *,
    kind: str,
    name: str,
    stored_hash: str,
    live_hash: str,
    on_drift: OnDriftPolicy,
    result: ReplayResult,
) -> None:
    message = (
        f"RAKAIA_DRIFT {kind}={name!r} stored={stored_hash[:12]} "
        f"current={live_hash[:12]}"
    )
    if on_drift == "raise":
        raise HandlerDriftError(message)
    if name not in result.drift_detected:
        result.drift_detected.append(name)
    result.warnings.append(message)
    _log.warning(message)


def _decode_event(data: bytes, stream_path: str, seq: int) -> dict:
    try:
        return json.loads(data)
    except (ValueError, UnicodeDecodeError) as exc:
        raise ValueError(
            f"Cannot decode event at seq={seq} in stream={stream_path!r} as JSON: {exc}"
        ) from exc


def _call_handler(
    version: HandlerVersion,
    event: dict,
    reader: ProjectionReader | None,
) -> Iterable[AnyEffect]:
    # Stage > 0 handlers take (event, reader); stage 0 keeps the (event)
    # signature so existing single-stage handlers are unchanged.
    result = version.fn(event, reader) if version.stage > 0 else version.fn(event)
    return _normalize_effects(result)


def _normalize_effects(result: object) -> list[AnyEffect]:
    """Coerce a handler/reducer return (None | one effect | iterable) to a list."""
    if result is None:
        return []
    if isinstance(result, (Upsert, Update, Delete, Retire, ExternalEffect)):
        return [result]
    return list(result)  # type: ignore[arg-type]
