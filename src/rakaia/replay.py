"""
Replay orchestrator: re-derive materialized state from a stream by running
versioned handlers and an effect executor.

Per event the pipeline is:
    1. Load the raw event bytes from the stream
    2. Decode as JSON, upcast to current schema via UpcasterRegistry
    3. Resolve handlers via HandlerRegistry.resolve(event_match, seq, event, stage)
    4. Call each handler -> collect Effects
    5. Filter out op="external" effects unless include_external=True
    6. executor.apply(effects)

When every handler is stage 0 (the default), replay is a single streaming pass
over the range — one event decoded and applied at a time. When handlers span
more than one stage, replay decodes the range once and runs the pipeline as one
pass per stage in ascending order: the whole range through stage 0, then stage
1, and so on, so a stage > 0 handler (called `fn(event, reader)`) can read the
projections earlier stages committed. A stage may also declare **reducers**
(`register_reducer(name, stage, fn)`) that run once, after that stage's per-event
handlers, to recompute an aggregate from the committed projections via the
reader. See `register_handler(stage=...)` and `register_reducer(...)`.

Re-running the same range twice produces identical materialized state because
all Effects use update_or_create (idempotent).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass, field
from typing import Any, Literal

from .effects import Effect, Executor
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
    external_effects_skipped: int = 0
    warnings: list[str] = field(default_factory=list)
    drift_detected: list[str] = field(default_factory=list)


@dataclass
class _ReplayCtx:
    """Bundle of the invariants the pipeline helpers need, shared by `replay()`
    (single stream) and `merge_replay()` (several streams merged)."""

    handlers: HandlerRegistry
    executor: Executor
    reader: ProjectionReader | None
    include_external: bool
    on_drift: OnDriftPolicy
    result: ReplayResult


def _apply_effects(ctx: _ReplayCtx, effects: list[Effect]) -> None:
    external_count = sum(1 for e in effects if e.op == "external")
    if not ctx.include_external:
        ctx.result.external_effects_skipped += external_count
    to_apply = (
        effects if ctx.include_external else [e for e in effects if e.op != "external"]
    )
    if to_apply:
        ctx.executor.apply(to_apply)
        ctx.result.effects_applied += len(to_apply)


def _dispatch_event(
    ctx: _ReplayCtx, seq: int, match_str: str, upcasted: dict, stage: int | None
) -> None:
    versions = ctx.handlers.resolve(match_str, seq, event=upcasted, stage=stage)
    all_effects: list[Effect] = []
    for version in versions:
        _check_handler_drift(version, ctx.on_drift, ctx.result)
        all_effects.extend(_call_handler(version, upcasted, ctx.reader))
    _apply_effects(ctx, all_effects)


def _run_stage_reducers(ctx: _ReplayCtx, stage: int | None) -> None:
    # Reducers run once per stage, after that stage's per-event handlers have
    # committed, reading the accumulated projections via the reader. A None
    # stage (single, non-staged pass) has no reducers.
    for reducer in ctx.handlers.reducers_for_stage(stage):
        _check_reducer_drift(reducer, ctx.on_drift, ctx.result)
        _apply_effects(ctx, _normalize_effects(reducer.fn(ctx.reader)))


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
    include_external: bool = False,
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
        include_external: If False (default), Effects with op="external" are
            counted but not passed to the executor.
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
    handlers, its reducers (`register_reducer`) run once as `fn(reader)` to
    recompute aggregates. With only stage 0 handlers and no reducers, replay is a
    single pass, exactly as before.
    """
    handlers = handler_registry or get_default_registry()
    upcasters = upcaster_registry or get_default_upcaster_registry()
    match_str = event_match if event_match is not None else stream_path

    messages, _ = store.read(stream_path)

    result = ReplayResult()

    def _upcaster_drift(uv: UpcasterVersion, live_hash: str) -> None:
        _record_drift(
            kind="upcaster",
            name=uv.dotted_path,
            stored_hash=uv.source_hash,
            live_hash=live_hash,
            on_drift=on_drift,
            result=result,
        )

    def _decode_upcast(seq: int, data: bytes) -> dict:
        # Target version is resolved per event so content-routed upcasters
        # (match_field) can key off the event body, not just the stream path.
        return upcasters.upcast_to_current(
            _decode_event(data, stream_path, seq),
            match_str,
            drift_callback=_upcaster_drift,
        )

    def _in_range(seq: int) -> bool:
        return seq >= start_seq and (end_seq is None or seq < end_seq)

    ctx = _ReplayCtx(
        handlers=handlers,
        executor=executor,
        reader=reader,
        include_external=include_external,
        on_drift=on_drift,
        result=result,
    )
    stage_numbers = handlers.stages()
    staged = any(s > 0 for s in stage_numbers) or handlers.has_reducers()

    if not staged:
        # Single stage: stream one event at a time (bounded memory, and drift /
        # partial-progress semantics identical to pre-staged replay).
        for seq, msg in enumerate(messages):
            if end_seq is not None and seq >= end_seq:
                break
            if not _in_range(seq):
                continue
            _dispatch_event(ctx, seq, match_str, _decode_upcast(seq, msg.data), None)
            result.events_processed += 1
        return result

    if reader is None:
        raise ValueError(
            "Replay has stage > 0 handlers or reducers but no reader was "
            "provided; pass reader= so they can read earlier stages' projections."
        )
    # Staged: decode + upcast each in-range event once, then run one pass per
    # stage in ascending order — that stage's per-event handlers, then its
    # reducers once — over the shared list.
    decoded: list[tuple[int, dict]] = []
    for seq, msg in enumerate(messages):
        if end_seq is not None and seq >= end_seq:
            break
        if _in_range(seq):
            decoded.append((seq, _decode_upcast(seq, msg.data)))
    for stage in stage_numbers:
        for seq, upcasted in decoded:
            _dispatch_event(ctx, seq, match_str, upcasted, stage)
        _run_stage_reducers(ctx, stage)
    result.events_processed = len(decoded)
    return result


def merge_replay(
    store: ReadableStore,
    stream_paths: list[str],
    executor: Executor,
    *,
    order_key: str | _EnvelopeTs = "ts",
    handler_registry: HandlerRegistry | None = None,
    upcaster_registry: UpcasterRegistry | None = None,
    event_match: str | None = None,
    include_external: bool = False,
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
        include_external / on_drift / reader: as for `replay()`. A reader is
            required when any handler declares stage > 0 or any reducer exists.

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
    handlers = handler_registry or get_default_registry()
    upcasters = upcaster_registry or get_default_upcaster_registry()
    result = ReplayResult()

    def _upcaster_drift(uv: UpcasterVersion, live_hash: str) -> None:
        _record_drift(
            kind="upcaster",
            name=uv.dotted_path,
            stored_hash=uv.source_hash,
            live_hash=live_hash,
            on_drift=on_drift,
            result=result,
        )

    # Decode + upcast every event of every stream, tag with the sort key.
    tagged: list[tuple[tuple[Any, str, int], str, dict]] = []
    for path in stream_paths:
        match_str = event_match if event_match is not None else path
        messages, _ = store.read(path)
        for offset, msg in enumerate(messages):
            # per-event target so content-routed (match_field) upcasters resolve
            upcasted = upcasters.upcast_to_current(
                _decode_event(msg.data, path, offset),
                match_str,
                drift_callback=_upcaster_drift,
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

    ctx = _ReplayCtx(
        handlers=handlers,
        executor=executor,
        reader=reader,
        include_external=include_external,
        on_drift=on_drift,
        result=result,
    )
    stage_numbers = handlers.stages()
    staged = any(s > 0 for s in stage_numbers) or handlers.has_reducers()
    if staged and reader is None:
        raise ValueError(
            "merge_replay has stage > 0 handlers or reducers but no reader was "
            "provided; pass reader= so they can read earlier stages' projections."
        )

    # seq is the event's position in the merged order.
    merged = [(seq, m, ev) for seq, (_, m, ev) in enumerate(tagged)]
    passes: list[int | None] = list(stage_numbers) if staged else [None]
    for stage in passes:
        for seq, match_str, upcasted in merged:
            _dispatch_event(ctx, seq, match_str, upcasted, stage)
        _run_stage_reducers(ctx, stage)
    result.events_processed = len(merged)
    return result


def _check_handler_drift(
    version: HandlerVersion,
    on_drift: OnDriftPolicy,
    result: ReplayResult,
) -> None:
    live_hash = hash_function_source(version.fn)
    if live_hash == version.source_hash:
        return
    _record_drift(
        kind="handler",
        name=version.name,
        stored_hash=version.source_hash,
        live_hash=live_hash,
        on_drift=on_drift,
        result=result,
    )


def _check_reducer_drift(
    reducer: ReducerVersion,
    on_drift: OnDriftPolicy,
    result: ReplayResult,
) -> None:
    live_hash = hash_function_source(reducer.fn)
    if live_hash == reducer.source_hash:
        return
    _record_drift(
        kind="reducer",
        name=reducer.name,
        stored_hash=reducer.source_hash,
        live_hash=live_hash,
        on_drift=on_drift,
        result=result,
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
) -> Iterable[Effect]:
    # Stage > 0 handlers take (event, reader); stage 0 keeps the (event)
    # signature so existing single-stage handlers are unchanged.
    result = version.fn(event, reader) if version.stage > 0 else version.fn(event)
    return _normalize_effects(result)


def _normalize_effects(result: object) -> list[Effect]:
    """Coerce a handler/reducer return (None | Effect | iterable) to a list."""
    if result is None:
        return []
    if isinstance(result, Effect):
        return [result]
    return list(result)  # type: ignore[arg-type]
