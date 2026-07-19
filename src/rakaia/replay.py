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
from typing import Literal

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


@dataclass
class ReplayResult:
    """Summary of a single replay() invocation."""

    events_processed: int = 0
    effects_applied: int = 0
    external_effects_skipped: int = 0
    warnings: list[str] = field(default_factory=list)
    drift_detected: list[str] = field(default_factory=list)


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
    target_version = upcasters.current_version(match_str)

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
        return upcasters.apply_chain(
            _decode_event(data, stream_path, seq),
            match_str,
            target_version,
            drift_callback=_upcaster_drift,
        )

    def _apply(effects: list[Effect]) -> None:
        external_count = sum(1 for e in effects if e.op == "external")
        if not include_external:
            result.external_effects_skipped += external_count
        to_apply = (
            effects if include_external else [e for e in effects if e.op != "external"]
        )
        if to_apply:
            executor.apply(to_apply)
            result.effects_applied += len(to_apply)

    def _dispatch(seq: int, upcasted: dict, stage: int | None) -> None:
        versions = handlers.resolve(match_str, seq, event=upcasted, stage=stage)
        all_effects: list[Effect] = []
        for version in versions:
            _check_handler_drift(version, on_drift, result)
            all_effects.extend(_call_handler(version, upcasted, reader))
        _apply(all_effects)

    def _run_reducers(stage: int) -> None:
        # Reducers run once per stage, after that stage's per-event handlers
        # have committed, reading the accumulated projections via `reader`.
        for reducer in handlers.reducers_for_stage(stage):
            _check_reducer_drift(reducer, on_drift, result)
            _apply(_normalize_effects(reducer.fn(reader)))

    def _in_range(seq: int) -> bool:
        return seq >= start_seq and (end_seq is None or seq < end_seq)

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
            _dispatch(seq, _decode_upcast(seq, msg.data), stage=None)
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
            _dispatch(seq, upcasted, stage=stage)
        _run_reducers(stage)
    result.events_processed = len(decoded)
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
