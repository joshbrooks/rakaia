"""
Replay orchestrator: re-derive materialized state from a stream by running
versioned handlers and an effect executor.

Pipeline per event:
    1. Load the raw event bytes from the stream
    2. Decode as JSON, upcast to current schema via UpcasterRegistry
    3. Resolve handlers via HandlerRegistry.resolve(event_match, seq)
    4. Call each handler -> collect Effects
    5. Filter out op="external" effects unless include_external=True
    6. executor.apply(effects)

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
from .registry import (
    HandlerDriftError,
    HandlerRegistry,
    HandlerVersion,
    UpcasterRegistry,
    UpcasterVersion,
    get_default_registry,
    get_default_upcaster_registry,
)
from .source_hash import hash_function_source
from .store import StreamStore

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
    store: StreamStore,
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
) -> ReplayResult:
    """
    Replay events in `stream_path` from `start_seq` (inclusive) to `end_seq`
    (exclusive) through the registered handlers, applying produced effects
    via `executor`.

    Args:
        store: The StreamStore holding the events.
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

    for seq, msg in enumerate(messages):
        if seq < start_seq:
            continue
        if end_seq is not None and seq >= end_seq:
            break

        event_dict = _decode_event(msg.data, stream_path, seq)
        upcasted = upcasters.apply_chain(
            event_dict,
            match_str,
            target_version,
            drift_callback=_upcaster_drift,
        )

        versions = handlers.resolve(match_str, seq)
        all_effects: list[Effect] = []
        for version in versions:
            _check_handler_drift(version, on_drift, result)
            effects = _call_handler(version, upcasted)
            all_effects.extend(effects)

        external_count = sum(1 for e in all_effects if e.op == "external")
        result.external_effects_skipped += (
            external_count if not include_external else 0
        )
        to_apply = (
            all_effects
            if include_external
            else [e for e in all_effects if e.op != "external"]
        )
        if to_apply:
            executor.apply(to_apply)
            result.effects_applied += len(to_apply)

        result.events_processed += 1

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
            f"Cannot decode event at seq={seq} in stream={stream_path!r} "
            f"as JSON: {exc}"
        ) from exc


def _call_handler(version: HandlerVersion, event: dict) -> Iterable[Effect]:
    result = version.fn(event)
    if result is None:
        return []
    if isinstance(result, Effect):
        return [result]
    return list(result)
