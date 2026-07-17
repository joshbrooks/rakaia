"""
Versioned handler registry: sequence-bracketed dispatch of pure event handlers.

Handlers are registered with a `[from_seq, to_seq)` range. On dispatch, the
registry resolves the version covering the event's sequence number, raising
clearly if multiple versions overlap (rejected at registration) or if a gap
in coverage exists for a registered handler name.

Registrations are durably appended to a reserved meta-stream so they survive
process restarts and provide their own audit log. Re-registration with
identical (name, event_match, from, to, dotted_path, source_hash) is a no-op.
"""

from __future__ import annotations

import fnmatch
import importlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .source_hash import hash_function_source

if TYPE_CHECKING:
    from .store import StreamStore

HANDLERS_META_STREAM = "__rakaia__:handlers"
UPCASTERS_META_STREAM = "__rakaia__:upcasters"


# =============================================================================
# Errors
# =============================================================================


class HandlerOverlapError(Exception):
    """Two versions of the same handler claim overlapping sequence ranges."""


class HandlerGapError(Exception):
    """No registered version of a handler covers the requested sequence."""


class UpcasterConflictError(Exception):
    """Two upcasters were registered for the same (event_match, from_version)."""


class UpcasterChainError(Exception):
    """Cannot upcast: missing or ambiguous link in the upcaster chain."""


class HandlerDriftError(Exception):
    """A handler/upcaster's source body differs from its registered hash."""


# =============================================================================
# Data structures
# =============================================================================


@dataclass(frozen=True)
class HandlerVersion:
    """One registered version of a handler."""

    name: str
    """Handler name, stable across versions (e.g. 'mogrify')."""

    event_match: str
    """Glob pattern matched against the event's stream/event-type."""

    effective_from: int
    """Inclusive lower bound on stream sequence."""

    effective_to: int | None
    """Exclusive upper bound on stream sequence. None = currently active."""

    fn: Callable[..., Any]
    """The handler callable. event -> Effect | list[Effect]."""

    dotted_path: str
    """fn.__module__ + '.' + fn.__qualname__ — for persistence in Step 3."""

    source_hash: str
    """SHA-256 of fn's source body — for drift detection in Step 6."""

    match_field: str | None = None
    """When set, `event_match` is matched against `event[match_field]` (e.g.
    'form_type') instead of the stream path. None = match the stream path."""


# =============================================================================
# Registry
# =============================================================================


class HandlerRegistry:
    """
    Registry of versioned handlers, optionally backed by a meta-stream.

    Versions are stored under the (handler_name, event_match) key so that two
    handlers with the same name but different patterns are treated as parallel
    series. Overlap detection runs at registration time within a series.

    If a `StreamStore` is provided, registrations are appended to a reserved
    meta-stream (`__rakaia__:handlers`). Identical re-registrations are no-ops
    so registration is safe to call from module-import-time decorators across
    process restarts.
    """

    def __init__(
        self,
        store: StreamStore | None = None,
        *,
        stream_path: str = HANDLERS_META_STREAM,
    ) -> None:
        # (name, event_match) -> sorted list of HandlerVersion by effective_from
        self._versions: dict[tuple[str, str], list[HandlerVersion]] = {}
        self._store = store
        self._stream_path = stream_path
        # Identity tuples already persisted to the stream — used for dedup.
        self._persisted_ids: set[
            tuple[str, str, int, int | None, str, str, str | None]
        ] = set()
        if store is not None:
            self._ensure_stream()
            self._load_persisted_ids()

    def register(
        self,
        name: str,
        event_match: str,
        fn: Callable[..., Any],
        effective_from: int,
        effective_to: int | None = None,
        *,
        match_field: str | None = None,
    ) -> HandlerVersion:
        """
        Register a handler version. Raises HandlerOverlapError on overlap.

        Re-registering with identical (name, event_match, from, to,
        dotted_path, source_hash, match_field) is a no-op that returns the
        existing version without re-appending to the meta-stream.

        When `match_field` is set, `event_match` is matched against
        `event[match_field]` (e.g. 'form_type') instead of the stream path.
        """
        if effective_from < 0:
            raise ValueError(
                f"effective_from must be non-negative, got {effective_from}"
            )
        if effective_to is not None and effective_to <= effective_from:
            raise ValueError(
                f"effective_to ({effective_to}) must be > effective_from "
                f"({effective_from})"
            )

        version = HandlerVersion(
            name=name,
            event_match=event_match,
            effective_from=effective_from,
            effective_to=effective_to,
            fn=fn,
            dotted_path=f"{fn.__module__}.{fn.__qualname__}",
            source_hash=hash_function_source(fn),
            match_field=match_field,
        )

        key = (name, event_match)
        series = self._versions.setdefault(key, [])

        # In-memory dedup: exact identical registration → no-op
        for existing in series:
            if _identity(existing) == _identity(version):
                return existing

        self._check_overlap(version, series)

        if self._store is not None:
            self._persist_if_new(version)

        series.append(version)
        series.sort(key=lambda v: v.effective_from)
        return version

    def resolve(
        self,
        event_match: str,
        seq: int,
        event: dict[str, Any] | None = None,
    ) -> list[HandlerVersion]:
        """
        Return all handler versions whose pattern matches AND whose [from, to)
        range covers seq. By default the pattern is matched against the
        `event_match` string (the stream path); a series registered with a
        `match_field` is instead matched against `event[match_field]`, so pass
        the decoded `event` when any content-routed handlers exist.

        Raises HandlerGapError if any matching handler series has no version
        covering seq (a gap, not "no handlers").
        """
        if seq < 0:
            raise ValueError(f"seq must be non-negative, got {seq}")

        resolved: list[HandlerVersion] = []
        for (name, pattern), versions in self._versions.items():
            subject = self._match_subject(name, versions, event_match, event)
            if not fnmatch.fnmatchcase(subject, pattern):
                continue
            covering = [
                v
                for v in versions
                if v.effective_from <= seq
                and (v.effective_to is None or seq < v.effective_to)
            ]
            if not covering:
                raise HandlerGapError(
                    f"No version of handler {name!r} (pattern {pattern!r}) covers "
                    f"seq={seq} for event_match={event_match!r}. "
                    f"Registered ranges: "
                    f"{[(v.effective_from, v.effective_to) for v in versions]}"
                )
            # Overlap is rejected at registration, so at most one matches.
            resolved.append(covering[0])
        return resolved

    @staticmethod
    def _match_subject(
        name: str,
        versions: list[HandlerVersion],
        event_match: str,
        event: dict[str, Any] | None,
    ) -> str:
        """The string a series' pattern is tested against.

        Stream-path routing (the default) returns `event_match`. Content
        routing (a `match_field` on the series) returns `event[match_field]`.
        match_field is consistent within a (name, pattern) series, so the first
        version decides.
        """
        match_field = versions[0].match_field
        if match_field is None:
            return event_match
        if event is None:
            raise ValueError(
                f"Handler {name!r} routes on match_field={match_field!r} but "
                f"resolve() was called without an event."
            )
        return str(event.get(match_field, ""))

    def all_versions(self) -> list[HandlerVersion]:
        """Return every registered version (test/debug helper)."""
        out: list[HandlerVersion] = []
        for series in self._versions.values():
            out.extend(series)
        return out

    def rehydrate(self) -> None:
        """
        Import every dotted-path's module so its `@register_handler`
        decorators run and re-register against this registry.

        No-op if no backing store is configured. Modules are imported once
        (Python caches imports), so calling this after some modules have
        already been imported is safe.
        """
        if self._store is None:
            return
        for ident in self._persisted_ids:
            dotted_path = ident[4]
            module_name = dotted_path.rsplit(".", 1)[0]
            importlib.import_module(module_name)

    # ------------------------------------------------------------------
    # Internal — persistence
    # ------------------------------------------------------------------

    def _ensure_stream(self) -> None:
        # Meta-stream uses no content_type so each append is stored as a
        # standalone bytes blob (one JSON object per message). We avoid
        # application/json mode because it appends a trailing comma and
        # treats the whole stream as a flattened JSON array.
        assert self._store is not None
        if not self._store.has(self._stream_path):
            self._store.create(self._stream_path)

    def _load_persisted_ids(self) -> None:
        assert self._store is not None
        messages, _ = self._store.read(self._stream_path)
        for msg in messages:
            try:
                payload = json.loads(msg.data)
            except (ValueError, UnicodeDecodeError):
                continue
            self._persisted_ids.add(_identity_from_payload(payload))

    def _persist_if_new(self, version: HandlerVersion) -> None:
        assert self._store is not None
        ident = _identity(version)
        if ident in self._persisted_ids:
            return
        payload = {
            "name": version.name,
            "event_match": version.event_match,
            "effective_from": version.effective_from,
            "effective_to": version.effective_to,
            "dotted_path": version.dotted_path,
            "source_hash": version.source_hash,
            "match_field": version.match_field,
        }
        self._store.append(
            self._stream_path,
            json.dumps(payload).encode("utf-8"),
        )
        self._persisted_ids.add(ident)

    # ------------------------------------------------------------------
    # Internal — overlap check
    # ------------------------------------------------------------------

    @staticmethod
    def _check_overlap(new: HandlerVersion, series: list[HandlerVersion]) -> None:
        new_to = new.effective_to if new.effective_to is not None else float("inf")
        for existing in series:
            ex_to = (
                existing.effective_to
                if existing.effective_to is not None
                else float("inf")
            )
            # half-open intervals [a, b) overlap [c, d) iff a < d and c < b
            if new.effective_from < ex_to and existing.effective_from < new_to:
                raise HandlerOverlapError(
                    f"Handler {new.name!r} on {new.event_match!r}: "
                    f"new range [{new.effective_from}, {new.effective_to}) overlaps "
                    f"existing [{existing.effective_from}, {existing.effective_to})"
                )


# =============================================================================
# Upcaster registry
# =============================================================================


@dataclass(frozen=True)
class UpcasterVersion:
    """One registered upcaster: transforms event from `from_version` to next."""

    event_match: str
    """Glob pattern matched against the event's stream/event-type."""

    from_version: int
    """The schema version this upcaster expects as input. Output is from+1."""

    fn: Callable[[dict[str, Any]], dict[str, Any]]
    """The upcaster callable. dict -> dict."""

    dotted_path: str
    source_hash: str


class UpcasterRegistry:
    """
    Registry of schema-version upcasters, keyed by (event_match, from_version).

    Each upcaster transforms an event dict from schema_version N to N+1. The
    registry composes them into a chain via `apply_chain`.
    """

    def __init__(
        self,
        store: StreamStore | None = None,
        *,
        stream_path: str = UPCASTERS_META_STREAM,
    ) -> None:
        self._upcasters: dict[tuple[str, int], UpcasterVersion] = {}
        self._store = store
        self._stream_path = stream_path
        self._persisted_ids: set[tuple[str, int, str, str]] = set()
        if store is not None:
            self._ensure_stream()
            self._load_persisted_ids()

    def register(
        self,
        event_match: str,
        from_version: int,
        fn: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> UpcasterVersion:
        """Register an upcaster from `from_version` to `from_version + 1`."""
        if from_version < 1:
            raise ValueError(f"from_version must be >= 1, got {from_version}")

        new = UpcasterVersion(
            event_match=event_match,
            from_version=from_version,
            fn=fn,
            dotted_path=f"{fn.__module__}.{fn.__qualname__}",
            source_hash=hash_function_source(fn),
        )

        key = (event_match, from_version)
        existing = self._upcasters.get(key)
        if existing is not None:
            if _u_identity(existing) == _u_identity(new):
                return existing
            raise UpcasterConflictError(
                f"Upcaster already registered for event_match={event_match!r}, "
                f"from_version={from_version} (existing dotted_path="
                f"{existing.dotted_path!r}, new={new.dotted_path!r})"
            )

        if self._store is not None:
            self._persist_if_new(new)
        self._upcasters[key] = new
        return new

    def current_version(self, event_match_str: str) -> int:
        """Highest schema version reachable for an event matching this string."""
        matching_froms = [
            fv
            for (pattern, fv) in self._upcasters
            if fnmatch.fnmatchcase(event_match_str, pattern)
        ]
        if not matching_froms:
            return 1
        return max(matching_froms) + 1

    def apply_chain(
        self,
        event: dict[str, Any],
        event_match_str: str,
        target_version: int,
        *,
        drift_callback: Callable[[UpcasterVersion, str], None] | None = None,
    ) -> dict[str, Any]:
        """
        Upcast `event` from its current schema_version up to `target_version`.

        The event's current version is read from event["schema_version"]
        (default 1). Each step finds the upcaster matching the event_match
        and the current version, then bumps schema_version on the result.

        If `drift_callback` is provided, it is called once per step with
        `(version, current_hash)` if the upcaster's source hash has changed
        since registration. The callback decides whether to raise or warn.
        """
        current = int(event.get("schema_version", 1))
        if current > target_version:
            raise UpcasterChainError(
                f"Event has schema_version={current} but target_version="
                f"{target_version} is lower; missing upcaster for a newer schema?"
            )
        while current < target_version:
            matching = [
                up
                for (pattern, fv), up in self._upcasters.items()
                if fv == current and fnmatch.fnmatchcase(event_match_str, pattern)
            ]
            if not matching:
                raise UpcasterChainError(
                    f"Missing upcaster v{current} -> v{current + 1} for "
                    f"event_match={event_match_str!r}"
                )
            if len(matching) > 1:
                raise UpcasterChainError(
                    f"Multiple upcasters match (event_match={event_match_str!r}, "
                    f"from_version={current}): "
                    f"{[u.event_match for u in matching]}"
                )
            step = matching[0]
            if drift_callback is not None:
                live_hash = hash_function_source(step.fn)
                if live_hash != step.source_hash:
                    drift_callback(step, live_hash)
            event = step.fn(event)
            current += 1
            event = {**event, "schema_version": current}
        return event

    def upcast_to_current(
        self,
        event: dict[str, Any],
        event_match_str: str,
        *,
        drift_callback: Callable[[UpcasterVersion, str], None] | None = None,
    ) -> dict[str, Any]:
        """Upcast `event` all the way to the current schema for its match.

        Convenience over `current_version` + `apply_chain`: the normalise-on-read
        step every consumer outside `replay()` needs. Returns the event unchanged
        when no upcasters apply or it is already current.
        """
        target = self.current_version(event_match_str)
        return self.apply_chain(
            event, event_match_str, target, drift_callback=drift_callback
        )

    def all_upcasters(self) -> list[UpcasterVersion]:
        return list(self._upcasters.values())

    def rehydrate(self) -> None:
        if self._store is None:
            return
        for ident in self._persisted_ids:
            dotted_path = ident[2]
            module_name = dotted_path.rsplit(".", 1)[0]
            importlib.import_module(module_name)

    # ------------------------------------------------------------------
    # Internal — persistence (parallels HandlerRegistry)
    # ------------------------------------------------------------------

    def _ensure_stream(self) -> None:
        assert self._store is not None
        if not self._store.has(self._stream_path):
            self._store.create(self._stream_path)

    def _load_persisted_ids(self) -> None:
        assert self._store is not None
        messages, _ = self._store.read(self._stream_path)
        for msg in messages:
            try:
                payload = json.loads(msg.data)
            except (ValueError, UnicodeDecodeError):
                continue
            self._persisted_ids.add(_u_identity_from_payload(payload))

    def _persist_if_new(self, version: UpcasterVersion) -> None:
        assert self._store is not None
        ident = _u_identity(version)
        if ident in self._persisted_ids:
            return
        payload = {
            "event_match": version.event_match,
            "from_version": version.from_version,
            "dotted_path": version.dotted_path,
            "source_hash": version.source_hash,
        }
        self._store.append(
            self._stream_path,
            json.dumps(payload).encode("utf-8"),
        )
        self._persisted_ids.add(ident)


# =============================================================================
# Identity helpers
# =============================================================================


def _identity(
    v: HandlerVersion,
) -> tuple[str, str, int, int | None, str, str, str | None]:
    # match_field is appended LAST so dotted_path stays at index 4 (rehydrate()).
    return (
        v.name,
        v.event_match,
        v.effective_from,
        v.effective_to,
        v.dotted_path,
        v.source_hash,
        v.match_field,
    )


def _identity_from_payload(
    p: dict[str, Any],
) -> tuple[str, str, int, int | None, str, str, str | None]:
    return (
        p["name"],
        p["event_match"],
        int(p["effective_from"]),
        p["effective_to"] if p["effective_to"] is None else int(p["effective_to"]),
        p["dotted_path"],
        p["source_hash"],
        p.get("match_field"),  # tolerate pre-match_field persisted payloads
    )


def _u_identity(v: UpcasterVersion) -> tuple[str, int, str, str]:
    return (v.event_match, v.from_version, v.dotted_path, v.source_hash)


def _u_identity_from_payload(p: dict[str, Any]) -> tuple[str, int, str, str]:
    return (
        p["event_match"],
        int(p["from_version"]),
        p["dotted_path"],
        p["source_hash"],
    )


# =============================================================================
# Default registry + decorator
# =============================================================================


_default_registry = HandlerRegistry()
_default_upcaster_registry = UpcasterRegistry()


def get_default_registry() -> HandlerRegistry:
    """Return the process-wide default handler registry."""
    return _default_registry


def get_default_upcaster_registry() -> UpcasterRegistry:
    """Return the process-wide default upcaster registry."""
    return _default_upcaster_registry


def upcast(
    event: dict[str, Any],
    event_match: str,
    *,
    registry: UpcasterRegistry | None = None,
) -> dict[str, Any]:
    """Normalise `event` to the current schema for `event_match`.

    The discoverable one-liner over the upcaster chain::

        from rakaia import upcast
        normalised = upcast(raw_event, "submissions")

    Uses the process-wide default registry unless `registry` is given.
    """
    target = registry if registry is not None else _default_upcaster_registry
    return target.upcast_to_current(event, event_match)


def register_handler(
    name: str,
    event_match: str,
    effective_from: int,
    effective_to: int | None = None,
    *,
    match_field: str | None = None,
    registry: HandlerRegistry | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator that registers a handler version with the default registry.

    Pass `match_field` to route on a payload field (e.g. 'form_type') instead
    of the stream path.

    Example:
        @register_handler(
            name="mogrify",
            event_match="room:*:messages",
            effective_from=0,
            effective_to=10_000,
        )
        def mogrify_v1(event):
            return Effect(
                op="update_or_create",
                model_label="myapp.Room",
                lookup={"id": event["room_id"]},
                defaults={"name": event["name"]},
            )
    """
    target = registry if registry is not None else _default_registry

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        target.register(
            name=name,
            event_match=event_match,
            fn=fn,
            effective_from=effective_from,
            effective_to=effective_to,
            match_field=match_field,
        )
        return fn

    return decorator


def register_upcaster(
    event_match: str,
    from_version: int,
    *,
    registry: UpcasterRegistry | None = None,
) -> Callable[
    [Callable[[dict[str, Any]], dict[str, Any]]],
    Callable[[dict[str, Any]], dict[str, Any]],
]:
    """
    Decorator that registers an upcaster from `from_version` to `from_version+1`.

    Example:
        @register_upcaster(event_match="room:*:messages", from_version=1)
        def upcast_room_v1_to_v2(event: dict) -> dict:
            return {**event, "currency": "USD"}
    """
    target = registry if registry is not None else _default_upcaster_registry

    def decorator(
        fn: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> Callable[[dict[str, Any]], dict[str, Any]]:
        target.register(event_match=event_match, from_version=from_version, fn=fn)
        return fn

    return decorator
