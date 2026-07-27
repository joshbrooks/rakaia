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
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from .source_hash import hash_function_source

if TYPE_CHECKING:
    from .store import StreamStore

HANDLERS_META_STREAM = "__rakaia__:handlers"
UPCASTERS_META_STREAM = "__rakaia__:upcasters"
REDUCERS_META_STREAM = "__rakaia__:reducers"


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

    event_match: str | frozenset[str]
    """Glob pattern(s) matched against the routing subject. A ``str`` is a single
    glob (the common case); a ``frozenset`` matches the subject against **any**
    of its element globs, so one registration covers several unrelated
    form_types that share no glob prefix (e.g. ``{"TF_6_1_1", "SF_1_2"}``). Set
    at registration from a str or any iterable of strings."""

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

    stage: int = 0
    """Replay stage. Handlers run in ascending stage order; every event is fed
    through stage 0 in full before stage 1 begins, and a stage > 0 handler is
    called with a read-only projection reader as its second argument so it can
    resolve facts materialised by earlier stages. Default 0 = single-stage,
    called with just the event (backward compatible)."""


@dataclass(frozen=True)
class ReducerVersion:
    """A per-stage reduce step: recompute an aggregate once from the projections.

    Unlike a handler, a reducer is not matched or called per event. During
    staged replay it runs **once** per its stage, after that stage's per-event
    handlers have committed, and is invoked as `fn(reader)` — reading the
    accumulated projections (e.g. all contributing rows for a group) and
    returning the idempotent Effects that materialise the aggregate. It is the
    replay-time hook that `reconcile_aggregate` is designed to fill.

    **Optional touched-subjects arg.** A reducer that declares a second
    parameter is invoked as `fn(reader, touched)`, where ``touched`` is the
    deterministic, deduplicated tuple of `rakaia.TouchedSubject`s the pass's
    per-event handlers wrote (their effects' ``(model_label, lookup)``). This
    lets one reducer serve both paths: scope the recompute to ``touched`` on an
    incremental forward replay, or ignore it and recompute everything on a full
    rebuild. A plain ``fn(reader)`` reducer is unaffected (the arg is detected by
    signature, mirroring how a stage > 0 handler opts in to the reader).

    **Replacement semantics — last-write-wins by name, not seq-versioned.**
    A reducer is a single *current* definition keyed by `name`, unlike a handler,
    which is a series of `[from_seq, to_seq)`-bracketed versions. Registering a
    different function under an existing name **replaces** the prior one. This is
    intentional: a reducer recomputes an aggregate *wholesale* from the committed
    projections on every replay, so there is no per-sequence window a reducer
    could be versioned over — the only thing that matters is the definition in
    force when replay runs. If you need two coexisting reduce steps, give them
    distinct names.
    """

    name: str
    """Reducer name, unique within a registry."""

    stage: int
    """The stage this reducer runs in (after that stage's event handlers)."""

    fn: Callable[..., Any]
    """The reducer callable: ``reader -> ...`` or ``reader, touched -> ...``,
    returning ``Effect | list[Effect] | None``."""

    dotted_path: str
    source_hash: str


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
        self._versions: dict[
            tuple[str, str | frozenset[str]], list[HandlerVersion]
        ] = {}
        # reducer name -> ReducerVersion
        self._reducers: dict[str, ReducerVersion] = {}
        self._store = store
        self._stream_path = stream_path
        self._reducer_stream_path = REDUCERS_META_STREAM
        # Identity tuples already persisted to the stream — used for dedup.
        self._persisted_ids: set[
            tuple[str, str | frozenset[str], int, int | None, str, str, str | None, int]
        ] = set()
        self._reducer_persisted_ids: set[tuple[str, int, str, str]] = set()
        if store is not None:
            self._ensure_stream()
            self._load_persisted_ids()
            self._ensure_reducer_stream()
            self._load_reducer_ids()

    def register(
        self,
        name: str,
        event_match: str | Iterable[str],
        fn: Callable[..., Any],
        effective_from: int,
        effective_to: int | None = None,
        *,
        match_field: str | None = None,
        stage: int = 0,
    ) -> HandlerVersion:
        """
        Register a handler version. Raises HandlerOverlapError on overlap.

        Re-registering with identical (name, event_match, from, to,
        dotted_path, source_hash, match_field, stage) is a no-op that returns
        the existing version without re-appending to the meta-stream.

        `event_match` is a glob string or an iterable of glob strings; a
        collection matches the subject against **any** of its members, so one
        registration covers several unrelated form_types that share no glob
        (canonicalised to a `frozenset`, so member order does not matter). When
        `match_field` is set, the pattern(s) are matched against
        `event[match_field]` (e.g. 'form_type') instead of the stream path.

        `stage` (default 0) controls replay ordering: replay runs stages in
        ascending order, and a stage > 0 handler is invoked as `fn(event,
        reader)` so it can read earlier stages' projections.
        """
        event_match = _canonical_event_match(event_match)
        if effective_from < 0:
            raise ValueError(
                f"effective_from must be non-negative, got {effective_from}"
            )
        if effective_to is not None and effective_to <= effective_from:
            raise ValueError(
                f"effective_to ({effective_to}) must be > effective_from "
                f"({effective_from})"
            )
        if stage < 0:
            raise ValueError(f"stage must be non-negative, got {stage}")

        version = HandlerVersion(
            name=name,
            event_match=event_match,
            effective_from=effective_from,
            effective_to=effective_to,
            fn=fn,
            dotted_path=f"{fn.__module__}.{fn.__qualname__}",
            source_hash=hash_function_source(fn),
            match_field=match_field,
            stage=stage,
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

    def register_reducer(
        self,
        name: str,
        stage: int,
        fn: Callable[..., Any],
    ) -> ReducerVersion:
        """Register a per-stage reduce step (runs once at `stage`, `fn(reader)`).

        Re-registering with identical (name, stage, dotted_path, source_hash) is
        a no-op. Registering a *different* function under an existing name
        replaces it (a reducer is a single current definition, not a version
        series) and appends a new audit event.
        """
        if stage < 0:
            raise ValueError(f"stage must be non-negative, got {stage}")

        reducer = ReducerVersion(
            name=name,
            stage=stage,
            fn=fn,
            dotted_path=f"{fn.__module__}.{fn.__qualname__}",
            source_hash=hash_function_source(fn),
        )

        existing = self._reducers.get(name)
        if existing is not None and _reducer_identity(existing) == _reducer_identity(
            reducer
        ):
            return existing

        if self._store is not None:
            self._persist_reducer_if_new(reducer)

        self._reducers[name] = reducer
        return reducer

    def reset(self) -> None:
        """Clear all in-memory registrations (handlers, reducers, and the
        persisted-id dedup caches) so a fresh set can be registered.

        This resets **in-memory registration state only** — it is the seam for
        per-test isolation (see `reset_default_registries()` and the test-isolation
        section of ``docs/versioned-handlers.md``). It does **not** delete the
        durable meta-streams (``__rakaia__:handlers`` / ``__rakaia__:reducers``);
        to discard those, delete the streams via the store. On a store-backed
        registry, clearing the dedup cache means the next `register()` re-appends
        an audit event even for an already-persisted registration.
        """
        self._versions.clear()
        self._reducers.clear()
        self._persisted_ids.clear()
        self._reducer_persisted_ids.clear()

    def reducers_for_stage(self, stage: int) -> list[ReducerVersion]:
        """Reducers registered for `stage`, in deterministic (name) order."""
        return sorted(
            (r for r in self._reducers.values() if r.stage == stage),
            key=lambda r: r.name,
        )

    def all_reducers(self) -> list[ReducerVersion]:
        """Every registered reducer (test/debug helper)."""
        return list(self._reducers.values())

    def resolve(
        self,
        event_match: str,
        seq: int,
        event: dict[str, Any] | None = None,
        *,
        stage: int | None = None,
    ) -> list[HandlerVersion]:
        """
        Return all handler versions whose pattern matches AND whose [from, to)
        range covers seq. By default the pattern is matched against the
        `event_match` string (the stream path); a series registered with a
        `match_field` is instead matched against `event[match_field]`, so pass
        the decoded `event` when any content-routed handlers exist.

        When `stage` is given, only versions in that stage are returned; the
        coverage check still runs across every matching series, so a genuine
        gap surfaces regardless of which stage is being resolved.

        Raises HandlerGapError if any matching handler series has no version
        covering seq (a gap, not "no handlers").
        """
        if seq < 0:
            raise ValueError(f"seq must be non-negative, got {seq}")

        resolved: list[HandlerVersion] = []
        for (name, pattern), versions in self._versions.items():
            subject = self._match_subject(name, versions, event_match, event)
            if not _pattern_matches(pattern, subject):
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
            if stage is None or covering[0].stage == stage:
                resolved.append(covering[0])
        return resolved

    def stages(self) -> list[int]:
        """Sorted list of the distinct stages any handler or reducer declares.

        `[0]` (or `[]` when empty) means single-stage replay; more than one
        entry — or any reducer — drives staged replay in ascending order.
        """
        return sorted(
            {v.stage for v in self.all_versions()}
            | {r.stage for r in self._reducers.values()}
        )

    def has_reducers(self) -> bool:
        """Whether any reducer is registered (reducers require staged replay)."""
        return bool(self._reducers)

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
        if not versions:
            # A registration that failed after setdefault() can leave an empty
            # series behind; fall back to stream-path routing rather than
            # IndexError on versions[0].
            return event_match
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
        for r_ident in self._reducer_persisted_ids:
            module_name = r_ident[2].rsplit(".", 1)[0]  # index 2 = dotted_path
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
            # A frozenset event_match serializes as a sorted list so the
            # meta-stream is stable regardless of set iteration order.
            "event_match": (
                version.event_match
                if isinstance(version.event_match, str)
                else sorted(version.event_match)
            ),
            "effective_from": version.effective_from,
            "effective_to": version.effective_to,
            "dotted_path": version.dotted_path,
            "source_hash": version.source_hash,
            "match_field": version.match_field,
            "stage": version.stage,
        }
        self._store.append(
            self._stream_path,
            json.dumps(payload).encode("utf-8"),
        )
        self._persisted_ids.add(ident)

    # ------------------------------------------------------------------
    # Internal — reducer persistence (parallels the handler helpers)
    # ------------------------------------------------------------------

    def _ensure_reducer_stream(self) -> None:
        assert self._store is not None
        if not self._store.has(self._reducer_stream_path):
            self._store.create(self._reducer_stream_path)

    def _load_reducer_ids(self) -> None:
        assert self._store is not None
        messages, _ = self._store.read(self._reducer_stream_path)
        for msg in messages:
            try:
                payload = json.loads(msg.data)
            except (ValueError, UnicodeDecodeError):
                continue
            self._reducer_persisted_ids.add(_reducer_identity_from_payload(payload))

    def _persist_reducer_if_new(self, reducer: ReducerVersion) -> None:
        assert self._store is not None
        ident = _reducer_identity(reducer)
        if ident in self._reducer_persisted_ids:
            return
        payload = {
            "name": reducer.name,
            "stage": reducer.stage,
            "dotted_path": reducer.dotted_path,
            "source_hash": reducer.source_hash,
        }
        self._store.append(
            self._reducer_stream_path,
            json.dumps(payload).encode("utf-8"),
        )
        self._reducer_persisted_ids.add(ident)

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

    match_field: str | None = None
    """When set, the ``event_match`` pattern is tested against
    ``event[match_field]`` (content routing) instead of the stream/event-match
    string — mirrors ``register_handler``, so a per-form upcaster can route on a
    per-entity stream (e.g. ``submissions:<uuid>``) whose path carries no type."""


class UpcasterRegistry:
    """
    Registry of schema-version upcasters, keyed by (event_match, from_version,
    match_field).

    Each upcaster transforms an event dict from schema_version N to N+1. The
    registry composes them into a chain via `apply_chain`.
    """

    def __init__(
        self,
        store: StreamStore | None = None,
        *,
        stream_path: str = UPCASTERS_META_STREAM,
    ) -> None:
        self._upcasters: dict[tuple[str, int, str | None], UpcasterVersion] = {}
        self._store = store
        self._stream_path = stream_path
        self._persisted_ids: set[tuple[str, int, str, str, str | None]] = set()
        if store is not None:
            self._ensure_stream()
            self._load_persisted_ids()

    def register(
        self,
        event_match: str,
        from_version: int,
        fn: Callable[[dict[str, Any]], dict[str, Any]],
        *,
        match_field: str | None = None,
    ) -> UpcasterVersion:
        """Register an upcaster from `from_version` to `from_version + 1`.

        When `match_field` is set, the `event_match` pattern is tested against
        `event[match_field]` (content routing) instead of the stream/event-match
        string, so a per-form upcaster can route on a per-entity stream.

        A stream-routed and a content-routed upcaster may share the same
        `(event_match, from_version)` — `match_field` is part of the identity
        key, so they are distinct registrations, not a conflict. A conflict
        is only raised when `(event_match, from_version, match_field)` are
        all identical but the function/hash differ.
        """
        if from_version < 1:
            raise ValueError(f"from_version must be >= 1, got {from_version}")

        new = UpcasterVersion(
            event_match=event_match,
            from_version=from_version,
            fn=fn,
            dotted_path=f"{fn.__module__}.{fn.__qualname__}",
            source_hash=hash_function_source(fn),
            match_field=match_field,
        )

        key = (event_match, from_version, match_field)
        existing = self._upcasters.get(key)
        if existing is not None:
            if _u_identity(existing) == _u_identity(new):
                return existing
            raise UpcasterConflictError(
                f"Upcaster already registered for event_match={event_match!r}, "
                f"from_version={from_version}, match_field={match_field!r} "
                f"(existing dotted_path={existing.dotted_path!r}, "
                f"new dotted_path={new.dotted_path!r})."
            )

        if self._store is not None:
            self._persist_if_new(new)
        self._upcasters[key] = new
        return new

    @staticmethod
    def _subject(
        up: UpcasterVersion, event: dict[str, Any] | None, event_match_str: str
    ) -> str:
        """The string an upcaster's pattern is matched against: a content field
        when `match_field` is set (mirrors `HandlerRegistry._match_subject`), else
        the stream/event-match string.

        A content-routed upcaster matched with `event=None` raises — like handler
        dispatch — to catch a caller who forgot to pass the event (which would
        otherwise leave the event silently un-upcast). An event that is present
        but lacks the field yields "" and simply does not match (a different
        form_type on the stream is normal, not an error)."""
        if up.match_field is not None:
            if event is None:
                raise ValueError(
                    f"Upcaster (event_match={up.event_match!r}) routes on "
                    f"match_field={up.match_field!r} but was matched without an "
                    "event; pass the decoded event to current_version()/"
                    "apply_chain() (or use upcast_to_current)."
                )
            return str(event.get(up.match_field, ""))
        return event_match_str

    def current_version(
        self, event_match_str: str, event: dict[str, Any] | None = None
    ) -> int:
        """Highest schema version reachable for an event matching this string.

        Pass `event` so content-routed upcasters (registered with `match_field`)
        match against `event[match_field]`; without it they are skipped.
        """
        matching_froms = [
            fv
            for (pattern, fv, _match_field), up in self._upcasters.items()
            if fnmatch.fnmatchcase(self._subject(up, event, event_match_str), pattern)
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
        (default 1). Each step finds the upcaster matching the event_match (or
        `event[match_field]` for content-routed upcasters) and the current
        version, then bumps schema_version on the result.

        Content-routing note: each step is re-matched against the *progressively
        upcasted* event, so a content-routed chain's `match_field` discriminator
        must stay stable across the chain. An upcaster that renames or rewrites
        the discriminator (e.g. v1→v2 renaming `form_type`) re-routes subsequent
        steps off the new value — keep the discriminator field constant, or route
        those steps on the stream path.

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
                for (pattern, fv, _match_field), up in self._upcasters.items()
                if fv == current
                and fnmatch.fnmatchcase(
                    self._subject(up, event, event_match_str), pattern
                )
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
        target = self.current_version(event_match_str, event)
        return self.apply_chain(
            event, event_match_str, target, drift_callback=drift_callback
        )

    def all_upcasters(self) -> list[UpcasterVersion]:
        return list(self._upcasters.values())

    def reset(self) -> None:
        """Clear all in-memory upcaster registrations and the dedup cache.

        In-memory state only — mirrors `HandlerRegistry.reset()`: the seam for
        per-test isolation, not a delete of the durable ``__rakaia__:upcasters``
        meta-stream (delete that via the store if you need to).
        """
        self._upcasters.clear()
        self._persisted_ids.clear()

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
            "match_field": version.match_field,
        }
        self._store.append(
            self._stream_path,
            json.dumps(payload).encode("utf-8"),
        )
        self._persisted_ids.add(ident)


# =============================================================================
# event_match helpers
# =============================================================================


def _canonical_event_match(event_match: str | Iterable[str]) -> str | frozenset[str]:
    """Normalise an `event_match` argument to its stored form.

    A plain string is kept as-is (the common single-glob case). Any other
    iterable of strings becomes a ``frozenset`` — hashable (so it works as the
    series key and in the identity tuple) and order-independent (so the same
    members in a different order dedup). Raises on an empty collection or a
    non-string member, which are always mistakes.
    """
    if isinstance(event_match, str):
        return event_match
    members = frozenset(event_match)
    if not members:
        raise ValueError(
            "event_match collection must contain at least one pattern (got empty)"
        )
    if not all(isinstance(m, str) for m in members):
        raise ValueError("event_match collection members must all be strings")
    return members


def _pattern_matches(pattern: str | frozenset[str], subject: str) -> bool:
    """Whether `subject` matches `pattern` — a single glob, or any glob in a set."""
    if isinstance(pattern, str):
        return fnmatch.fnmatchcase(subject, pattern)
    return any(fnmatch.fnmatchcase(subject, p) for p in pattern)


# =============================================================================
# Identity helpers
# =============================================================================


def _identity(
    v: HandlerVersion,
) -> tuple[str, str | frozenset[str], int, int | None, str, str, str | None, int]:
    # match_field / stage are appended after dotted_path (index 4) and
    # source_hash (index 5) so rehydrate()'s ident[4] lookup stays valid.
    return (
        v.name,
        v.event_match,
        v.effective_from,
        v.effective_to,
        v.dotted_path,
        v.source_hash,
        v.match_field,
        v.stage,
    )


def _identity_from_payload(
    p: dict[str, Any],
) -> tuple[str, str | frozenset[str], int, int | None, str, str, str | None, int]:
    return (
        p["name"],
        # Reconstruct the same canonical (frozenset) identity a list-serialized
        # set was persisted from, so a reload dedups against the live version.
        p["event_match"]
        if isinstance(p["event_match"], str)
        else frozenset(p["event_match"]),
        int(p["effective_from"]),
        p["effective_to"] if p["effective_to"] is None else int(p["effective_to"]),
        p["dotted_path"],
        p["source_hash"],
        p.get("match_field"),  # tolerate pre-match_field persisted payloads
        int(p.get("stage", 0)),  # tolerate pre-stage persisted payloads
    )


def _reducer_identity(r: ReducerVersion) -> tuple[str, int, str, str]:
    return (r.name, r.stage, r.dotted_path, r.source_hash)


def _reducer_identity_from_payload(p: dict[str, Any]) -> tuple[str, int, str, str]:
    return (p["name"], int(p["stage"]), p["dotted_path"], p["source_hash"])


def _u_identity(v: UpcasterVersion) -> tuple[str, int, str, str, str | None]:
    # match_field appended last so rehydrate()'s ident[2] (dotted_path) stays valid.
    return (v.event_match, v.from_version, v.dotted_path, v.source_hash, v.match_field)


def _u_identity_from_payload(
    p: dict[str, Any],
) -> tuple[str, int, str, str, str | None]:
    return (
        p["event_match"],
        int(p["from_version"]),
        p["dotted_path"],
        p["source_hash"],
        p.get("match_field"),  # tolerate pre-match_field persisted payloads
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


def reset_default_registries() -> None:
    """Reset both process-wide default registries (handlers + upcasters).

    The one call a test's teardown needs to stop registrations leaking between
    tests when they use the default registries (via the bare `@register_handler`
    / `@register_upcaster` / `@register_reducer` decorators). Prefer constructing
    fresh `HandlerRegistry()` / `UpcasterRegistry()` instances and injecting them
    into `replay()` where you can; use this when the code under test registers
    against the module-global default. See the test-isolation section of
    ``docs/versioned-handlers.md``.

    Resets in-memory registration state only — it does not touch any durable
    meta-streams (see `HandlerRegistry.reset()`).
    """
    _default_registry.reset()
    _default_upcaster_registry.reset()


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
    event_match: str | Iterable[str],
    effective_from: int,
    effective_to: int | None = None,
    *,
    match_field: str | None = None,
    stage: int = 0,
    registry: HandlerRegistry | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator that registers a handler version with the default registry.

    `event_match` is a glob string or an iterable of glob strings; a collection
    matches any of its members, so one registration covers several unrelated
    form_types (e.g. `{"TF_6_1_1", "SF_1_2"}`) without a per-value loop. Pass
    `match_field` to route on a payload field (e.g. 'form_type') instead of the
    stream path. Pass `stage` (default 0) to place the handler in a replay stage;
    stage > 0 handlers are called `fn(event, reader)`.

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

        # One registration for several form_types that share no glob:
        @register_handler(
            name="sweep",
            event_match={"TF_6_1_1", "SF_1_2", "POM_1"},
            effective_from=0,
            match_field="form_type",
        )
        def sweep(event): ...
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
            stage=stage,
        )
        return fn

    return decorator


def register_simple(
    name: str,
    event_match: str | Iterable[str],
    *,
    match_field: str | None = None,
    stage: int = 0,
    registry: HandlerRegistry | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """Register an always-on handler — the common "just project" case.

    Shorthand for ``register_handler(name, event_match, effective_from=0,
    effective_to=None, ...)``: one open-ended version covering every sequence,
    with no seq-range ceremony. Reach for `register_handler` only when you
    actually need version brackets (a handler whose body changes at a known
    sequence). `match_field` and `stage` pass straight through.

    Example:
        @register_simple("project_registry", "TF_6_1_1", match_field="form_type")
        def project_registry(event):
            return Effect(op="update_or_create", ...)
    """
    return register_handler(
        name,
        event_match,
        effective_from=0,
        effective_to=None,
        match_field=match_field,
        stage=stage,
        registry=registry,
    )


def register_reducer(
    name: str,
    stage: int,
    *,
    registry: HandlerRegistry | None = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator that registers a per-stage reduce step with the default registry.

    The decorated function is called `fn(reader)` once during staged replay,
    after `stage`'s per-event handlers commit, and returns the Effects that
    materialise the aggregate (typically via `reconcile_aggregate`). Declare a
    second parameter — `fn(reader, touched)` — to also receive the tuple of
    `rakaia.TouchedSubject`s the pass's handlers wrote, so the same reducer can
    scope its recompute to what changed (incremental) or recompute everything
    (full rebuild). The arg is detected by signature; `fn(reader)` is unchanged.

    A reducer is a single current definition keyed by `name` (last-write-wins),
    not a seq-versioned series like a handler — see `ReducerVersion`.

    Example:
        @register_reducer(name="balance", stage=1)
        def balance(reader):
            groups = _recompute_totals(reader)
            return reconcile_aggregate("ida.Balance", {}, "suku", groups)

        # touched-aware: only the groups this pass changed
        @register_reducer(name="balance", stage=1)
        def balance(reader, touched):
            sukus = {r.lookup["suku"] for r in touched if r.model_label == "ida.Line"}
            if not sukus:
                return []  # nothing changed this pass — a no-op, not a clear
            groups = _recompute_totals(reader, only=sukus)
            return reconcile_aggregate("ida.Balance", {}, "suku", groups)
    """
    target = registry if registry is not None else _default_registry

    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        target.register_reducer(name=name, stage=stage, fn=fn)
        return fn

    return decorator


def register_upcaster(
    event_match: str,
    from_version: int,
    *,
    match_field: str | None = None,
    registry: UpcasterRegistry | None = None,
) -> Callable[
    [Callable[[dict[str, Any]], dict[str, Any]]],
    Callable[[dict[str, Any]], dict[str, Any]],
]:
    """
    Decorator that registers an upcaster from `from_version` to `from_version+1`.

    By default `event_match` is matched against the stream/event-match string.
    Set `match_field` to match it against `event[match_field]` instead (content
    routing, mirroring `register_handler`) — so a per-form upcaster can route on
    a per-entity stream (e.g. `submissions:<uuid>`) whose path carries no type.

    Example:
        @register_upcaster(event_match="room:*:messages", from_version=1)
        def upcast_room_v1_to_v2(event: dict) -> dict:
            return {**event, "currency": "USD"}

        @register_upcaster(event_match="TF_13_2_1", from_version=1,
                           match_field="form_type")
        def upcast_tf1321_v1_to_v2(event: dict) -> dict:
            return {**event, ...}

    **Upcasters rewrite history — the contract.** An upcaster is keyed by
    ``(event_match, from_version)``, *not* by a stream sequence range. It is not
    versioned the way a handler is. So editing the body of a shipped upcaster
    retroactively changes the effective shape of **every** historical event at
    that schema step: the next replay upcasts them through the new code, not the
    code that was live when they were written. Source-hash drift **detects** this
    (replay warns, or raises under ``on_drift="raise"``) but does **not** prevent
    it. The rule: once events exist at a schema version, treat that version's
    upcaster body as append-only history — evolve the schema by adding a **new**
    ``from_version`` step, never by editing a shipped one in place.
    """
    target = registry if registry is not None else _default_upcaster_registry

    def decorator(
        fn: Callable[[dict[str, Any]], dict[str, Any]],
    ) -> Callable[[dict[str, Any]], dict[str, Any]]:
        target.register(
            event_match=event_match,
            from_version=from_version,
            fn=fn,
            match_field=match_field,
        )
        return fn

    return decorator
