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
import inspect
import warnings
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

# `HandlerDriftError` is defined next to the rule that raises it, in
# `rakaia.drift`, and re-exported here so `rakaia.registry.HandlerDriftError` —
# where callers have always imported it from — keeps working.
from .drift import DriftLedger
from .drift import HandlerDriftError as HandlerDriftError
from .registration_log import RegistrationLog
from .source_hash import hash_function_source, is_importable, unwrap_handler

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


# =============================================================================
# One rule per rule
# =============================================================================
#
# Everything in this section exists once and is used by every registration kind.
# Each of these used to be written out per kind — content routing twice, the
# frozenset serialization rule three times, and identity/payload/identity-again
# nine times across three record types. `rakaia.registration_log` did the same
# consolidation one level down and its module docstring says why.


def _canonical_event_match(event_match: str | Iterable[str]) -> str | frozenset[str]:
    """Normalise an `event_match` argument to its stored form.

    A plain string is kept as-is (the common single-glob case). Any other
    iterable of strings becomes a ``frozenset`` — hashable (so it works as the
    series key and in the identity tuple) and order-independent (so the same
    members in a different order dedup). Raises on an empty collection or a
    non-string member, which are always mistakes.

    Also the **decoder** for a persisted `event_match`, which is a string or a
    list: reading a payload back is the same normalisation as accepting an
    argument, so there is no second implementation to keep in step.
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


def _event_match_payload(event_match: str | frozenset[str]) -> str | list[str]:
    """The JSON form of a stored `event_match`.

    A frozenset serializes as a **sorted** list, so the meta-stream is stable
    regardless of set iteration order — otherwise every restart would see a
    "new" identity and re-append the same handler. `_canonical_event_match`
    reads it back.
    """
    return event_match if isinstance(event_match, str) else sorted(event_match)


def _pattern_matches(pattern: str | frozenset[str], subject: str) -> bool:
    """Whether `subject` matches `pattern` — a single glob, or any glob in a set."""
    if isinstance(pattern, str):
        return fnmatch.fnmatchcase(subject, pattern)
    return any(fnmatch.fnmatchcase(subject, p) for p in pattern)


def _routing_subject(
    match_field: str | None,
    event: dict[str, Any] | None,
    stream_subject: str,
    *,
    registrant: str,
    remedy: str,
) -> str:
    """The string a registration's glob is tested against — the content-routing
    rule, for handlers and upcasters alike.

    Without a `match_field` the subject is the stream/event-match string and the
    event is irrelevant. With one, it is ``str(event[match_field])``.

    Two edges carry the weight, and both are the same for both kinds:

    * the field is **absent** from an event that is present → ``""``, which
      simply does not match. A stream carrying a different form_type is normal
      traffic, not a configuration error.
    * the **event** is absent → `ValueError`. A caller who forgot to pass it
      would otherwise silently leave the event unhandled or un-upcast.

    `registrant` and `remedy` only shape that error, so each caller keeps the
    wording that names the right entry point.
    """
    if match_field is None:
        return stream_subject
    if event is None:
        raise ValueError(
            f"{registrant} routes on match_field={match_field!r} but {remedy}"
        )
    return str(event.get(match_field, ""))


def _module_prefix(dotted_path: str | None) -> str | None:
    """The module part of a dotted path, by chopping the last segment.

    Only correct when the path is ``module.function``, which is why it is no
    longer how `rehydrate()` finds a module — see `_registration_site`. Kept as
    the fallback for meta-stream payloads written before `registered_in`
    existed.
    """
    if not dotted_path or "." not in dotted_path:
        return dotted_path or None
    return dotted_path.rsplit(".", 1)[0]


def _registration_site() -> str | None:
    """The module that made this registration — what `rehydrate()` re-imports.

    Restoring a registration means re-running its **decorator**, so the module
    that matters is the one holding the decorator call, not the one holding the
    function. Those are different whenever a shared function is wired up
    elsewhere — which `functools.partial(fn, **deps)` dependency binding makes
    the normal layout, since the partial is built at the wiring site while `fn`
    lives in a library module.

    Walks out of rakaia's own frames, so it lands on the caller whether they
    used `@register_handler`, `@register_simple`, or called `register()`
    directly.
    """
    frame = inspect.currentframe()
    try:
        while frame is not None:
            module = frame.f_globals.get("__name__")
            if module is not None and not (
                module == "rakaia" or module.startswith("rakaia.")
            ):
                return module
            frame = frame.f_back
        return None
    finally:
        # Frames hold references to their locals, which include this one.
        del frame


_REQUIRED: Any = object()


@dataclass(frozen=True)
class _PayloadField:
    """One persisted field of a registration record, and how it crosses JSON.

    Declaring the fields is what lets `_MetaStreamRecord` derive `identity`,
    `to_payload` and `identity_from_payload` from a single list per record type
    instead of three hand-written methods that had to agree.
    """

    name: str
    encode: Callable[[Any], Any] = lambda value: value
    decode: Callable[[Any], Any] = lambda value: value
    default: Callable[[dict[str, Any]], Any] = _REQUIRED
    """Value for a payload written before this field existed. Meta-streams
    already in the wild still have to load, so every field added after the
    format shipped needs one."""

    def read(self, payload: dict[str, Any]) -> Any:
        if self.name in payload:
            return self.decode(payload[self.name])
        if self.default is _REQUIRED:
            raise KeyError(f"registration payload is missing {self.name!r}")
        return self.default(payload)


def _as_int(value: Any) -> int:
    return int(value)


def _as_int_or_none(value: Any) -> int | None:
    return None if value is None else int(value)


class _MetaStreamRecord:
    """`identity` / `to_payload` / `identity_from_payload`, derived once.

    Satisfies `rakaia.registration_log.RegistrationRecord` for any subclass that
    declares `_PAYLOAD_FIELDS`. The three methods were written out per record
    type — an identity built in one place and rebuilt in another, three times
    over — which is exactly the pair that drifts. Here the round trip is a
    property of the field list, so adding a field is one line in one place.
    """

    _PAYLOAD_FIELDS: ClassVar[tuple[_PayloadField, ...]] = ()

    @property
    def identity(self) -> tuple:
        """Dedup key for the meta-stream. Excludes the callable itself (not
        serializable, and two registrations of the same source are the same
        registration)."""
        return tuple(getattr(self, f.name) for f in self._PAYLOAD_FIELDS)

    def to_payload(self) -> dict[str, Any]:
        return {f.name: f.encode(getattr(self, f.name)) for f in self._PAYLOAD_FIELDS}

    @classmethod
    def identity_from_payload(cls, payload: dict[str, Any]) -> tuple:
        return tuple(f.read(payload) for f in cls._PAYLOAD_FIELDS)


#: The registration-site field, shared by all three record kinds. The fallback
#: is the old derivation — right whenever the decoration site and the definition
#: site were the same module, which is the case that used to work.
_REGISTERED_IN = _PayloadField(
    "registered_in", default=lambda p: _module_prefix(p.get("dotted_path"))
)


# =============================================================================
# Data structures
# =============================================================================


def _dotted_path(fn: Any, *, persisted: bool = False) -> str:
    """The importable path recorded for `fn`, warning when there isn't one.

    Taken from the *unwrapped* callable so a `functools.partial` records the
    function it binds rather than being unrecordable. This is "where the logic
    is", for drift detection; "where it was registered" is a separate field
    (`registered_in`, from `_registration_site`), because those are not the same
    module once dependencies are bound at a wiring site.

    A closure or a nested lambda has a `<locals>` qualname. Its `source_hash`
    then covers only the wrapper, so drift detection is blind to whatever the
    wrapper calls — and `rehydrate()` restores it only if the module that
    registered it re-registers it on import, which a closure built inside a
    function does not.

    Warned rather than refused: a closure is legitimate in a test, and rakaia's
    own suite registers lambdas. The point is that it stops being silent. Bind
    dependencies with `functools.partial` instead, which records the wrapped
    function and keeps both properties.

    Only warned when the registry **persists** (`persisted=True`). A registry
    with no backing store records nothing and restores nothing, so an
    unimportable path costs it nothing — warning there would fire on every
    throwaway registry in every test suite, which is how a real warning gets
    filtered out and stops being read. The drift caveat applies either way, and
    is stated in the message.
    """
    target = unwrap_handler(fn)
    path = f"{target.__module__}.{target.__qualname__}"
    if persisted and not is_importable(fn):
        warnings.warn(
            f"Handler {path!r} is not importable: drift detection covers only "
            f"the wrapper, not the function it calls, and `rehydrate()` can "
            f"restore it only if the module that registered it re-registers on "
            f"import. Bind dependencies with functools.partial(fn, **deps) "
            f"instead of a closure.",
            UserWarning,
            stacklevel=3,
        )
    return path


@dataclass(frozen=True)
class HandlerVersion(_MetaStreamRecord):
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
    """fn.__module__ + '.' + fn.__qualname__ — *where the logic is*, taken from
    the unwrapped callable. Used for drift reporting, not for restoring."""

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

    registered_in: str | None = None
    """The module that made this registration — *where the decorator ran*, which
    is what `rehydrate()` imports. Separate from `dotted_path` because binding a
    dependency with `functools.partial` puts the wiring in one module and the
    function in another. Set from the calling frame; see `_registration_site`."""

    # -- meta-stream record (see `rakaia.registration_log`) -----------------
    #
    # Identity and serialization are derived from this list by
    # `_MetaStreamRecord`, so adding a field is one line here rather than three
    # methods that have to agree. Fields added after the format shipped carry a
    # `default`, because meta-streams written without them still have to load.
    # Order is the identity tuple's order; append, don't insert.

    _PAYLOAD_FIELDS: ClassVar[tuple[_PayloadField, ...]] = (
        _PayloadField("name"),
        _PayloadField(
            "event_match", encode=_event_match_payload, decode=_canonical_event_match
        ),
        _PayloadField("effective_from", decode=_as_int),
        _PayloadField("effective_to", decode=_as_int_or_none),
        _PayloadField("dotted_path"),
        _PayloadField("source_hash"),
        _PayloadField("match_field", default=lambda _p: None),
        _PayloadField("stage", decode=_as_int, default=lambda _p: 0),
        _REGISTERED_IN,
    )


@dataclass(frozen=True)
class ReducerVersion(_MetaStreamRecord):
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

    registered_in: str | None = None
    """The module that made this registration — see `HandlerVersion`."""

    # -- meta-stream record (see `rakaia.registration_log`) -----------------

    _PAYLOAD_FIELDS: ClassVar[tuple[_PayloadField, ...]] = (
        _PayloadField("name"),
        _PayloadField("stage", decode=_as_int),
        _PayloadField("dotted_path"),
        _PayloadField("source_hash"),
        _REGISTERED_IN,
    )


# =============================================================================
# Registry
# =============================================================================


class _LogBackedRegistry:
    """The meta-stream half of a registry, once for both of them.

    A registry keeps one `RegistrationLog` per record kind it owns (handlers
    keep two — handlers and reducers — upcasters keep one). Loading them,
    clearing them and re-importing the modules they name is the same work in
    each case, and used to be written out in each.
    """

    _logs: tuple[RegistrationLog, ...]

    def _load_logs(self) -> None:
        for log in self._logs:
            log.load()

    def _reset_logs(self) -> None:
        for log in self._logs:
            log.reset()

    def rehydrate(self) -> None:
        """Import the module that made each recorded registration, so its
        `@register_*` decorator runs again and re-registers here.

        No-op without a backing store. Modules are imported once (Python caches
        imports), so calling this after some are already imported is safe — but
        note that it therefore restores nothing for a module already imported in
        this process, which is the normal case outside a fresh start.
        """
        modules: set[str] = set()
        for log in self._logs:
            modules |= log.modules()
        for module_name in modules:
            importlib.import_module(module_name)


class HandlerRegistry(_LogBackedRegistry):
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
        # One log per record kind. Each owns create/read/dedup/append for its
        # stream; `RegistrationLog` is a no-op when `store` is None, so nothing
        # below needs to special-case an unbacked registry.
        self._handler_log = RegistrationLog(store, stream_path, HandlerVersion)
        self._reducer_log = RegistrationLog(
            store, self._reducer_stream_path, ReducerVersion
        )
        self._logs = (self._handler_log, self._reducer_log)
        self._load_logs()

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
            dotted_path=_dotted_path(fn, persisted=self._store is not None),
            source_hash=hash_function_source(fn),
            registered_in=_registration_site(),
            match_field=match_field,
            stage=stage,
        )

        key = (name, event_match)
        series = self._versions.setdefault(key, [])

        # In-memory dedup: exact identical registration → no-op
        for existing in series:
            if existing.identity == version.identity:
                return existing

        self._check_overlap(version, series)

        self._handler_log.record(version)

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
            dotted_path=_dotted_path(fn, persisted=self._store is not None),
            source_hash=hash_function_source(fn),
            registered_in=_registration_site(),
        )

        existing = self._reducers.get(name)
        if existing is not None and existing.identity == reducer.identity:
            return existing

        self._reducer_log.record(reducer)

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
        self._reset_logs()

    def reducers_for_stage(self, stage: int | None) -> list[ReducerVersion]:
        """Reducers registered for `stage`, in deterministic (name) order.

        `stage=None` is the single, non-staged pass, and it is a defined input
        rather than an accident: a reducer always registers against a numbered
        stage, so a non-staged pass has no reducers and this returns an empty
        list. Callers on that path (``replay._run_stage_reducers``) rely on it.
        """
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
        """The string a series' pattern is tested against — `_routing_subject`
        for the rule, which upcasters share.

        `match_field` is consistent within a (name, pattern) series, so the
        first version decides. A registration that failed after `setdefault()`
        can leave an empty series behind; that falls back to stream-path routing
        rather than an IndexError on `versions[0]`.
        """
        match_field = versions[0].match_field if versions else None
        return _routing_subject(
            match_field,
            event,
            event_match,
            registrant=f"Handler {name!r}",
            remedy="resolve() was called without an event.",
        )

    def all_versions(self) -> list[HandlerVersion]:
        """Return every registered version (test/debug helper)."""
        out: list[HandlerVersion] = []
        for series in self._versions.values():
            out.extend(series)
        return out

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
class UpcasterVersion(_MetaStreamRecord):
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

    registered_in: str | None = None
    """The module that made this registration — see `HandlerVersion`."""

    # -- meta-stream record (see `rakaia.registration_log`) -----------------

    _PAYLOAD_FIELDS: ClassVar[tuple[_PayloadField, ...]] = (
        _PayloadField("event_match"),
        _PayloadField("from_version", decode=_as_int),
        _PayloadField("dotted_path"),
        _PayloadField("source_hash"),
        _PayloadField("match_field", default=lambda _p: None),
        _REGISTERED_IN,
    )


class UpcasterRegistry(_LogBackedRegistry):
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
        self._log = RegistrationLog(store, stream_path, UpcasterVersion)
        self._logs = (self._log,)
        self._load_logs()

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
            dotted_path=_dotted_path(fn, persisted=self._store is not None),
            source_hash=hash_function_source(fn),
            match_field=match_field,
            registered_in=_registration_site(),
        )

        key = (event_match, from_version, match_field)
        existing = self._upcasters.get(key)
        if existing is not None:
            if existing.identity == new.identity:
                return existing
            raise UpcasterConflictError(
                f"Upcaster already registered for event_match={event_match!r}, "
                f"from_version={from_version}, match_field={match_field!r} "
                f"(existing dotted_path={existing.dotted_path!r}, "
                f"new dotted_path={new.dotted_path!r})."
            )

        self._log.record(new)
        self._upcasters[key] = new
        return new

    @staticmethod
    def _subject(
        up: UpcasterVersion, event: dict[str, Any] | None, event_match_str: str
    ) -> str:
        """The string an upcaster's pattern is matched against.

        The same content-routing rule handler dispatch uses — one function,
        `_routing_subject`, rather than a second copy that a comment promised
        was kept in step. ADR 0002 item 3 made content routing available to
        upcasters; this is where that lands, not a walk-back of it.
        """
        return _routing_subject(
            up.match_field,
            event,
            event_match_str,
            registrant=f"Upcaster (event_match={up.event_match!r})",
            remedy=(
                "was matched without an event; pass the decoded event to "
                "current_version()/apply_chain() (or use upcast_to_current)."
            ),
        )

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
        drift: DriftLedger | None = None,
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

        Pass a `DriftLedger` as `drift` to have each step checked against the
        source hash it was registered with; the ledger holds the policy (warn or
        raise), reports each drifted upcaster once however many events pass
        through it, and memoises the hashing. Omit it and no step is checked.

        This used to be two options — a callback and a hasher — that had to
        agree: the callback without the hasher re-read the upcaster's source for
        every event (a measurable share of replay time, #156), and the hasher
        without the callback skipped the check in silence. One object cannot be
        half-passed (#187).
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
            if drift is not None:
                drift.check(
                    kind="upcaster",
                    name=step.dotted_path,
                    stored_hash=step.source_hash,
                    fn=step.fn,
                )
            event = step.fn(event)
            current += 1
            event = {**event, "schema_version": current}
        return event

    def upcast_to_current(
        self,
        event: dict[str, Any],
        event_match_str: str,
        *,
        drift: DriftLedger | None = None,
    ) -> dict[str, Any]:
        """Upcast `event` all the way to the current schema for its match.

        Convenience over `current_version` + `apply_chain`: the normalise-on-read
        step every consumer outside `replay()` needs. Returns the event unchanged
        when no upcasters apply or it is already current.

        `drift` is forwarded to `apply_chain`; without one, no step is checked
        against its registered source hash.
        """
        target = self.current_version(event_match_str, event)
        return self.apply_chain(event, event_match_str, target, drift=drift)

    def all_upcasters(self) -> list[UpcasterVersion]:
        return list(self._upcasters.values())

    def reset(self) -> None:
        """Clear all in-memory upcaster registrations and the dedup cache.

        In-memory state only — mirrors `HandlerRegistry.reset()`: the seam for
        per-test isolation, not a delete of the durable ``__rakaia__:upcasters``
        meta-stream (delete that via the store if you need to).
        """
        self._upcasters.clear()
        self._reset_logs()


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
    drift: DriftLedger | None = None,
) -> dict[str, Any]:
    """Normalise `event` to the current schema for `event_match`.

    The discoverable one-liner over the upcaster chain::

        from rakaia import upcast
        normalised = upcast(raw_event, "submissions")

    Uses the process-wide default registry unless `registry` is given.

    Normalise-on-read is **not** drift-checked by default: a one-off read has
    nowhere to report to, and warning on every read of an edited upcaster would
    be noise. Pass a `DriftLedger` to opt in — then this path answers the same
    question a replay does, and the answers land in `ledger.warnings` /
    `ledger.drifted` (or raise, with `on_drift="raise"`).
    """
    target = registry if registry is not None else _default_upcaster_registry
    return target.upcast_to_current(event, event_match, drift=drift)


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
            return Upsert(
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
            return Upsert(...)
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
