"""
Effects: pure data descriptions of side-effects produced by handlers.

Versioned handlers are pure functions that return Effect descriptions instead
of performing I/O directly. A separate Executor applies the effects. This
makes replay idempotent (re-applying an :class:`Upsert` converges to the same
state) and keeps handlers trivially testable without a database.

There are four database effects — :class:`Upsert`, :class:`Update`,
:class:`Delete` and :class:`Retire` — which share a row identity
(:class:`RowEffect`: a ``model_label`` and a ``lookup``) and otherwise carry
only the fields their own operation uses. ``Effect`` is the union of the four.
An :class:`ExternalEffect` (an email, a third-party call) is deliberately *not*
one of them: it names no row, no executor applies it, and replay returns it to
the caller instead.

Each operation is its own type, so a ``produces`` on a delete or a ``patch`` on
an upsert is a type error rather than a runtime ``ValueError``. The two
alternative row-sparing mechanisms are one ``spare`` field with two shapes
(:class:`Exclude` or :class:`SpareKeys`), and the transition pair is one
:class:`Transition` object.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from dataclasses import field as dc_field
from dataclasses import replace as dc_replace
from typing import Any, Protocol, TypeVar, assert_never, cast, runtime_checkable

from .errors import RakaiaError

# =============================================================================
# Symbolic refs — bind to a sibling effect's generated key
# =============================================================================


@dataclass(frozen=True)
class Ref:
    """A batch-local reference to a row a *sibling* effect materialises.

    Placed as a **direct value** inside another effect's ``lookup``/``defaults``/
    ``patch`` (or a delete's ``spare``), it stands in for a column of the row
    produced by the effect that declared the matching ``produces=`` id earlier
    in the same ``apply()`` batch. The executor substitutes the real value at
    apply time.

    Must be a **direct** value: a ``Ref`` nested inside a list or dict (e.g.
    ``defaults={"tags": [Ref("t")]}``) is not resolved, nor is one in an
    :class:`ExternalEffect` payload, since external effects never reach an
    executor.

    ``field`` defaults to ``"pk"`` (the produced row's primary key — the FK case),
    or names any other column, e.g. ``Ref("proj", "suku")``.

    ``Ref("proj")`` resolves to a **DB-assigned pk**, which is not stable across
    a full rebuild; use a natural-key ``field`` if you need a stable value.
    """

    produces: str
    """The correlation id a sibling effect declared via ``produces=``."""

    field: str = "pk"
    """Which column of the produced row to substitute. Default: the primary key."""


# =============================================================================
# Row-sparing shapes — the two alternatives, as two types
# =============================================================================


@dataclass(frozen=True)
class Exclude:
    """Spare rows from a delete via a single flat lookup.

    ``filter(**lookup).exclude(**spare.lookup)`` — e.g.
    ``Exclude({"idx__in": [0, 1]})``, the positional `reconcile_children` case.
    For composite natural keys use :class:`SpareKeys` instead.
    """

    lookup: dict[str, Any]


@dataclass(frozen=True)
class SpareKeys:
    """Spare rows from a delete or retire by composite natural key.

    ``filter(**lookup).exclude(Q(**k0) | Q(**k1) | ...)``: the natural-key
    reconcile primitive, e.g. ``SpareKeys([{"alert_type": "ff4", "field_key":
    "a"}])``. An empty list spares nothing (the whole scope is affected).
    """

    keys: list[dict[str, Any]]


# =============================================================================
# Transition — the machine-resolution notification request
# =============================================================================


@dataclass(frozen=True)
class Transition:
    """A retire's opt-in request for per-flip notifications.

    When a :class:`Retire` carries one, the replay orchestrator emits one
    :class:`ExternalEffect` of ``kind`` per row the retire actually flipped (its
    liveness sentinel went NULL->set). A retire without one costs no extra query.

    ``key_fields`` names the columns identifying each flipped row; the executor's
    ``ORDER BY`` and identity ``SELECT`` rely on it being non-empty. It is the
    retire's scope-equality columns plus its natural key, used verbatim (the
    executor does not re-derive it from ``lookup``). ``reconcile_by_key`` sets it
    from ``scope`` + ``key_fields``.
    """

    kind: str
    """Sub-kind of the emitted external effect ('alert_resolved', ...)."""

    key_fields: tuple[str, ...]
    """The full ordered set of columns identifying each flipped row."""

    def __post_init__(self) -> None:
        if not self.key_fields:
            raise ValueError(
                "Transition has empty key_fields; it must name at least one "
                "column identifying each flipped row."
            )


#: The ``state`` every transition payload carries: a transition is emitted only
#: for a row that actually flipped, so there is no other value.
TRANSITION_RESOLVED = "resolved"


def transition_payload(
    patch: Mapping[str, Any] | None, identity: Mapping[str, Any]
) -> dict[str, Any]:
    """Build the payload a :class:`Transition` notification carries.

    ``patch`` is spread first so the orchestrator's own ``key`` and ``state``
    always win: a patch column named ``key`` or ``state`` must not clobber the row
    identity or the transition state.
    """
    return {
        **(patch or {}),
        "key": dict(identity),
        "state": TRANSITION_RESOLVED,
    }


# =============================================================================
# The database effects
# =============================================================================


@dataclass(frozen=True)
class RowEffect:
    """The row identity every database effect shares.

    Not constructed directly — use :class:`Upsert`, :class:`Update`,
    :class:`Delete` or :class:`Retire`.
    """

    model_label: str
    """'app_label.ModelName', e.g. 'myapp.Room'."""

    lookup: dict[str, Any]
    """Lookup kwargs. For an :class:`Upsert`, passed as ``**kwargs``; for the
    others, the ``filter()`` scope. An empty dict on a delete scopes the whole
    model."""


@dataclass(frozen=True)
class Upsert(RowEffect):
    """Create the row matching ``lookup``, or update it in place — the
    replay-safe write. Re-applying converges to the same row."""

    defaults: dict[str, Any] | None = None
    """Field values passed as ``defaults=`` to ``update_or_create``."""

    produces: str | None = None
    """A batch-local correlation id naming the single row this upsert
    materialises, so a sibling effect can bind to it via ``Ref(produces)``
    without a staging split or a natural-key reader lookup. Only an upsert has
    it: an :class:`Update` can match many rows and a delete/retire produces
    none.

    Never stored, and unrelated to an event's ``correlation_id``
    (`AppendOptions.correlation_id`), which names a business correlation such as
    an incident and is kept with the event."""


@dataclass(frozen=True)
class Update(RowEffect):
    """Update-if-exists: the rows matching ``lookup`` are updated in place and
    **never** inserted (a no-op when nothing matches or ``defaults`` is empty).

    This is the primitive a secondary owner of a multi-owned projection row uses
    instead of a hand-rolled exists-guard."""

    defaults: dict[str, Any] | None = None
    """The ``update()`` SET values."""


@dataclass(frozen=True)
class Delete(RowEffect):
    """Hard-delete the rows matching ``lookup``, minus any ``spare``."""

    spare: Exclude | SpareKeys | None = None
    """Rows to spare: an :class:`Exclude` (flat lookup, the positional
    `reconcile_children` case) or :class:`SpareKeys` (composite natural keys).
    They are alternatives, so this is one field, not two."""


@dataclass(frozen=True)
class Retire(RowEffect):
    """Soft-delete: the rows matching ``lookup`` (minus ``spare``) are UPDATEd
    with ``patch`` instead of DELETEd, e.g. ``{"resolved_at": <event ts>}``.

    The patch value must be the *triggering event's* timestamp, never
    ``timezone.now()``, or replay is not deterministic."""

    patch: dict[str, Any]
    """The soft-delete SET values. Must be non-empty."""

    spare: SpareKeys | None = None
    """Composite natural keys to spare. (A retire has no flat-exclude form —
    that is the positional delete case.)"""

    transition: Transition | None = None
    """Opt in to one external notification per row this retire actually flips.
    See :class:`Transition`."""


#: The four database effects. An executor applies exactly these.
Effect = Upsert | Update | Delete | Retire


# =============================================================================
# External effects — application-level, never applied by an executor
# =============================================================================


@dataclass(frozen=True)
class ExternalEffect:
    """An application-level side effect: an email, a webhook, a payment.

    Not an :class:`Effect`. It names no row and no executor applies it:
    ``replay()`` collects them into ``ReplayResult.external`` and hands them back
    to the caller, who decides whether to deliver them, so a rebuild never
    re-sends one by accident.
    """

    kind: str
    """Sub-kind of external effect ('email', 'stripe_charge', ...)."""

    payload: dict[str, Any]
    """Opaque payload."""


#: Anything a handler or reducer may return.
AnyEffect = Upsert | Update | Delete | Retire | ExternalEffect


# =============================================================================
# Errors
# =============================================================================


class EffectCollisionError(RakaiaError):
    """
    Two sibling effects target the same (model_label, lookup) row with
    overlapping keys in `defaults`, violating the disjoint-defaults invariant.
    """

    code = "effect_collision"


class UnresolvedRefError(RakaiaError):
    """A `Ref` names a `produces` id no earlier effect in the batch produced
    (a forward reference or a typo)."""

    code = "unresolved_ref"


class DuplicateProducesError(RakaiaError):
    """Two effects in one batch declare the same `produces` id. The id would
    silently bind to the second producer's row, orphaning the first — always a
    bug, so it is rejected rather than resolved to the wrong row."""

    code = "duplicate_produces"


# =============================================================================
# Ref resolution (used by applying executors — DjangoExecutor, OverlayExecutor)
# =============================================================================


#: An effect, kept as its own concrete type through `RefResolver.resolve_effect`
#: so a caller that hands in an `Update` gets an `Update` back.
_EffectT = TypeVar("_EffectT", bound="Effect")

#: The two shapes of `spare`, likewise kept concrete: a `Delete` may spare by
#: flat lookup or composite key, a `Retire` only by composite key.
_SpareT = TypeVar("_SpareT", bound="Exclude | SpareKeys | None")


class RefResolver:
    """Resolves `Ref` placeholders against rows produced earlier in one batch.

    `rakaia.applying.apply_effects` keeps one resolver per ``apply()`` call. As
    it applies each write effect that declares ``produces=X``, it calls
    ``record(X, accessor)`` with a callable mapping a field name to the produced
    row's value. Before applying any effect it calls ``resolve_effect(effect)`` to
    substitute every ``Ref`` value; a ``Ref`` to an id not yet produced raises
    :class:`UnresolvedRefError`, never a silent ``None``. The `CollectingExecutor`
    resolves too, so an unknown ``Ref`` fails a dry run as it fails a write, but it
    records the effects as emitted, with their literal ``Ref`` values.
    """

    def __init__(self) -> None:
        self._symbols: dict[str, Callable[[str], Any]] = {}

    def record(self, produces_id: str, accessor: Callable[[str], Any]) -> None:
        """Register the row produced under `produces_id`; `accessor(field)`
        returns that row's value for a column (``"pk"`` for the primary key).

        Raises :class:`DuplicateProducesError` if `produces_id` was already
        declared in this batch, since a re-declaration would silently rebind every
        `Ref` to the second producer's row.
        """
        if produces_id in self._symbols:
            raise DuplicateProducesError(
                f"Two effects in this batch declare produces={produces_id!r}; "
                f"a Ref to it would bind to the wrong row. Give each producer a "
                f"distinct id."
            )
        self._symbols[produces_id] = accessor

    def resolve_value(self, value: Any) -> Any:
        """A `Ref` -> the produced row's value; any other value unchanged."""
        if not isinstance(value, Ref):
            return value
        accessor = self._symbols.get(value.produces)
        if accessor is None:
            raise UnresolvedRefError(
                f"Effect references produces={value.produces!r} but no earlier "
                f"effect in this batch produced it (forward reference or typo)."
            )
        try:
            return accessor(value.field)
        except AttributeError as exc:
            raise UnresolvedRefError(
                f"Ref(produces={value.produces!r}, field={value.field!r}): the "
                f"produced row has no attribute {value.field!r}."
            ) from exc

    def _resolve_lookup(self, mapping: dict[str, Any]) -> dict[str, Any]:
        """A mapping with its `Ref` values substituted, or the mapping itself
        (same object) when it carries none — so a caller can test by identity."""
        if not mapping or not any(isinstance(v, Ref) for v in mapping.values()):
            return mapping
        return {k: self.resolve_value(v) for k, v in mapping.items()}

    def _resolve_mapping(self, mapping: dict[str, Any] | None) -> dict[str, Any] | None:
        """`_resolve_lookup` for a field that may be absent (an `Upsert`'s or
        `Update`'s ``defaults``)."""
        return mapping if mapping is None else self._resolve_lookup(mapping)

    def _resolve_spare(self, spare: _SpareT) -> _SpareT:
        """Substitute Refs inside a `spare`, returning it unchanged when there
        is nothing to substitute (so the caller can compare by identity).

        Generic in the spare's own type because a `Delete` may spare by flat
        lookup or composite key and a `Retire` only by composite key; returning
        the declared union would make `dc_replace(retire, spare=...)` a type error.
        """
        if isinstance(spare, Exclude):
            resolved = self._resolve_lookup(spare.lookup)
            if resolved is spare.lookup:
                return spare
            return cast("_SpareT", Exclude(lookup=resolved))
        if isinstance(spare, SpareKeys):
            keys = [self._resolve_lookup(k) for k in spare.keys]
            if all(n is o for n, o in zip(keys, spare.keys, strict=True)):
                return spare
            return cast("_SpareT", SpareKeys(keys=keys))
        return spare

    def resolve_effect(self, effect: _EffectT) -> _EffectT:
        """Return `effect` with every `Ref` value substituted, or `effect`
        itself (unchanged) when it carries no refs — the common fast path.

        Each variant names its own ref-bearing fields as attribute reads rather
        than a tuple of strings and `hasattr`: a renamed field made `hasattr`
        return False, so the `Ref` was never substituted and the placeholder
        object itself was written to the database (#161). Renaming `Retire.patch`
        now fails `just typecheck` here, and `assert_never` does the same for a
        new effect variant.
        """
        if isinstance(effect, (Upsert, Update)):
            lookup = self._resolve_lookup(effect.lookup)
            defaults = self._resolve_mapping(effect.defaults)
            if lookup is effect.lookup and defaults is effect.defaults:
                return effect
            return cast(
                "_EffectT", dc_replace(effect, lookup=lookup, defaults=defaults)
            )
        if isinstance(effect, Delete):
            lookup = self._resolve_lookup(effect.lookup)
            spare = self._resolve_spare(effect.spare)
            if lookup is effect.lookup and spare is effect.spare:
                return effect
            return cast("_EffectT", dc_replace(effect, lookup=lookup, spare=spare))
        if isinstance(effect, Retire):
            lookup = self._resolve_lookup(effect.lookup)
            patch = self._resolve_lookup(effect.patch)
            spare = self._resolve_spare(effect.spare)
            if (
                lookup is effect.lookup
                and patch is effect.patch
                and spare is effect.spare
            ):
                return effect
            return cast(
                "_EffectT",
                dc_replace(effect, lookup=lookup, patch=patch, spare=spare),
            )
        assert_never(effect)


# =============================================================================
# Executor protocol
# =============================================================================


@dataclass(frozen=True)
class ApplyReport:
    """What ``Executor.apply`` observed while applying a batch.

    ``upserts_created``, ``upserts_written`` and ``upserts_skipped`` count how the
    batch's :class:`Upsert` effects resolved: ``created`` inserted a row,
    ``written`` issued any write (so ``created`` is a subset of it), and
    ``skipped`` matched a row whose stored values already equalled the effect's,
    so no write was issued. ``written + skipped`` is the number of upserts, and an
    executor that always writes reports ``skipped`` as 0.

    ``retire_flips`` holds one ``(retire_effect, flipped_rows)`` entry per retire
    that asked to be notified (it carries a :class:`Transition`), where
    ``flipped_rows`` are the identity dicts of the rows that actually went from
    live to retired. An executor that does not observe flips may return ``None``
    from ``apply`` instead of a report; the caller reads that as no flips.
    """

    retire_flips: list[tuple[Retire, list[dict[str, Any]]]] = dc_field(
        default_factory=list
    )
    upserts_created: int = 0
    upserts_written: int = 0
    upserts_skipped: int = 0


@runtime_checkable
class Executor(Protocol):
    """
    Applies a batch of effects to durable storage.

    Concrete implementations live outside rakaia core (e.g. DjangoExecutor
    in django_rakaia). Only database effects reach ``apply()``.

    ``runtime_checkable`` so ``isinstance(x, Executor)`` works. That checks method
    *presence* only, which is why the shared conformance suite
    (``rakaia/testing/executor_contract.py``) goes on to pin behaviour.
    """

    def apply(self, effects: Iterable[Effect]) -> ApplyReport | None: ...


# =============================================================================
# Collision detection
# =============================================================================


def _row_key(effect: RowEffect) -> tuple[str, str]:
    canon_lookup = json.dumps(effect.lookup, sort_keys=True, default=str)
    return (effect.model_label, canon_lookup)


class _WrittenFields:
    """Which columns of which rows a batch of effects already writes.

    `check_disjoint_defaults` walks it once over a whole batch; the replay
    orchestrator asks the same question incrementally ("would adding this event's
    effects collide?") so the rule has one home (#152).

    `collides` only reports and `record` only remembers; neither raises, which is
    what lets the orchestrator flush early instead.
    """

    def __init__(self) -> None:
        # (model_label, canonical lookup) -> {column: index of first writer}
        self._seen: dict[tuple[str, str], dict[str, int]] = {}

    @staticmethod
    def _writes(effect: Effect) -> tuple[tuple[str, str], dict[str, Any]] | None:
        """The row and columns `effect` writes, or None if it writes none.

        Only :class:`Upsert` and :class:`Update` carry `defaults`.
        """
        if not isinstance(effect, (Upsert, Update)) or not effect.defaults:
            return None
        return _row_key(effect), effect.defaults

    def collides(self, effect: Effect) -> tuple[str, int] | None:
        """The first ``(column, earlier effect index)`` `effect` would overwrite,
        or None if it is disjoint from everything recorded so far."""
        writes = self._writes(effect)
        if writes is None:
            return None
        row, defaults = writes
        existing = self._seen.get(row)
        if existing is None:
            return None
        for field_name in defaults:
            if field_name in existing:
                return (field_name, existing[field_name])
        return None

    def record(self, effect: Effect, idx: int) -> None:
        """Remember that effect number `idx` writes these columns of this row."""
        writes = self._writes(effect)
        if writes is None:
            return
        row, defaults = writes
        existing = self._seen.setdefault(row, {})
        for field_name in defaults:
            existing.setdefault(field_name, idx)


def check_disjoint_defaults(effects: Iterable[Effect]) -> None:
    """
    Raise EffectCollisionError if two write effects targeting the same
    (model_label, lookup) row share any key in their `defaults`.

    Covers :class:`Upsert` and :class:`Update`, including a mix of the two: the
    multi-owner invariant is that each owner writes a disjoint set of columns.
    Delete and retire effects are ignored, as are effects without `defaults`.
    """
    seen = _WrittenFields()
    for idx, eff in enumerate(effects):
        hit = seen.collides(eff)
        if hit is not None:
            field_name, first = hit
            raise EffectCollisionError(
                f"Effects #{first} and #{idx} both write field "
                f"{field_name!r} on {eff.model_label} {eff.lookup!r}"
            )
        seen.record(eff, idx)


# =============================================================================
# Write order
# =============================================================================


def _write_order_rank(effect: Effect) -> int:
    """Which pass of an applied batch `effect` belongs to.

    Every executor applies a batch in three passes (every write, then every
    delete, then every retire) so a reconcile batch converges regardless of
    emission order and a ``produces=`` row is recorded before any :class:`Ref`
    to it. That is why effects from two events cannot simply be poured into one
    batch: it would hoist a later event's write above an earlier event's delete.

    Consulted by the apply loop both executors share (`rakaia.applying`) and by
    the replay buffer that decides where a batch boundary falls (#152, #256, #325).
    """
    if isinstance(effect, (Upsert, Update)):
        return 0
    if isinstance(effect, Delete):
        return 1
    return 2


_WRITE_ORDER_PASSES = (0, 1, 2)
"""The passes `_write_order_rank` sorts a batch into, in application order.

`rakaia.applying.apply_effects` loops over this and dispatches on the rank, so
a fourth pass would be added here and nowhere else.
"""
