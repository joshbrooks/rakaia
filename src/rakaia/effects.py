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

Modelling each operation as its own type is what removes cross-field validation
from this module: a ``produces`` on a delete or a ``patch`` on an upsert is a
type error, not a runtime ``ValueError``. The two constraints that live *within*
one operation are modelled away too — the two alternative row-sparing
mechanisms become one ``spare`` field with two shapes (:class:`Exclude` or
:class:`SpareKeys`), and the transition pair becomes one :class:`Transition`
object. Only ``Transition.key_fields`` being non-empty survives as a check, and
it lives inside ``Transition``.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable
from dataclasses import dataclass
from dataclasses import field as dc_field
from dataclasses import replace as dc_replace
from typing import Any, Protocol, runtime_checkable

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

    Must be a **direct** value — a ``Ref`` nested inside a list or dict (e.g.
    ``defaults={"tags": [Ref("t")]}``) is not resolved. Not resolved in
    :class:`ExternalEffect` payloads either: external effects never reach an
    executor, so a ``Ref`` there would never be substituted.

    ``field`` defaults to ``"pk"`` (the produced row's primary key — the FK case),
    or names any other column, e.g. ``Ref("proj", "suku")``.

    Determinism note: ``Ref("proj")`` resolves to a **DB-assigned pk**, which is
    not stable across a full rebuild (correct for an FK column, whose integrity
    holds; use a natural-key ``field`` if you need a value stable across rebuilds).
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
    For composite natural keys use :class:`SpareKeys` instead. The two are
    alternatives, which is why a delete carries one ``spare`` field rather than
    two fields and a rule forbidding both.
    """

    lookup: dict[str, Any]


@dataclass(frozen=True)
class SpareKeys:
    """Spare rows from a delete or retire by composite natural key.

    ``filter(**lookup).exclude(Q(**k0) | Q(**k1) | ...)``. This is the
    natural-key reconcile primitive — retire every row in scope *except* the
    composite keys still present (e.g.
    ``SpareKeys([{"alert_type": "ff4", "field_key": "a"}])``). An empty list
    spares nothing (retires/deletes the whole scope).
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
    liveness sentinel went NULL->set) — the "one transition per real machine
    resolution" of a ``reconcile_by_key`` soft-delete. Opt-in: a retire without
    it costs no extra query.

    The two values are a pair, which is why they are one object: ``key_fields``
    names the columns identifying each flipped row, and the executor's
    deterministic ``ORDER BY``/identity ``SELECT`` relies on them being present
    whenever a transition is requested. It is the retire's scope-equality columns
    *plus* its natural key, used verbatim (the executor does not re-derive them
    from ``lookup``). ``reconcile_by_key`` sets it from ``scope`` + ``key_fields``.
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
    none."""


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

    Not an :class:`Effect`. It names no row, shares none of the row fields, and
    no executor applies it — ``replay()`` collects them into
    ``ReplayResult.external`` and hands them back to the caller, who decides
    whether to deliver them. A rebuild therefore never re-spams by accident, and
    an executor never needs an external branch.
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


class EffectCollisionError(Exception):
    """
    Two sibling effects target the same (model_label, lookup) row with
    overlapping keys in `defaults`, violating the disjoint-defaults invariant.
    """


class UnresolvedRefError(Exception):
    """A `Ref` names a `produces` id no earlier effect in the batch produced
    (a forward reference or a typo)."""


class DuplicateProducesError(Exception):
    """Two effects in one batch declare the same `produces` id. The id would
    silently bind to the second producer's row, orphaning the first — always a
    bug, so it is rejected rather than resolved to the wrong row."""


# =============================================================================
# Ref resolution (used by applying executors — DjangoExecutor, OverlayExecutor)
# =============================================================================


class RefResolver:
    """Resolves `Ref` placeholders against rows produced earlier in one batch.

    An applying executor keeps one resolver per ``apply()`` call. As it applies
    each write effect that declares ``produces=X``, it calls ``record(X,
    accessor)`` with a callable mapping a field name to the produced row's value
    (e.g. ``lambda f: obj.pk if f in ("pk", "id") else getattr(obj, f)``). Before
    applying any effect it calls ``resolve_effect(effect)`` to substitute every
    ``Ref`` value; a ``Ref`` to an id not yet produced raises
    :class:`UnresolvedRefError` (deterministic — a forward ref is never a silent
    ``None``). The `CollectingExecutor` does *not* resolve — a dry run keeps the
    literal ``Ref`` values, which is correct.
    """

    # Effect fields whose dict values may carry a Ref. Which of them a given
    # variant has is decided by the variant's own type, not by a rule here.
    _MAPPING_FIELDS = ("lookup", "defaults", "patch")

    def __init__(self) -> None:
        self._symbols: dict[str, Callable[[str], Any]] = {}

    def record(self, produces_id: str, accessor: Callable[[str], Any]) -> None:
        """Register the row produced under `produces_id`; `accessor(field)`
        returns that row's value for a column (``"pk"`` for the primary key).

        Raises :class:`DuplicateProducesError` if `produces_id` was already
        declared in this batch — a re-declaration would silently rebind every
        `Ref` to the second producer's row, so it is a loud error, never a
        silent wrong binding.
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

    def _resolve_mapping(self, mapping: dict[str, Any] | None) -> dict[str, Any] | None:
        if not mapping or not any(isinstance(v, Ref) for v in mapping.values()):
            return mapping  # unchanged (same object) — no Ref to substitute
        return {k: self.resolve_value(v) for k, v in mapping.items()}

    def _resolve_spare(self, spare: Any) -> Any:
        """Substitute Refs inside a `spare`, returning it unchanged when there
        is nothing to substitute (so the caller can compare by identity)."""
        if isinstance(spare, Exclude):
            resolved = self._resolve_mapping(spare.lookup)
            return spare if resolved is spare.lookup else Exclude(lookup=resolved or {})
        if isinstance(spare, SpareKeys):
            keys = [self._resolve_mapping(k) for k in spare.keys]
            if all(n is o for n, o in zip(keys, spare.keys, strict=True)):
                return spare
            return SpareKeys(keys=[k or {} for k in keys])
        return spare

    def resolve_effect(self, effect: Any) -> Any:
        """Return `effect` with every `Ref` value substituted, or `effect`
        itself (unchanged) when it carries no refs — the common fast path."""
        changed: dict[str, Any] = {}
        for name in self._MAPPING_FIELDS:
            if not hasattr(effect, name):
                continue
            original = getattr(effect, name)
            resolved = self._resolve_mapping(original)
            if resolved is not original:
                changed[name] = resolved
        spare = getattr(effect, "spare", None)
        if spare is not None:
            resolved_spare = self._resolve_spare(spare)
            if resolved_spare is not spare:
                changed["spare"] = resolved_spare
        if not changed:
            return effect
        return dc_replace(effect, **changed)


# =============================================================================
# Executor protocol
# =============================================================================


@dataclass(frozen=True)
class ApplyReport:
    """What ``Executor.apply`` observed while applying a batch.

    Currently carries ``retire_flips``: one ``(retire_effect, flipped_rows)``
    entry per retire that opted into notifications (a :class:`Transition`),
    where ``flipped_rows`` is the list of identity dicts for the rows whose
    liveness sentinel actually went NULL->set. The orchestrator turns these into
    one :class:`ExternalEffect` each. Executors that don't observe flips (or
    predate this) may return ``None``; the orchestrator treats that as empty."""

    retire_flips: list[tuple[Retire, list[dict[str, Any]]]] = dc_field(
        default_factory=list
    )


@runtime_checkable
class Executor(Protocol):
    """
    Applies a batch of effects to durable storage.

    Concrete implementations live outside rakaia core (e.g. DjangoExecutor
    in django_rakaia). Only database effects reach ``apply()`` — external
    effects are not Effects and never arrive here.

    ``runtime_checkable`` so ``isinstance(x, Executor)`` works — every protocol
    in ``protocols.py`` carries it, and the shared conformance suite
    (``tests/executor_contract.py``) opens with that assertion. It checks method
    *presence* only, which is why the suite goes on to pin behaviour.
    """

    def apply(self, effects: Iterable[Effect]) -> ApplyReport | None: ...


# =============================================================================
# Collision detection
# =============================================================================


def _row_key(effect: RowEffect) -> tuple[str, str]:
    canon_lookup = json.dumps(effect.lookup, sort_keys=True, default=str)
    return (effect.model_label, canon_lookup)


def check_disjoint_defaults(effects: Iterable[Effect]) -> None:
    """
    Raise EffectCollisionError if two write effects targeting the same
    (model_label, lookup) row share any key in their `defaults`.

    Covers both :class:`Upsert` and :class:`Update` (the effects that carry
    `defaults`), including a mix of the two — the multi-owner invariant is that
    each owner writes a *disjoint* set of columns, so two effects writing the
    same column on the same row is always a bug. Delete and retire effects are
    ignored, as are effects without `defaults`.
    """
    # field -> first-seen effect index, keyed by row
    seen: dict[tuple[str, str], dict[str, int]] = {}
    for idx, eff in enumerate(effects):
        if not isinstance(eff, (Upsert, Update)) or not eff.defaults:
            continue
        row = _row_key(eff)
        existing = seen.setdefault(row, {})
        for field in eff.defaults:
            if field in existing:
                raise EffectCollisionError(
                    f"Effects #{existing[field]} and #{idx} both write field "
                    f"{field!r} on {eff.model_label} {eff.lookup!r}"
                )
            existing[field] = idx
