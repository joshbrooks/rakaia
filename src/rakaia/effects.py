"""
Effects: pure data descriptions of side-effects produced by handlers.

Versioned handlers are pure functions that return Effect descriptions instead
of performing I/O directly. A separate Executor applies the effects. This
makes replay idempotent (re-applying update_or_create converges to the same
state) and keeps handlers trivially testable without a database.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from dataclasses import field as dc_field
from typing import Any, Literal, Protocol

EffectOp = Literal["update_or_create", "delete", "external", "retire"]


# =============================================================================
# Effect
# =============================================================================


@dataclass(frozen=True)
class Effect:
    """A pure data description of one side-effect."""

    op: EffectOp
    """The kind of effect. 'update_or_create', 'delete' and 'retire' are
    replay-safe; 'external' (email, third-party call) is skipped by default on
    replay."""

    # Fields for op="update_or_create", op="delete" and op="retire"
    model_label: str | None = None
    """'app_label.ModelName', e.g. 'myapp.Room'."""

    lookup: dict[str, Any] | None = None
    """Lookup kwargs. For 'update_or_create', passed as **kwargs; for 'delete'
    and 'retire', the `filter()` scope. An empty dict on a delete scopes the
    whole model."""

    defaults: dict[str, Any] | None = None
    """Default field values passed as `defaults=` to update_or_create."""

    # Field for op="delete"
    exclude: dict[str, Any] | None = None
    """Rows to spare from a delete via a single flat lookup:
    `filter(**lookup).exclude(**exclude)` — e.g. `{"idx__in": [0, 1]}` (the
    positional reconcile_children case). For composite natural keys use
    `spare_keys` instead. Ignored for other ops."""

    # Fields for op="delete" and op="retire"
    spare_keys: list[dict[str, Any]] | None = None
    """Composite keys to spare from a delete/retire:
    `filter(**lookup).exclude(Q(**k0) | Q(**k1) | ...)`. This is the
    natural-key reconcile primitive — retire every row in scope *except* the
    composite keys still present (e.g. `[{"alert_type": "ff4", "field_key":
    "a"}]`). An empty list spares nothing (retires/deletes the whole scope)."""

    # Field for op="retire"
    patch: dict[str, Any] | None = None
    """Soft-delete SET values for op="retire": rows in scope (minus
    `spare_keys`) are UPDATEd with these instead of DELETEd, e.g.
    `{"resolved_at": <event ts>}`. The value must be the *triggering event's*
    timestamp, never `timezone.now()`, or replay is not deterministic."""

    # Fields for op="external"
    kind: str | None = None
    """Sub-kind of external effect ('email', 'stripe_charge', ...)."""

    payload: dict[str, Any] | None = None
    """Opaque payload for external effects."""

    # Fields for op="retire" — machine-resolution notifications
    transition_kind: str | None = None
    """When set on a retire, the orchestrator emits one ``external`` effect of
    this ``kind`` per row the retire actually flipped (its liveness sentinel went
    NULL->set) — the "one transition per real machine resolution" of a
    ``reconcile_by_key`` soft-delete. Opt-in: a retire without it costs no extra
    query. Only meaningful on op="retire" (a hard delete leaves no resolved row
    to notify about)."""

    transition_key_fields: tuple[str, ...] | None = None
    """The full ordered set of columns identifying each flipped row in the
    transition payload — the retire's scope-equality columns *plus* its natural
    key. The executor uses these verbatim for its deterministic identity SELECT
    (it does not re-derive them from ``lookup``). Set by ``reconcile_by_key``
    from ``scope`` + ``key_fields`` when ``transition_kind`` is requested. Only
    meaningful on op="retire"."""

    def __post_init__(self) -> None:
        # `exclude` and `spare_keys` are ALTERNATIVE row-sparing mechanisms.
        # Setting both silently ANDs them in the executor
        # (`.exclude(**exclude).exclude(Q(...))`), which is never intended —
        # reject it at construction so the mistake surfaces in the handler.
        if self.exclude is not None and self.spare_keys is not None:
            raise ValueError(
                "Effect sets both `exclude` and `spare_keys`; they are alternative "
                "row-sparing mechanisms — use one. `exclude` is the flat "
                "reconcile_children (positional) case; `spare_keys` is the composite "
                "natural-key case."
            )
        # `exclude` only applies to op="delete"; the retire path ignores it, so an
        # `exclude` on a retire would be silently dropped. Fail loudly instead.
        if self.exclude is not None and self.op != "delete":
            raise ValueError(
                f"Effect sets `exclude` with op={self.op!r}; `exclude` only applies "
                "to op='delete'. Use `spare_keys` to spare rows from a retire."
            )
        # `transition_kind`/`transition_key_fields` drive per-flip notifications,
        # which only a retire produces (it flips rows silently). On any other op
        # they would be dropped — fail loudly.
        if (
            self.transition_kind is not None or self.transition_key_fields is not None
        ) and self.op != "retire":
            raise ValueError(
                f"Effect sets `transition_kind`/`transition_key_fields` with "
                f"op={self.op!r}; it only applies to op='retire'."
            )
        # The two are a pair: `transition_key_fields` names the columns that
        # identify each flipped row, and the executor's deterministic
        # `ORDER BY`/identity SELECT relies on them being present whenever
        # `transition_kind` asks for notifications. Enforce both-or-neither (and
        # non-empty key fields) so a half-set retire can't silently degrade to a
        # non-deterministic, identity-less transition.
        if (self.transition_kind is None) != (self.transition_key_fields is None):
            raise ValueError(
                "Effect sets one of `transition_kind`/`transition_key_fields` "
                "without the other; they must be set together."
            )
        if self.transition_key_fields is not None and not self.transition_key_fields:
            raise ValueError(
                "Effect sets an empty `transition_key_fields`; it must name at "
                "least one column identifying each flipped row."
            )


# =============================================================================
# Errors
# =============================================================================


class EffectCollisionError(Exception):
    """
    Two sibling effects target the same (model_label, lookup) row with
    overlapping keys in `defaults`, violating the disjoint-defaults invariant.
    """


# =============================================================================
# Executor protocol
# =============================================================================


@dataclass(frozen=True)
class ApplyReport:
    """What ``Executor.apply`` observed while applying a batch.

    Currently carries ``retire_flips``: one ``(retire_effect, flipped_rows)``
    entry per retire that opted into notifications (``transition_kind`` set),
    where ``flipped_rows`` is the list of identity dicts for the rows whose
    liveness sentinel actually went NULL->set. The orchestrator turns these into
    one ``external`` transition each. Executors that don't observe flips (or
    predate this) may return ``None``; the orchestrator treats that as empty."""

    retire_flips: list[tuple[Effect, list[dict[str, Any]]]] = dc_field(
        default_factory=list
    )


class Executor(Protocol):
    """
    Applies a batch of effects to durable storage.

    Concrete implementations live outside rakaia core (e.g. DjangoExecutor
    in django_rakaia). The replay orchestrator filters external effects
    before calling apply().
    """

    def apply(self, effects: Iterable[Effect]) -> ApplyReport | None: ...


# =============================================================================
# Collision detection
# =============================================================================


def _row_key(effect: Effect) -> tuple[str, str]:
    if effect.model_label is None or effect.lookup is None:
        raise ValueError(
            f"Effect with op={effect.op!r} requires model_label and lookup"
        )
    canon_lookup = json.dumps(effect.lookup, sort_keys=True, default=str)
    return (effect.model_label, canon_lookup)


def check_disjoint_defaults(effects: Iterable[Effect]) -> None:
    """
    Raise EffectCollisionError if two update_or_create effects targeting the
    same (model_label, lookup) row share any key in their `defaults`.

    External effects are ignored. Effects without `defaults` are ignored.
    """
    # field -> first-seen effect index, keyed by row
    seen: dict[tuple[str, str], dict[str, int]] = {}
    for idx, eff in enumerate(effects):
        if eff.op != "update_or_create" or not eff.defaults:
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


# =============================================================================
# External effect dispatch
# =============================================================================


def dispatch_external(
    effects: Iterable[Effect],
    handlers: Mapping[str, Callable[[Effect], Any]],
    *,
    on_unknown: Literal["raise", "ignore"] = "raise",
) -> int:
    """Route ``op="external"`` effects to per-``kind`` handlers.

    rakaia never applies external effects itself: `replay()` filters them out
    (unless ``include_external=True``) and executors drop them. This is the seam
    for the *application* to act on them — email, webhooks, an alert transition —
    without every consumer re-implementing ``for e in effects: if e.op ==
    "external" and e.kind == ...``.

    Pass a ``{kind: fn}`` map; each external effect is routed to
    ``handlers[effect.kind]`` (non-external effects are skipped). Returns the
    number dispatched. An effect whose ``kind`` has no handler raises ``KeyError``
    by default, or is skipped with ``on_unknown="ignore"``.
    """
    dispatched = 0
    for eff in effects:
        if eff.op != "external":
            continue
        handler = handlers.get(eff.kind) if eff.kind is not None else None
        if handler is None:
            if on_unknown == "ignore":
                continue
            raise KeyError(f"No handler for external effect kind={eff.kind!r}")
        handler(eff)
        dispatched += 1
    return dispatched
