"""
Effects: pure data descriptions of side-effects produced by handlers.

Versioned handlers are pure functions that return Effect descriptions instead
of performing I/O directly. A separate Executor applies the effects. This
makes replay idempotent (re-applying update_or_create converges to the same
state) and keeps handlers trivially testable without a database.
"""

from __future__ import annotations

import json
from collections.abc import Iterable
from dataclasses import dataclass
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


class Executor(Protocol):
    """
    Applies a batch of effects to durable storage.

    Concrete implementations live outside rakaia core (e.g. DjangoExecutor
    in django_rakaia). The replay orchestrator filters external effects
    before calling apply().
    """

    def apply(self, effects: Iterable[Effect]) -> None: ...


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
