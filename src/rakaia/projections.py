"""
Projection helpers: turn a parent record with a repeated child collection into
a set of idempotent, orphan-free Effects.

The canonical shape is "one source record fans out into N child rows" — a
FormKit repeater, an order's line items, a form's answers. Replaying such a
projection with plain `update_or_create` leaks orphans: if a later version of
the parent has *fewer* children, the dropped child rows survive forever because
nothing deletes them.

`reconcile_children` closes that gap. It emits one `update_or_create` per
current child (keyed by parent + child index) plus a single reconcile `delete`
that removes every child under the parent *except* the indices still present.
Re-running it converges, and shrinking the collection prunes the tail.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any

from .effects import Effect


def reconcile_children(
    model_label: str,
    parent_lookup: dict[str, Any],
    child_key: str,
    items: Sequence[Any],
    defaults_fn: Callable[[Any], dict[str, Any]],
) -> list[Effect]:
    """Return Effects that materialise `items` as child rows without orphans.

    Args:
        model_label: 'app_label.ModelName' of the child row model.
        parent_lookup: lookup identifying the parent scope, e.g.
            ``{"submission_id": sid}``. Each child row is keyed by this plus
            ``{child_key: index}``; the reconcile delete is scoped to it.
        child_key: the field holding the child's positional index within the
            parent (its ordinality), e.g. ``"activity_index"``.
        items: the current child collection, in order. Position becomes the
            child's index.
        defaults_fn: maps one item to its ``defaults=`` field values.

    Returns:
        One ``update_or_create`` Effect per item, followed by one ``delete``
        Effect that removes stale children (those whose index is no longer in
        the collection). An empty ``items`` yields just the delete, which
        removes every child under ``parent_lookup``.
    """
    effects: list[Effect] = [
        Effect(
            op="update_or_create",
            model_label=model_label,
            lookup={**parent_lookup, child_key: index},
            defaults=defaults_fn(item),
        )
        for index, item in enumerate(items)
    ]
    effects.append(
        Effect(
            op="delete",
            model_label=model_label,
            lookup=dict(parent_lookup),
            exclude={f"{child_key}__in": list(range(len(items)))},
        )
    )
    return effects
