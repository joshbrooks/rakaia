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

`reconcile_by_key` generalises the same idea past positional children: the
child is keyed by an arbitrary *composite natural key* (e.g. an alert's
`(alert_type, field_key)`), the reconcile pass can be scoped by a
`retire_filter` distinct from the per-child key, and stale rows can be
*soft-deleted* (an UPDATE stamping `resolved_at`) instead of hard-deleted.
That combination is what a quality-flag / alert projection needs:

* the `retire_filter` keeps the reconcile scoped to *machine-owned* rows, so a
  re-derivation never clobbers authored ones (the "authored-clobber" bug);
* the soft-delete `retire` preserves the audit trail;
* the composite `spare_keys` retire only the natural keys that stopped
  violating.
"""

from __future__ import annotations

from collections.abc import Callable, Sequence
from typing import Any, Literal

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


def reconcile_by_key(
    model_label: str,
    scope: dict[str, Any],
    key_fields: tuple[str, ...],
    items: Sequence[Any],
    key_fn: Callable[[Any], dict[str, Any]],
    defaults_fn: Callable[[Any], dict[str, Any]],
    *,
    retire_filter: dict[str, Any] | None = None,
    retire: Literal["delete"] | dict[str, Any] = "delete",
) -> list[Effect]:
    """Reconcile a set of rows keyed by a *composite natural key*.

    The natural-key generalisation of `reconcile_children` (which is the special
    case `scope=parent, key=(index,), retire="delete"`). Emits one
    `update_or_create` per current item plus a single reconcile pass that
    retires every row in `scope` (further narrowed by `retire_filter`) whose
    composite key is no longer present.

    Args:
        model_label: 'app_label.ModelName' of the row model.
        scope: identity shared by every child lookup *and* the base of the
            retire filter, e.g. ``{"stream_key": sid}``. Not mutated.
        key_fields: the composite natural key's field names, e.g.
            ``("alert_type", "field_key")``. `key_fn` must return exactly these.
        items: the current collection (e.g. the current rule-violations).
        key_fn: maps one item to its natural key dict ``{kf: value}``.
        defaults_fn: maps one item to its ``defaults=`` field values.
        retire_filter: extra filter scoping **only** the retire pass — distinct
            from the per-child key. This is what keeps a machine reconcile off
            authored rows, e.g. ``{"alert_type__in": MACHINE_TYPES}``. Without
            it the reconcile would reap every row under `scope`.
        retire: ``"delete"`` (hard delete stale rows) or a *patch* dict for a
            soft-delete, e.g. ``{"resolved_at": event_ts}``. A patch retires via
            UPDATE and, to stay idempotent and fire one transition per real
            change, is guarded to rows whose patch fields are currently NULL
            (i.e. not already retired). The patch value must be the triggering
            event's timestamp, never ``timezone.now()``.

    Returns:
        One ``update_or_create`` Effect per item, followed by one retire Effect
        (``op="delete"`` or ``op="retire"``) sparing the present composite keys.
    """
    scope = dict(scope)
    retire_filter = dict(retire_filter or {})
    keys = [key_fn(item) for item in items]

    for k in keys:
        if tuple(k.keys()) != key_fields and set(k) != set(key_fields):
            raise ValueError(
                f"key_fn returned {sorted(k)} but key_fields is "
                f"{sorted(key_fields)}; they must match."
            )

    effects: list[Effect] = [
        Effect(
            op="update_or_create",
            model_label=model_label,
            lookup={**scope, **key},
            defaults=defaults_fn(item),
        )
        for item, key in zip(items, keys, strict=True)
    ]

    if retire == "delete":
        effects.append(
            Effect(
                op="delete",
                model_label=model_label,
                lookup={**scope, **retire_filter},
                spare_keys=keys,
            )
        )
    else:
        # Soft-delete: retire only rows not already carrying the patch fields,
        # so a re-run neither re-stamps a resolution nor re-fires a transition.
        open_guard = {f"{field}__isnull": True for field in retire}
        effects.append(
            Effect(
                op="retire",
                model_label=model_label,
                lookup={**scope, **retire_filter, **open_guard},
                spare_keys=keys,
                patch=dict(retire),
            )
        )
    return effects
