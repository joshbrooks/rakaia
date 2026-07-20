"""
Projection helpers: turn a parent record with a repeated child collection into
a set of idempotent, orphan-free Effects.

The canonical shape is "one source record fans out into N child rows" — a
FormKit repeater, an order's line items, a form's answers. Replaying such a
projection with plain `update_or_create` leaks orphans: if a later version of
the parent has *fewer* children, the dropped child rows survive forever because
nothing deletes them.

Three helpers close that gap, each emitting idempotent upserts plus a single
reconcile `delete` scoped to spare exactly the rows still present:

* `reconcile_children` — a flat collection keyed by positional index.
* `reconcile_tree` — an unbounded nested tree keyed by stable node id, with the
  reconcile scoped to the whole subtree so orphans are pruned at any depth.
* `reconcile_aggregate` — one recomputed aggregate row per group, with the
  reconcile removing groups that no longer have any contributors.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Mapping, Sequence
from typing import Any

from .effects import Effect
from .types import StreamMessage


def reconcile_children(
    model_label: str,
    parent_lookup: dict[str, Any],
    child_key: str,
    items: Sequence[Any],
    defaults_fn: Callable[[Any], dict[str, Any]],
) -> list[Effect]:
    """Return Effects that materialise `items` as child rows without orphans.

    Keys each row by its positional index, so this fits **fixed-order or
    append-only** collections. For **reorderable** collections prefer
    `reconcile_tree`, keyed by a stable id with order as a fractional-index
    field — index keying renumbers (and rewrites) the tail on every reorder and
    breaks external references to a row. See ADR 0001.

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


def reconcile_tree(
    model_label: str,
    scope_lookup: dict[str, Any],
    node_key: str,
    nodes: Sequence[Any],
    id_fn: Callable[[Any], Any],
    defaults_fn: Callable[[Any], dict[str, Any]],
) -> list[Effect]:
    """Materialise an unbounded nested tree without orphans at any depth.

    The tree generalisation of `reconcile_children`: where that keys children by
    positional index under one parent, this keys each node by its **stable id**
    and scopes the reconcile delete to the whole ``scope_lookup`` (e.g. the
    submission), not a single parent level. Because the delete spares every kept
    id regardless of depth, pruning an entire subtree — whose intermediate
    parent is gone — leaves no deep orphans, which a per-parent reconcile would.

    This helper is **order-agnostic**: it neither sorts ``nodes`` nor imposes an
    order field. To make a reorderable collection read back in order, compute an
    order value yourself (a fractional index is recommended — see ADR 0001) and
    return it from ``defaults_fn``, then read with ``ORDER BY``. Likewise the
    identity stability it enables is only as stable as ``id_fn``: the ids must be
    **unique within ``nodes``** and persistent across submissions (a business key
    or one assigned at ingestion). Duplicate ids emit colliding upserts.

    Args:
        model_label: 'app_label.ModelName' of the node model.
        scope_lookup: lookup identifying the whole tree, e.g.
            ``{"submission_id": sid}``. Each node is keyed by this plus
            ``{node_key: id}``; the reconcile delete is scoped to it.
        node_key: the field holding a node's stable id, e.g. ``"node_id"``.
        nodes: the current node collection, flattened to any depth (order is
            preserved in the emitted upserts).
        id_fn: maps one node to its stable id (the ``node_key`` value).
        defaults_fn: maps one node to its ``defaults=`` field values (typically
            including its ``parent_node_id`` and payload).

    Returns:
        One ``update_or_create`` Effect per node, followed by one ``delete``
        Effect removing every node under ``scope_lookup`` whose id is no longer
        present. Empty ``nodes`` yields just the delete, clearing the subtree.
    """
    kept_ids = [id_fn(node) for node in nodes]
    effects: list[Effect] = [
        Effect(
            op="update_or_create",
            model_label=model_label,
            lookup={**scope_lookup, node_key: node_id},
            defaults=defaults_fn(node),
        )
        for node_id, node in zip(kept_ids, nodes, strict=True)
    ]
    effects.append(
        Effect(
            op="delete",
            model_label=model_label,
            lookup=dict(scope_lookup),
            exclude={f"{node_key}__in": kept_ids},
        )
    )
    return effects


def reconcile_aggregate(
    model_label: str,
    scope_lookup: dict[str, Any],
    group_key: str,
    groups: Mapping[Any, dict[str, Any]],
) -> list[Effect]:
    """Materialise one recomputed aggregate row per group, without stale rows.

    The aggregate analogue of `reconcile_children`. An increment is not
    replay-safe (re-running doubles the total), so the caller **recomputes** each
    group's aggregate from its contributing rows and passes the results here as
    ``{group_value: defaults}``. This emits one idempotent ``update_or_create``
    per group plus a reconcile ``delete`` that removes aggregate rows for groups
    which no longer have any contributors.

    .. warning::
        The reconcile delete is scoped to ``scope_lookup``. With a global scope
        (``scope_lookup={}``) **and** empty ``groups``, the delete matches every
        row in the model and clears the whole table. Pass a non-empty
        ``scope_lookup`` whenever the aggregate model holds more than this one
        rollup, so an empty recompute clears only that scope.

    Args:
        model_label: 'app_label.ModelName' of the aggregate model.
        scope_lookup: lookup bounding the aggregate set, e.g. ``{}`` for a global
            rollup or ``{"report_id": r}`` for a scoped one. Each row is keyed by
            this plus ``{group_key: value}``; the reconcile delete is scoped to
            it.
        group_key: the field identifying a group, e.g. ``"suku"`` or
            ``"district"``. (Single-field groups; composite groups are out of
            scope — a simple ``exclude`` cannot express "tuple not in set".)
        groups: mapping of group value to its recomputed ``defaults=`` values.

    Returns:
        One ``update_or_create`` Effect per group (in mapping order), followed by
        one ``delete`` Effect removing rows under ``scope_lookup`` whose group is
        not present. Empty ``groups`` yields just the delete, clearing the set.
    """
    group_values = list(groups)
    effects: list[Effect] = [
        Effect(
            op="update_or_create",
            model_label=model_label,
            lookup={**scope_lookup, group_key: value},
            defaults=dict(groups[value]),
        )
        for value in group_values
    ]
    effects.append(
        Effect(
            op="delete",
            model_label=model_label,
            lookup=dict(scope_lookup),
            exclude={f"{group_key}__in": group_values},
        )
    )
    return effects


def project_latest(
    messages: Sequence[StreamMessage],
    model_label: str,
    *,
    subject_of: Callable[[dict[str, Any]], Any],
    defaults_of: Callable[[StreamMessage, dict[str, Any]], dict[str, Any]],
    subject_field: str = "subject",
    tombstone_labels: Sequence[str] = ("delete",),
) -> list[Effect]:
    """Project each subject's **latest** snapshot into one row — the current-state
    read model behind an event log.

    Folds `messages` to the newest event per subject (oldest-first, last wins)
    and returns one ``update_or_create`` per live subject, plus a ``delete`` for
    any subject whose latest event is a **tombstone** (``label in
    tombstone_labels`` — e.g. a delete, Decision #2). Because every event is a
    full snapshot, "latest" needs no reducer.

    Unlike `reconcile_children` (parent → child fan-out), this is subject →
    latest-row, keyed on the aggregate identity (`subject_field`). It only
    touches subjects **present in** `messages`, so it serves a full-stream
    rebuild *and* an incremental tail read (``store.read(path, offset=…)``): a
    subject's absence is never a delete — only an explicit tombstone is.

    Args:
        messages: the stream's messages (from ``store.read``), oldest-first.
        model_label: 'app_label.ModelName' of the projection table.
        subject_of: maps a decoded event to its subject (the aggregate id).
        defaults_of: maps ``(message, decoded event)`` to the row's ``defaults=``.
        subject_field: the projection's key column.
        tombstone_labels: envelope labels that mean "no live row" for the subject.
    """
    latest: dict[Any, tuple[StreamMessage, dict[str, Any]]] = {}
    for msg in messages:
        event = json.loads(msg.data)
        latest[subject_of(event)] = (msg, event)

    tombstones = set(tombstone_labels)
    effects: list[Effect] = []
    for subject, (msg, event) in latest.items():
        if msg.label in tombstones:
            effects.append(
                Effect(
                    op="delete",
                    model_label=model_label,
                    lookup={subject_field: subject},
                )
            )
        else:
            effects.append(
                Effect(
                    op="update_or_create",
                    model_label=model_label,
                    lookup={subject_field: subject},
                    defaults=defaults_of(msg, event),
                )
            )
    return effects
