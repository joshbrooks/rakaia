# Projections & fan-out

A handler often needs to turn **one source event into many rows** — a form
submission with a repeater, an order with line items, a survey with answers.
This page covers how to project such collections safely, and the one trap to
avoid: **orphans on shrink**.

## `@stream_model` is not this

It's worth stating plainly, because the names are close. The
[`@stream_model`](django-integration.md) decorator emits **one stream event per
model save** — it turns a Django model's lifecycle into a stream. That is the
*opposite* direction from what this page is about: here we take *one* event and
fan it out into *many* projection rows. Don't reach for `@stream_model` to
decompose a nested payload; write a fan-out handler instead.

## The fan-out handler

A handler may return a **list** of `Effect`s. One update per child row, keyed by
the parent plus the child's index:

```python
from rakaia import Effect, register_handler

@register_handler(name="activity_rows", event_match="submissions",
                  effective_from=0)
def activity_rows(event: dict) -> list[Effect]:
    sid = event["submission_id"]
    return [
        Effect(
            op="update_or_create",
            model_label="submissions.ActivityProgress",
            lookup={"submission_id": sid, "activity_index": i},
            defaults={"name": a["name"], "progress_pct": a["progress_pct"]},
        )
        for i, a in enumerate(event["fields"]["activities"])
    ]
```

This is idempotent — replaying converges, because each row is an
`update_or_create` keyed on `(submission_id, activity_index)`.

## The orphan trap

The handler above has a latent bug. Suppose a submission is **corrected** and
re-appended with *fewer* activities than before:

```
seq 3  submission S  -> activities [A, B, C]   # writes indices 0, 1, 2
seq 9  submission S  -> activities [A, B]       # writes indices 0, 1
```

Replaying the whole stream leaves the index-2 row (`C`) alive forever. Nothing
in the fan-out ever deletes it — `update_or_create` only ever writes. Any
collection projection that can shrink hits this.

## `reconcile_children`

`reconcile_children` is the fix: it emits the per-child upserts **and** a single
reconcile `delete` that removes every child under the parent *except* the
indices still present.

```python
from rakaia import register_handler, reconcile_children

@register_handler(name="activity_rows", event_match="submissions",
                  effective_from=0)
def activity_rows(event: dict):
    return reconcile_children(
        model_label="submissions.ActivityProgress",
        parent_lookup={"submission_id": event["submission_id"]},
        child_key="activity_index",
        items=event["fields"]["activities"],
        defaults_fn=lambda a: {"name": a["name"], "progress_pct": a["progress_pct"]},
    )
```

For `items=[A, B]` it returns two `update_or_create` Effects (indices 0 and 1)
followed by:

```python
Effect(
    op="delete",
    model_label="submissions.ActivityProgress",
    lookup={"submission_id": sid},
    exclude={"activity_index__in": [0, 1]},   # spare the current children
)
```

so the stale index-2 row is pruned. An empty `items` yields just the delete,
which removes every child under the parent.

The `DjangoExecutor` applies **all upserts before any deletes** within one
transaction, so a reconcile batch converges regardless of the order handlers
returned effects in, and the delete never races the writes it's meant to keep.

## Why not a raw `delete` every time?

You can build the same thing by hand with an `op="delete"` Effect — that's all
`reconcile_children` does. The helper exists so every collection projection gets
the orphan handling for free instead of re-deriving it (and forgetting the
`exclude`, which is the whole point). Reach for the raw delete only when the
scope isn't "children of a parent keyed by index".
