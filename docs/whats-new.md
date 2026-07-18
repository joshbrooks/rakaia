---
icon: lucide/sparkles
---

# What's new — a guided tour

New here and evaluating rakaia? This page is the fast path. It walks the
capabilities added most recently, each with the problem it solves, a small
snippet, and a **one-command demo you can run to prove it** — no setup beyond
`just install`.

Rakaia is a [Durable Streams protocol](protocol.md) server plus a Django
integration. On top of the raw stream it adds an **event-sourcing layer**: your
projections (database rows) are *derived* from an append-only event log by pure,
versioned handlers, so you can replay history and get time-correct results
instead of whatever today's code would compute. The features below are what make
that practical to adopt.

## Run the whole tour at once

```bash
just install     # sync all dependency groups
just demo        # runs the scripted demos end-to-end, with narration
```

`just demo` runs the two headless demos below and then points you at the two live
SSE demos. To jump straight to one, use its command from the table.

| Example | Proves | Command |
|---|---|---|
| [`orders`](../examples/orders/) | Versioned handlers, upcasters, replay, dry-run | `just orders-demo` |
| [`formkit_submissions`](../examples/formkit_submissions/) | Projections/fan-out, `reconcile_children`, migration parity | `just formkit-demo` |
| [`chat`](../examples/chat/) | `@stream_model`, multi-stream events, live SSE | `just dev` → http://localhost:8000 |
| [`polyglot`](../examples/polyglot/) | Language-scoped streams, live-editable translations | `just polyglot-dev` → http://localhost:8001 |

---

## 1. Versioned handlers & replay

**The problem.** Business rules change. A tax rate, a completion threshold, a
mapping — the rule that was correct last quarter isn't the rule today. If your
projections are computed by *current* code, re-running it over old events
silently rewrites history.

**What rakaia does.** Handlers are registered against a **sequence range**, so an
old event runs through the handler version that was correct *when it happened*.
Fixing a rule forward never rewrites the past.

```python
from rakaia import Effect, register_handler

@register_handler(name="order_totals", event_match="orders",
                  effective_from=0, effective_to=3)      # tax-free era
def order_totals_v1(event):
    return Effect(op="update_or_create", model_label="orders.OrderSummary",
                  lookup={"order_id": event["order_id"]},
                  defaults={"tax": 0})

@register_handler(name="order_totals", event_match="orders",
                  effective_from=3)                       # 10% from seq 3 on
def order_totals_v2(event):
    ...
```

Replay is idempotent (`update_or_create`), so you can re-run it safely, and it
detects **drift** — a "frozen" historical handler whose source was edited after
old events already ran through it (`--strict-drift`).

→ Deep dive: [Versioned handlers](versioned-handlers.md) · Demo: `just orders-demo`

## 2. Upcasters — evolve event schemas

**The problem.** Producers rename fields (`qty` → `quantity`). You don't want
every handler to special-case old payloads forever.

**What rakaia does.** An **upcaster** is a pure function that migrates an event
from one schema version to the next, applied on read *before* handlers see it.
Handlers only ever deal with the latest shape.

```python
from rakaia import register_upcaster

@register_upcaster(event_match="orders", from_version=1)
def rename_qty(event):
    return {**event, "quantity": event.pop("qty")}
```

→ Deep dive: [Versioned handlers — Concepts](versioned-handlers.md#concepts) · Demo: `just orders-demo`

## 3. Projections & fan-out without orphans

**The problem.** One event often projects into *many* rows (a form's repeater, an
order's line items). When a later event has fewer children, a naive
`update_or_create` fan-out leaves the dropped rows orphaned.

**What rakaia does.** `reconcile_children` emits the per-child upserts **and** a
reconcile delete in one transaction, so the projection always converges to
exactly the children the event describes.

```python
from rakaia import reconcile_children

def activity_rows(event):
    return reconcile_children(
        model_label="app.ActivityProgress",
        parent_lookup={"submission_id": event["id"]},
        child_key="activity_index",   # each child keyed by its position
        items=event["activities"],
        defaults_fn=lambda a: {"progress": a["pct"]},
    )
```

→ Deep dive: [Projections & fan-out](projections-and-fan-out.md) · Demo: `just formkit-demo`

## 4. Dry-run any replay

**The problem.** Before you cut a projection over to event-sourcing — or run a
big backfill — you want to know exactly what it will write.

**What rakaia does.** Handlers return effect *descriptions*, so a
`CollectingExecutor` records what a replay *would* do with zero side effects.
The dry-run count matches the real write count exactly.

```bash
python manage.py replay orders --dry-run
```

→ Deep dive: [Dry-run & executors](dry-run-and-executors.md) · Shown in both demos

## 5. A durable, database-backed store

**The problem.** The default in-memory store is process-local — great for a
demo, but the event log vanishes on restart, so you can't emit from a request
and replay later in another process.

**What rakaia does.** Set `RAKAIA_STORE = "durable"` and events persist in your
database via the normalized `Stream` / `StreamEvent` / `StreamEntry` models. Now
`manage.py replay <stream>` works across processes, unchanged.

→ Deep dive: [Adopting the durable store](django-integration.md#adopting-the-durable-store) · Used by `just formkit-demo`

## 6. Live SSE with `@stream_model`

**The problem.** You want real-time fan-out to browsers without hand-rolling a
pub/sub layer.

**What rakaia does.** Decorate a Django model; every save/delete emits a stream
event, broadcast to connected clients over Server-Sent Events via Channels.

```python
@stream_model(
    stream_paths=lambda o: f"room:{o.id}:messages",
    to_dataclass=lambda o: RoomData(id=o.id, name=o.name),
)
class ChatRoom(models.Model):
    name = models.CharField(max_length=100)
```

→ Deep dive: [Django integration](django-integration.md) · Demos: `just dev`, `just polyglot-dev`

## 7. Protocol conformance

Rakaia is checked against the upstream, language-agnostic
[`@durable-streams/server-conformance-tests`](https://github.com/durable-streams/durable-streams)
suite in CI. It passes the full protocol surface today except the stream
**forking** family (not yet implemented). Run it yourself with `just conformance`
(needs node/npm); details in [`conformance/README.md`](../conformance/README.md).

---

## Where to go next

- Want the reference for handlers, upcasters and drift? → [Versioned handlers](versioned-handlers.md)
- Adopting rakaia over an existing signal-based pipeline? → the
  [`formkit_submissions`](../examples/formkit_submissions/) spike proves a
  parity migration end-to-end.
- Deploying? → [Deployment](deployment.md).
