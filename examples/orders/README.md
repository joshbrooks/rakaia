# Orders — versioned handlers, upcasters & replay

A tiny e-commerce projection that shows why versioned handlers exist: **the
sales-tax rule changed on a date, and old orders must keep the tax that was
correct when they were placed.** Replaying the order event stream re-derives an
`OrderSummary` row per order — time-correctly and idempotently.

This is the runnable companion to [`docs/versioned-handlers.md`](../../docs/versioned-handlers.md).

## Run

```sh
just orders-demo           # seed + replay, prints the projection table
just orders-dev            # http://localhost:8002 — same rows in the browser
```

Or directly:

```sh
cd examples/orders
uv run python manage.py migrate
uv run python manage.py demo_orders --twice
```

Expected output (abridged):

```
Seeded 6 order events (tax rule changes at seq 3).
[replay] events=6 effects_applied=10 external_skipped=4

order     status       subtotal   rate      tax     total  points
-----------------------------------------------------------------
ORD-1001  PAID            24.00 0.0000     0.00     24.00      24
ORD-1002  PENDING         15.00 0.0000     0.00     15.00       0
ORD-1003  PAID            40.00 0.0000     0.00     40.00      40
ORD-1004  PAID            30.00 0.1000     3.00     33.00      30
ORD-1005  PAID            58.00 0.1000     5.80     63.80      58
ORD-1006  CANCELLED       40.00 0.1000     4.00     44.00       0

Replayed again: 6 -> 6 rows — idempotent ✓
```

## What each feature does

| Feature | Where | What to notice |
|---|---|---|
| **Time-correct versions** | `handlers.py` — `order_totals` v1/v2 | Orders at seq < 3 get 0% tax; seq ≥ 3 get 10%. The bugfix-forward never rewrites history. |
| **Upcaster** | `upcasters.py` — `qty` → `quantity` | Events `ORD-1001/1002/1005` use the legacy `qty` key; the upcaster normalises them so handlers only ever see `quantity`. |
| **Sibling handler** | `handlers.py` — `order_loyalty` | Writes a *disjoint* `defaults` key (`loyalty_points`) on the same row as `order_totals` — no `EffectCollisionError`. Only PAID orders earn points. |
| **External effect** | `handlers.py` — `order_receipt` | A receipt email tagged `op="external"`. Replay **skips** it (`external_skipped=4`), so re-deriving state never re-sends mail. Pass `--include-external` to count them differently. |
| **Idempotency** | `--twice` | `update_or_create` converges: replaying again produces identical rows. |

## How it fits together

* **`seed.py`** — the sample order events. Position in the list = stream `seq`,
  which selects the handler *version*. Each event also carries a
  `schema_version` that drives the upcaster chain.
* **`upcasters.py` / `handlers.py`** — autodiscovered by `django_rakaia` on app
  `ready()` (it imports every installed app's `handlers.py` and `upcasters.py`),
  so the `@register_*` decorators populate the process-wide registries with no
  manual wiring.
* **`management/commands/demo_orders.py`** — seeds the in-memory stream and calls
  `rakaia.replay.replay(...)` with the `DjangoExecutor`, which applies the
  produced effects via `update_or_create`.
* **`models.py` / `views.py`** — `OrderSummary` is the materialized projection;
  the view just displays it.

### Why seed + replay live in one command

`rakaia`'s `StreamStore` is **in-memory and process-local** — the event log is
not persisted to the database (only the derived `OrderSummary` rows are). So a
separate `manage.py replay orders` invocation would find an empty stream. The
`demo_orders` command therefore seeds and replays in a single process. In a real
deployment you'd back the stream with a durable store, at which point the
built-in `manage.py replay <stream>` command works exactly the same way.

## Drift detection

Not scripted here, because a single-process run computes the registration hash
and the live hash from the same source — they always match. See
[`docs/versioned-handlers.md#drift-detection`](../../docs/versioned-handlers.md)
for how `--strict-drift` catches a handler whose "frozen" source was edited after
old events were already processed under it. The `--strict-drift` flag is wired
through on `demo_orders` so you can see it pass on a clean run.
