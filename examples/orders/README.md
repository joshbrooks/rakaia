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
just orders-live           # http://localhost:8002/live/ — a live-streaming demo
```

Every page carries a nav to a **How it works** explainer
(`http://localhost:8002/info/`) — Mermaid diagrams and code for the replay
pipeline, versioned handlers, multi-owner columns, and `op="update"`.

Or directly:

```sh
cd examples/orders
uv run python manage.py migrate
uv run python manage.py demo_orders --twice
```

Expected output (abridged):

```
Seeded 6 order events (tax rule changes at seq 3) + 2 loyalty-bonus events.
Dry run: replay would apply 12 effects (no writes yet).
[replay] events=8 effects_applied=12 external_skipped=4

order     status       subtotal   rate      tax     total  points  bonus
------------------------------------------------------------------------
ORD-1001  PAID            24.00 0.0000     0.00     24.00      24      0
ORD-1002  PENDING         15.00 0.0000     0.00     15.00       0      0
ORD-1003  PAID            40.00 0.0000     0.00     40.00      40     50
ORD-1004  PAID            30.00 0.1000     3.00     33.00      30      0
ORD-1005  PAID            58.00 0.1000     5.80     63.80      58      0
ORD-1006  CANCELLED       40.00 0.1000     4.00     44.00       0      0

op="update": bonus landed on ORD-1003 (+50) and minted NO row for ORD-9999 (a
bonus for an order never placed) — update_or_create would have left a phantom
row. ✓

Replayed again: 6 -> 6 rows — idempotent ✓
```

## What each feature does

| Feature | Where | What to notice |
|---|---|---|
| **Time-correct versions** | `handlers.py` — `order_totals` v1/v2 | Orders at seq < 3 get 0% tax; seq ≥ 3 get 10%. The bugfix-forward never rewrites history. |
| **Upcaster** | `upcasters.py` — `qty` → `quantity` | Events `ORD-1001/1002/1005` use the legacy `qty` key; the upcaster normalises them so handlers only ever see `quantity`. |
| **Sibling handler** | `handlers.py` — `order_loyalty` | Writes a *disjoint* `defaults` key (`loyalty_points`) on the same row as `order_totals` — no `EffectCollisionError`. Only PAID orders earn points. |
| **Update-if-exists** | `handlers.py` — `order_bonus` | A loyalty-bonus event decorates an *existing* order's `bonus_points` via `op="update"`. It's a **secondary owner**: the bonus for `ORD-1003` lands, but the bonus for the never-placed `ORD-9999` is a clean no-op — no phantom row. `update_or_create` couldn't express "update only if it exists". |
| **External effect** | `handlers.py` — `order_receipt` | A receipt email tagged `op="external"`. Replay **skips** it (`external_skipped=4`), so re-deriving state never re-sends mail. Pass `--include-external` to count them differently. |
| **Idempotency** | `--twice` | `update_or_create` converges: replaying again produces identical rows. |
| **Dry-run preview** | `demo_orders.py` — `CollectingExecutor` | Records the effects replay *would* apply without writing them. The dry-run count matches `effects_applied` — the same primitive you'd use to verify a migration before committing it. |

## Live mode (`just orders-live`)

`just orders-live` serves a live-streaming version at
**http://localhost:8002/live/**. A background *producer* thread invents a random
order every ~0.5–1.5 s, appends it to the event stream, and **incrementally
replays** just the new events (`replay(start_seq=…)`) through the very same
versioned handlers into `OrderSummary`. The page polls a JSON snapshot ~1×/sec
and shows two panels side by side: the raw **event stream** (newest first) and
the derived **projection** table — so you watch events flow in and the read model
update in real time.

About one in four events is a loyalty bonus applied with `op="update"`. The feed
labels each: **applied** when it lands on a real order, or **no-op — no such
order** when it targets a fabricated `ORD-GHOST-*`. Watch the projection: a
`ORD-GHOST-*` row **never appears**, because update-if-exists refuses to insert.
That's the whole point of the op, shown live.

There's also a small form to **submit your own order** (item, quantity, status);
it's enqueued to the producer and shows up on the next poll as an `ORD-YOU-*`
row.

Implementation notes:

* **One thread owns the stream.** rakaia's in-memory `StreamStore` is a
  process-wide singleton but isn't safe for a background writer racing request
  readers. So the producer thread is the only code that touches the store; web
  requests read the durable `OrderSummary` rows (sqlite) and a lock-guarded ring
  buffer of recent events. A form POST doesn't append — it enqueues the order for
  the producer. See `orders/live.py`.
* Because seqs keep climbing past the tax-rule change, live orders are all taxed
  at the v2 (10%) rate; the *time-correct* v1/v2 split is what the static
  `orders-demo` above exists to show.

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
  produced effects (`update_or_create` for the order rows, `update` for the
  loyalty bonus).
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
