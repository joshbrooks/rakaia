"""Sample order events, appended to the stream by `demo_orders`.

Each dict is one event. Its position in this list becomes its stream sequence
number (seq), which is what selects the handler *version* at replay time:

    seq 0, 1, 2  -> pre-GST  -> order_totals v1  (0% tax)
    seq 3, 4, 5  -> post-GST -> order_totals v2  (10% tax)

Independently, each event declares a `schema_version`:

    schema_version 1  uses the legacy `qty` line-item key
    schema_version 2  uses the current `quantity` key

The v1->v2 upcaster (see upcasters.py) renames `qty` -> `quantity` on read, so
handlers only ever see `quantity`. Note seq 4 is *post-GST* but still *schema
v1* — it exercises the upcaster and the v2 tax handler at once.

Loyalty points are only awarded on PAID orders; PENDING/CANCELLED orders show
the sibling handler returning no effect.

`SAMPLE_BONUSES` (below the orders) are promotional `kind="loyalty_bonus"`
events, appended after the orders so they carry the highest seqs. They drive the
`order_bonus` handler's `op="update"` — the update-if-exists showcase.
"""

# The tax rule changed at this sequence boundary: orders placed before it keep
# 0% tax; orders from here on are taxed at 10%. Kept in sync with the
# `effective_from` / `effective_to` on the handlers in handlers.py.
TAX_CHANGE_SEQ = 3

SAMPLE_ORDERS: list[dict] = [
    # ---- pre-GST (seq 0..2, order_totals v1, 0% tax) ----
    {
        "schema_version": 1,  # legacy `qty` key -> upcaster fires
        "order_id": "ORD-1001",
        "status": "PAID",
        "currency": "USD",
        "items": [
            {"sku": "TEA-EARL", "qty": 2, "price": "6.00"},
            {"sku": "MUG-CERAMIC", "qty": 1, "price": "12.00"},
        ],
    },
    {
        "schema_version": 1,  # legacy `qty` key -> upcaster fires
        "order_id": "ORD-1002",
        "status": "PENDING",  # not PAID -> loyalty handler returns None
        "currency": "USD",
        "items": [
            {"sku": "TEA-GREEN", "qty": 3, "price": "5.00"},
        ],
    },
    {
        "schema_version": 2,
        "order_id": "ORD-1003",
        "status": "PAID",
        "currency": "USD",
        "items": [
            {"sku": "KETTLE-STEEL", "quantity": 1, "price": "40.00"},
        ],
    },
    # ---- post-GST (seq 3..5, order_totals v2, 10% tax) ----
    {
        "schema_version": 2,
        "order_id": "ORD-1004",
        "status": "PAID",
        "currency": "USD",
        "items": [
            {"sku": "TEA-EARL", "quantity": 5, "price": "6.00"},
        ],
    },
    {
        "schema_version": 1,  # post-GST *and* legacy schema -> both fire
        "order_id": "ORD-1005",
        "status": "PAID",
        "currency": "USD",
        "items": [
            {"sku": "MUG-CERAMIC", "qty": 4, "price": "12.00"},
            {"sku": "TEA-GREEN", "qty": 2, "price": "5.00"},
        ],
    },
    {
        "schema_version": 2,
        "order_id": "ORD-1006",
        "status": "CANCELLED",  # not PAID -> loyalty handler returns None
        "currency": "USD",
        "items": [
            {"sku": "KETTLE-STEEL", "quantity": 1, "price": "40.00"},
        ],
    },
]

# Promotional loyalty-bonus events (kind="loyalty_bonus"), appended *after* all
# the orders so they carry the highest seqs. Each credits `bonus` points to an
# order via the `order_bonus` handler's op="update" — update-if-exists:
#
#   * ORD-1003 exists (seeded above) -> the bonus lands on its row.
#   * ORD-9999 was never placed       -> op="update" is a clean no-op. NO row is
#     minted. (update_or_create would leave a phantom half-row here — the exact
#     footgun op="update" removes.)
#
# Because single-stage replay applies each event before the next, a bonus that
# targets an already-seen order finds the row waiting for it.
SAMPLE_BONUSES: list[dict] = [
    {
        "schema_version": 2,
        "kind": "loyalty_bonus",
        "order_id": "ORD-1003",  # a real, already-placed order
        "bonus": 50,
    },
    {
        "schema_version": 2,
        "kind": "loyalty_bonus",
        "order_id": "ORD-9999",  # no such order was ever placed
        "bonus": 10,
    },
]
