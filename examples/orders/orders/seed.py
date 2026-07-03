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
