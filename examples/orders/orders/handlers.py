"""Versioned handlers for the `orders` stream.

Autodiscovered by django_rakaia on app ready(): importing this module runs the
`@register_handler` decorators below.

The stream carries two kinds of event: **order** events (the default) and
**loyalty-bonus** events (``kind="loyalty_bonus"`` — a promotional credit
against an order). Four handler *names* fire, each guarding on the kind it cares
about:

* ``order_totals`` — two versions split at the tax-rule change (seq 3). Old
  orders keep 0% tax; newer ones are taxed at 10%. This is the "time-correct"
  guarantee: fixing tax going forward never rewrites history.
* ``order_loyalty`` — a sibling handler awarding points on PAID orders. It
  writes a *disjoint* ``defaults`` key (``loyalty_points``) on the same row, so
  it never collides with ``order_totals``.
* ``order_receipt`` — an ``op="external"`` effect (a receipt email). Replay
  skips external effects by default, so re-deriving state never re-sends mail.
* ``order_bonus`` — the ``op="update"`` (update-if-exists) showcase. A
  loyalty-bonus event *decorates* an existing order row's ``bonus_points``
  column; it is a **secondary owner** that must never mint a row. Because
  ``op="update"`` updates in place and never inserts, a bonus referencing an
  order that was never placed is a clean no-op — not a phantom half-row (which
  ``op="update_or_create"`` would leave behind).

Handlers are pure: they take an (already upcasted) event dict and return an
``Effect`` description — no I/O, no DB writes. The DjangoExecutor applies them.
"""

from decimal import Decimal

from rakaia import Effect, register_handler

from .seed import TAX_CHANGE_SEQ

MODEL = "orders.OrderSummary"
GST_RATE = Decimal("0.10")

# Marker on a promotional loyalty-bonus event. Order events omit `kind`; the
# order handlers skip anything carrying this so a bonus event never drives the
# totals/loyalty/receipt logic, and `order_bonus` fires on nothing else.
BONUS_KIND = "loyalty_bonus"


def _is_bonus(event: dict) -> bool:
    return event.get("kind") == BONUS_KIND


def _subtotal(event: dict) -> Decimal:
    """Sum quantity * price across line items.

    Always reads `quantity` — the v1->v2 upcaster guarantees the legacy `qty`
    key has already been renamed by the time a handler sees the event.
    """
    total = Decimal("0")
    for item in event.get("items", []):
        total += Decimal(str(item["price"])) * Decimal(str(item["quantity"]))
    return total


def _totals_effect(event: dict, rate: Decimal) -> Effect:
    subtotal = _subtotal(event)
    tax = (subtotal * rate).quantize(Decimal("0.01"))
    return Effect(
        op="update_or_create",
        model_label=MODEL,
        lookup={"order_id": event["order_id"]},
        defaults={
            "status": event.get("status", ""),
            "currency": event.get("currency", "USD"),
            "subtotal": subtotal,
            "tax_rate": rate,
            "tax": tax,
            "total": subtotal + tax,
        },
    )


# ---------------------------------------------------------------------------
# order_totals v1 — pre-GST, no tax. Active for seq [0, 3).
# ---------------------------------------------------------------------------
@register_handler(
    name="order_totals",
    event_match="orders",
    effective_from=0,
    effective_to=TAX_CHANGE_SEQ,
)
def order_totals_v1(event: dict) -> Effect | None:
    if _is_bonus(event):
        return None  # a loyalty-bonus event has no order to total
    return _totals_effect(event, Decimal("0"))


# ---------------------------------------------------------------------------
# order_totals v2 — post-GST, 10% tax. Active for seq [3, None).
# ---------------------------------------------------------------------------
@register_handler(
    name="order_totals",
    event_match="orders",
    effective_from=TAX_CHANGE_SEQ,
    effective_to=None,
)
def order_totals_v2(event: dict) -> Effect | None:
    if _is_bonus(event):
        return None  # a loyalty-bonus event has no order to total
    return _totals_effect(event, GST_RATE)


# ---------------------------------------------------------------------------
# order_loyalty — sibling handler, all seqs. Writes a disjoint defaults key,
# so it coexists with order_totals on the same row without collision.
# ---------------------------------------------------------------------------
@register_handler(
    name="order_loyalty",
    event_match="orders",
    effective_from=0,
    effective_to=None,
)
def order_loyalty(event: dict) -> Effect | None:
    if _is_bonus(event) or event.get("status") != "PAID":
        return None  # only PAID orders earn points
    points = int(_subtotal(event))  # 1 point per whole currency unit
    return Effect(
        op="update_or_create",
        model_label=MODEL,
        lookup={"order_id": event["order_id"]},
        defaults={"loyalty_points": points},
    )


# ---------------------------------------------------------------------------
# order_receipt — external effect (email). Skipped on replay unless
# --include-external, so re-deriving state never re-sends the receipt.
# ---------------------------------------------------------------------------
@register_handler(
    name="order_receipt",
    event_match="orders",
    effective_from=0,
    effective_to=None,
)
def order_receipt(event: dict) -> Effect | None:
    if _is_bonus(event) or event.get("status") != "PAID":
        return None
    return Effect(
        op="external",
        kind="email",
        payload={
            "to": f"{event['order_id']}@example.test",
            "template": "order_receipt",
            "order_id": event["order_id"],
        },
    )


# ---------------------------------------------------------------------------
# order_bonus — the op="update" (update-if-exists) showcase. Fires only on
# loyalty-bonus events. It decorates an *existing* order's bonus_points column
# and never inserts: a bonus for an order that was never placed is a no-op, not
# a phantom row. Contrast order_totals/order_loyalty, which use update_or_create
# because they co-own the row and legitimately create it.
# ---------------------------------------------------------------------------
@register_handler(
    name="order_bonus",
    event_match="orders",
    effective_from=0,
    effective_to=None,
)
def order_bonus(event: dict) -> Effect | None:
    if not _is_bonus(event):
        return None  # only loyalty-bonus events award a bonus
    return Effect(
        op="update",
        model_label=MODEL,
        lookup={"order_id": event["order_id"]},
        defaults={"bonus_points": int(event["bonus"])},
    )
