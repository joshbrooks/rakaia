"""Versioned handlers for the `orders` stream.

Autodiscovered by django_rakaia on app ready(): importing this module runs the
`@register_handler` decorators below.

Three handler *names* fire on every order event, independently:

* ``order_totals`` — two versions split at the tax-rule change (seq 3). Old
  orders keep 0% tax; newer ones are taxed at 10%. This is the "time-correct"
  guarantee: fixing tax going forward never rewrites history.
* ``order_loyalty`` — a sibling handler awarding points on PAID orders. It
  writes a *disjoint* ``defaults`` key (``loyalty_points``) on the same row, so
  it never collides with ``order_totals``.
* ``order_receipt`` — an ``op="external"`` effect (a receipt email). Replay
  skips external effects by default, so re-deriving state never re-sends mail.

Handlers are pure: they take an (already upcasted) event dict and return an
``Effect`` description — no I/O, no DB writes. The DjangoExecutor applies them.
"""

from decimal import Decimal

from rakaia.effects import Effect
from rakaia.registry import register_handler

from .seed import TAX_CHANGE_SEQ

MODEL = "orders.OrderSummary"
GST_RATE = Decimal("0.10")


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
def order_totals_v1(event: dict) -> Effect:
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
def order_totals_v2(event: dict) -> Effect:
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
    if event.get("status") != "PAID":
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
    if event.get("status") != "PAID":
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
