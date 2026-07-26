"""Materialized projection written by the versioned handlers.

`OrderSummary` is *derived* state — it is never edited directly. The order
event stream is the source of truth; replay re-derives every row via
`update_or_create`, which is why re-running `demo_orders` converges instead of
duplicating rows.
"""

from django.db import models


class OrderSummary(models.Model):
    """One materialized row per order, derived by replaying the stream."""

    order_id = models.CharField(max_length=64, unique=True)
    status = models.CharField(max_length=32, default="")
    currency = models.CharField(max_length=8, default="USD")

    # Written by the `order_totals` handler (v1 = no tax, v2 = 10% tax).
    subtotal = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    tax_rate = models.DecimalField(max_digits=5, decimal_places=4, default=0)
    tax = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    total = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    # Written by the sibling `order_loyalty` handler (disjoint defaults key).
    loyalty_points = models.IntegerField(default=0)

    # Written *only* by the `order_bonus` handler via op="update" — a secondary
    # owner that decorates an existing row and must never mint one. A bonus for
    # an order that was never placed is a clean no-op, not a phantom row.
    bonus_points = models.IntegerField(default=0)

    class Meta:
        ordering = ["order_id"]

    def __str__(self) -> str:
        return f"{self.order_id} ({self.total} {self.currency})"
