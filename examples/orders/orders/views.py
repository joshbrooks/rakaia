"""Read-only view of the materialized order projection.

The rows shown here are written by replaying the event stream (see the
`demo_orders` management command). This view never mutates anything — it just
displays whatever `OrderSummary` rows replay has produced.
"""

from typing import Any

from django.http import HttpResponse
from django.shortcuts import render
from django.views.decorators.http import require_GET

from .models import OrderSummary
from .seed import TAX_CHANGE_SEQ


@require_GET
def index(request: Any) -> HttpResponse:
    orders = OrderSummary.objects.all()
    context = {
        "orders": orders,
        "tax_change_seq": TAX_CHANGE_SEQ,
    }
    return render(request, "orders/index.html", context)
