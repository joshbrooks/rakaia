"""Views for the orders projection — a static page and a live-updating one.

The static `index` page displays whatever `OrderSummary` rows a `demo_orders`
replay has produced. The `live_*` views back `just orders-live`: a page that
polls a JSON snapshot ~1×/sec while a background producer (see `live.py`) streams
random orders through the same replay pipeline. None of these views mutate the
stream directly — `live_submit` enqueues an order for the producer thread, the
only code that touches the in-memory store.
"""

from __future__ import annotations

from typing import Any

from django.http import HttpResponse, HttpResponseRedirect, JsonResponse
from django.shortcuts import render
from django.urls import reverse
from django.views.decorators.http import require_GET, require_http_methods

from .live import _SKUS, _STATUSES, get_producer
from .models import OrderSummary
from .seed import TAX_CHANGE_SEQ


def _order_rows() -> list[dict[str, Any]]:
    return [
        {
            "order_id": o.order_id,
            "status": o.status,
            "subtotal": str(o.subtotal),
            "tax_rate": str(o.tax_rate),
            "tax": str(o.tax),
            "total": str(o.total),
            "loyalty_points": o.loyalty_points,
            "bonus_points": o.bonus_points,
        }
        for o in OrderSummary.objects.all()
    ]


@require_GET
def index(request: Any) -> HttpResponse:
    context = {
        "orders": OrderSummary.objects.all(),
        "tax_change_seq": TAX_CHANGE_SEQ,
    }
    return render(request, "orders/index.html", context)


@require_GET
def info(request: Any) -> HttpResponse:
    """Explainer page: how the derived OrderSummary read model is built."""
    return render(request, "orders/info.html", {})


@require_GET
def live_index(request: Any) -> HttpResponse:
    """The live page shell. Starts the producer on first hit so `just
    orders-live` needs nothing more than a runserver."""
    get_producer().ensure_started()
    return render(
        request,
        "orders/live.html",
        {"skus": [s[0] for s in _SKUS], "statuses": sorted(set(_STATUSES))},
    )


@require_GET
def live_data(request: Any) -> JsonResponse:  # noqa: ARG001
    """JSON snapshot the live page polls: current projection + recent events."""
    producer = get_producer()
    producer.ensure_started()
    return JsonResponse(
        {
            "orders": _order_rows(),
            "events": producer.recent(),
            "stats": producer.stats(),
        }
    )


@require_http_methods(["POST"])
def live_submit(request: Any) -> HttpResponse:
    """Enqueue a browser-submitted order for the producer to append + replay."""
    producer = get_producer()
    producer.ensure_started()

    sku = request.POST.get("sku") or _SKUS[0][0]
    price = dict(_SKUS).get(sku, "10.00")
    try:
        quantity = max(1, min(20, int(request.POST.get("quantity", "1"))))
    except ValueError:
        quantity = 1
    status = request.POST.get("status", "PAID")
    if status not in _STATUSES:
        status = "PAID"

    order = {
        "schema_version": 2,
        "order_id": (request.POST.get("order_id") or "").strip()
        or f"ORD-YOU-{producer.stats()['events'] + 1}",
        "status": status,
        "currency": "USD",
        "items": [{"sku": sku, "quantity": quantity, "price": price}],
    }
    producer.submit(order)

    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return HttpResponse(status=204)
    return HttpResponseRedirect(reverse("orders:live"))
