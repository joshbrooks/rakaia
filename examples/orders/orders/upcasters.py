"""Schema upcasters for the `orders` stream.

Autodiscovered by django_rakaia on app ready(): importing this module runs the
`@register_upcaster` decorator, registering the v1->v2 step in the process-wide
UpcasterRegistry.
"""

from rakaia.registry import register_upcaster


def _rename_qty(item: dict) -> dict:
    """Return a copy of a line item with `qty` renamed to `quantity`."""
    if "qty" not in item:
        return item
    renamed = {k: v for k, v in item.items() if k != "qty"}
    renamed["quantity"] = item["qty"]
    return renamed


@register_upcaster(event_match="orders", from_version=1)
def upcast_qty_to_quantity(event: dict) -> dict:
    """v1 -> v2: rename the legacy `qty` line-item key to `quantity`.

    Older order events used `qty`; newer producers emit `quantity`. Normalising
    here means every handler only ever sees `quantity`, no matter how old the
    event is. Pure: never mutates the input event.
    """
    items = [_rename_qty(item) for item in event.get("items", [])]
    return {**event, "items": items}
