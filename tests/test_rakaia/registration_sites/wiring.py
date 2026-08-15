"""Where the registrations happen. The functions come from `logic`.

An app's `handlers.py` that wires shared functions to streams — the layout
`functools.partial` dependency binding pushes people towards.
"""

from __future__ import annotations

import functools

from rakaia.registry import register_handler, register_reducer, register_upcaster

from . import handler_registry, logic, upcaster_registry

register_handler("room", "room:*", 0, registry=handler_registry)(
    functools.partial(logic.project_room, prefix="p")
)
register_reducer("rooms", 1, registry=handler_registry)(logic.reduce_rooms)
register_upcaster("room:*", 1, registry=upcaster_registry)(logic.upcast_room)
