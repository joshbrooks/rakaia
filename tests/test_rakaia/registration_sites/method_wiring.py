"""A handler registered as a bound method — dotted path ``pkg.mod.Class.method``.

Defined and registered in *different* modules as well, so this case is only
about the extra qualname segment a method carries.
"""

from __future__ import annotations

from rakaia.registry import register_handler

from . import handler_registry, logic

register_handler("methods", "room:*", 0, registry=handler_registry)(
    logic.Projector().project
)
