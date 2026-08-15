"""Two modules that split *where a handler is defined* from *where it is
registered* — the shape `RegistrationLog.modules()` has to survive.

`logic` holds the functions and a class; `wiring` and `method_wiring` are the
only modules that call a registration decorator. Nothing registers at
package-import time, so importing the package is inert and a wiring module can
be imported, dropped from `sys.modules` and re-imported to simulate the fresh
process `rehydrate()` exists for.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from rakaia.registry import HandlerRegistry, UpcasterRegistry

#: Set by a test *before* a wiring module is imported; the wiring modules
#: register against these rather than the process-wide defaults.
handler_registry: HandlerRegistry | None = None
upcaster_registry: UpcasterRegistry | None = None
