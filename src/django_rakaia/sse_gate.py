"""Whether this deployment wires its live-update surface — decided once.

`channels` is an optional extra (#41), so every part of the package that touches
it has to ask the same question first: does this deployment want SSE, and is the
library there? The answer has three states and they are not obvious, which is
why it belongs in one place rather than being re-derived per caller:

* ``RAKAIA_ENABLE_SSE = False`` — never load. The consumer has said this tier
  does not do live updates, and is entitled to not have the library installed.
* ``RAKAIA_ENABLE_SSE = True`` — load, and let an `ImportError` propagate. The
  consumer asked for SSE and did not install the extra; that is a real
  misconfiguration and silently downgrading it to polling would hide it.
* unset — auto-detect. Load if it imports, skip quietly if not, which is the
  framework-only consumer who never wanted SSE and never said so.

`apps.py` had this written out for its signal handlers and `urls.py` had nothing,
which is #230: the URL file imported the SSE view at module scope, so a
polling-only tier could not use the documented ``include()`` without installing
`channels` and, in production, a Redis channel layer. The setting could not help
because Django imports the URLconf before an `AppConfig` gets a say.
"""

from __future__ import annotations

from importlib import import_module
from typing import Any

from django.conf import settings

__all__ = ["sse_import"]


def sse_import(module: str, attr: str | None = None) -> Any | None:
    """Import `module` (or `attr` from it) under the SSE gate, or return None.

    Returns None when this deployment has opted out or has no `channels`, and
    raises `ImportError` when it explicitly opted *in* without the library. See
    the module docstring for why those are three cases and not two.
    """
    enabled = getattr(settings, "RAKAIA_ENABLE_SSE", None)
    if enabled is False:
        return None
    try:
        loaded = import_module(module)
    except ImportError:
        if enabled:
            raise
        return None
    return loaded if attr is None else getattr(loaded, attr)
