"""
Provenance middleware — the rakaia analogue of `pghistory.middleware.HistoryMiddleware`.

Add it to `MIDDLEWARE` (after `AuthenticationMiddleware`) and every stream append
made while handling a mutating request is stamped with the acting user and the
request path in its envelope `metadata`:

    MIDDLEWARE = [..., "django_rakaia.middleware.ProvenanceMiddleware"]

    # in a view: store.append(path, data, AppendOptions(label="update"))
    # -> metadata == {"user": <request.user.pk>, "url": <request.path>}

It only wraps mutating methods (POST/PUT/PATCH/DELETE), so reads add no overhead.

Caveat: it resolves `request.user` at request-entry time, which works for
session/`AuthenticationMiddleware` auth. Frameworks that authenticate *inside*
the view (DRF/ninja token auth) set the user later — for those, set provenance
explicitly with `rakaia.context.provenance(...)` in the view, or subclass this
to hook the late assignment (as pghistory does).
"""

from __future__ import annotations

from collections.abc import Callable
from typing import Any

from rakaia.context import provenance

_MUTATING = frozenset({"POST", "PUT", "PATCH", "DELETE"})


class ProvenanceMiddleware:
    """Stamp the acting user + request path onto envelope metadata per request."""

    def __init__(self, get_response: Callable[[Any], Any]) -> None:
        self.get_response = get_response

    def __call__(self, request: Any) -> Any:
        if request.method not in _MUTATING:
            return self.get_response(request)
        user = getattr(request, "user", None)
        uid = (
            user.pk
            if user is not None and getattr(user, "is_authenticated", False)
            else None
        )
        with provenance(user=uid, url=request.path):
            return self.get_response(request)
