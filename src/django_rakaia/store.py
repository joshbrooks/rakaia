import threading
from typing import Any

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from rakaia import StreamStore

#: The backends ``RAKAIA_STORE`` accepts, mapped to what they build. Anything
#: else is a configuration error, not a fallback — see `_build_store`.
BACKENDS = ("memory", "durable")

DEFAULT_BACKEND = "memory"

# One cached store instance per backend name. Keeping the in-memory store a
# process-wide singleton is essential — it holds all stream state in memory —
# so we never rebuild or replace it. The durable store is stateless (state
# lives in the DB) but is cached for symmetry.
_stores: dict[str, Any] = {}
_store_lock = threading.Lock()


def _build_store(backend: str) -> Any:
    """Build the store `backend` names, or refuse.

    An unrecognised name is **refused rather than defaulted**. This used to
    return the in-memory store for any string that wasn't exactly ``"durable"``,
    which made the one mistake that matters — a misspelt backend — invisible:
    ``RAKAIA_STORE = "durrable"`` selected a process-local dict, so every append
    a deployment believed it was persisting was lost on the next restart, with
    nothing in the logs. Since the setting is free-form text, the selector is
    the only place that can tell a typo from a choice.

    ADR 0002 flagged this ("swaps stores by string with no interface check").
    `django_rakaia.checks` reports the same problem at startup, so a misspelling
    fails `manage.py check` rather than waiting for the first append.
    """
    if backend == "durable":
        from .django_store import DjangoStreamStore

        return DjangoStreamStore()
    if backend == "memory":
        return StreamStore()
    raise ImproperlyConfigured(
        f"RAKAIA_STORE={backend!r} is not a known store backend. "
        f"Valid backends are: {', '.join(repr(b) for b in BACKENDS)}. "
        f"(The default is {DEFAULT_BACKEND!r}, which is in-memory and "
        f"process-local — it does not survive a restart.)"
    )


def get_store() -> Any:
    """
    Get the configured stream store.

    Selected by the ``RAKAIA_STORE`` Django setting: ``"memory"`` (default) for
    the in-memory `StreamStore`, or ``"durable"`` for the DB-backed
    `DjangoStreamStore`. Any other value raises `ImproperlyConfigured`.
    Thread-safe lazy initialization, one instance per backend.
    """
    backend = getattr(settings, "RAKAIA_STORE", DEFAULT_BACKEND)
    with _store_lock:
        if backend not in _stores:
            # Built before the assignment, so a refused backend leaves no entry
            # behind: correcting the setting works without restarting.
            _stores[backend] = _build_store(backend)
    return _stores[backend]


def reset_store_cache() -> None:
    """Drop every memoised store, so the next `get_store()` rebuilds.

    **For tests.** `get_store()` memoises per backend name, which is what makes
    the in-memory store a process-wide singleton — correct in production, and
    exactly what leaks between tests that vary ``RAKAIA_STORE``. Without this,
    the only way to clear the cache was to reach in and mutate the private
    ``_stores`` dict, which is the one place the suite reached past an interface
    by design.

    Not a way to *swap* the configured store: to drive a caller with a store of
    your own, pass one to `get_asgi_app(store=...)` or
    `django_rakaia.replay(store=...)` rather than reconfiguring a global.
    """
    with _store_lock:
        _stores.clear()
