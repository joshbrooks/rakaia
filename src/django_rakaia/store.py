import threading
from typing import Any

from django.conf import settings

from rakaia import StreamStore

# One cached store instance per backend name. Keeping the in-memory store a
# process-wide singleton is essential — it holds all stream state in memory —
# so we never rebuild or replace it. The durable store is stateless (state
# lives in the DB) but is cached for symmetry.
_stores: dict[str, Any] = {}
_store_lock = threading.Lock()


def _build_store(backend: str) -> Any:
    if backend == "durable":
        from .django_store import DjangoStreamStore

        return DjangoStreamStore()
    return StreamStore()


def get_store() -> Any:
    """
    Get the configured stream store.

    Selected by the ``RAKAIA_STORE`` Django setting: ``"memory"`` (default) for
    the in-memory `StreamStore`, or ``"durable"`` for the DB-backed
    `DjangoStreamStore`. Thread-safe lazy initialization, one instance per
    backend.
    """
    backend = getattr(settings, "RAKAIA_STORE", "memory")
    with _store_lock:
        if backend not in _stores:
            _stores[backend] = _build_store(backend)
    return _stores[backend]
