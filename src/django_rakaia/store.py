import threading
from typing import Any

from django.conf import settings
from django.core.exceptions import ImproperlyConfigured

from rakaia import StreamStore

#: The backends ``RAKAIA_STORE`` accepts, mapped to what they build. Anything
#: else is a configuration error, not a fallback — see `_build_store`.
BACKENDS = ("memory", "durable", "jsonl")

DEFAULT_BACKEND = "memory"

# One cached store instance per backend name. Keeping the in-memory store a
# process-wide singleton is essential — it holds all stream state in memory —
# so we never rebuild or replace it. The durable and JSONL stores are stateless
# (state lives in the database, or in the directory) but are cached for
# symmetry.
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
    if backend == "jsonl":
        from rakaia.jsonl_store import JsonlStreamStore

        root = getattr(settings, "RAKAIA_JSONL_ROOT", None)
        if not root:
            # Refused rather than defaulted, for the same reason a misspelt
            # backend is: a guessed root — a temp directory, the working
            # directory — would accept every append and put the log somewhere
            # the next deployment does not look.
            raise ImproperlyConfigured(
                "RAKAIA_STORE='jsonl' needs RAKAIA_JSONL_ROOT set to the "
                "directory the logs live in. There is no default: a guessed "
                "location would silently hold a log nothing else can find."
            )
        return JsonlStreamStore(
            root,
            segment_size=getattr(settings, "RAKAIA_JSONL_SEGMENT_SIZE", 10_000),
            fsync=getattr(settings, "RAKAIA_JSONL_FSYNC", True),
        )
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
    the in-memory `StreamStore`, ``"durable"`` for the DB-backed
    `DjangoStreamStore`, or ``"jsonl"`` for the file-backed `JsonlStreamStore`
    (which also needs ``RAKAIA_JSONL_ROOT``). Any other value raises
    `ImproperlyConfigured`. Thread-safe lazy initialization, one instance per
    backend.
    """
    backend = getattr(settings, "RAKAIA_STORE", DEFAULT_BACKEND)
    with _store_lock:
        if backend not in _stores:
            # Built before the assignment, so a refused backend leaves no entry
            # behind: correcting the setting works without restarting.
            _stores[backend] = _build_store(backend)
    return _stores[backend]
