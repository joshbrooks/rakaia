"""Startup checks for the Rakaia settings a deployment can get quietly wrong.

`django_rakaia.store.get_store` refuses an unknown ``RAKAIA_STORE`` — but it
only runs on the first append, which in a web process is during a request, and
in a worker may be hours in. These checks move the same verdict to
``manage.py check``, so a misconfigured deployment fails before it serves
anything.

The one that matters is `rakaia.E001`: the setting is free-form text, so a
misspelt backend used to select the in-memory store and lose every append on
restart. `rakaia.E002` is the same idea for the file-backed store, which cannot
be built without a root directory and must not be handed a guessed one.
`rakaia.W001` is softer — a correctly-spelt ``"memory"`` is legitimate for
development but is worth flagging if `DEBUG` is off, because a production
deployment almost never means it. `rakaia.W002` is the same kind of warning
about a real capability gap: the durable store broadcasts every append over
channels as it writes it, and the file-backed store cannot, so a deployment
that switches to it and has live consumers would find them quietly going
silent.
"""

from __future__ import annotations

from typing import Any

from django.conf import settings

# `Warning` shadows the builtin, but it is Django's own check class and this
# module's whole job is returning them; aliasing it would obscure that.
from django.core.checks import Error, Warning, register  # noqa: A004

from .store import BACKENDS, DEFAULT_BACKEND


def _channels_is_installed() -> bool:
    """Whether this deployment has channels, and so might expect a broadcast."""
    from django.apps import apps

    return apps.is_installed("channels")


@register()
def check_store_backend(app_configs: Any, **kwargs: Any) -> list[Any]:  # noqa: ARG001
    """Verify ``RAKAIA_STORE`` names a real backend, and flag in-memory in prod."""
    backend = getattr(settings, "RAKAIA_STORE", DEFAULT_BACKEND)

    if backend not in BACKENDS:
        return [
            Error(
                f"RAKAIA_STORE={backend!r} is not a known store backend.",
                hint=(
                    f"Set it to one of {', '.join(repr(b) for b in BACKENDS)}. "
                    "Until this is fixed every append raises "
                    "ImproperlyConfigured."
                ),
                id="rakaia.E001",
            )
        ]

    if backend == "jsonl" and not getattr(settings, "RAKAIA_JSONL_ROOT", None):
        return [
            Error(
                "RAKAIA_STORE is 'jsonl' but RAKAIA_JSONL_ROOT is not set.",
                hint=(
                    "Set RAKAIA_JSONL_ROOT to the directory the stream logs "
                    "should live in. There is no default: until this is fixed "
                    "every append raises ImproperlyConfigured."
                ),
                id="rakaia.E002",
            )
        ]

    if backend == "jsonl" and _channels_is_installed():
        return [
            Warning(
                "RAKAIA_STORE is 'jsonl' and channels is installed, but the "
                "file-backed store does not publish appends to live "
                "subscribers.",
                hint=(
                    "The durable store broadcasts each append over channels as "
                    "it writes it; the file-backed store cannot, because it "
                    "lives in the framework-independent package and has no way "
                    "to reach Django. Live consumers must poll the stream "
                    "instead — the protocol server's long-poll and SSE reads "
                    "work normally. Silence this with SILENCED_SYSTEM_CHECKS "
                    "if nothing depends on the broadcast."
                ),
                id="rakaia.W002",
            )
        ]

    if backend == "memory" and not settings.DEBUG:
        return [
            Warning(
                "RAKAIA_STORE is 'memory' with DEBUG off — the event log is "
                "held in process memory and is lost on every restart.",
                hint=(
                    "Set RAKAIA_STORE = 'durable' and run "
                    "`manage.py migrate django_rakaia` to persist the log. "
                    "Silence this with SILENCED_SYSTEM_CHECKS if in-memory is "
                    "deliberate here."
                ),
                id="rakaia.W001",
            )
        ]

    return []
