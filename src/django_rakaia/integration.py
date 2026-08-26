from typing import Any

from rakaia import ServerOptions, create_app

from .store import get_store


def get_asgi_app(
    options: ServerOptions | None = None, store: Any | None = None
) -> object:
    """
    Get the Rakaia ASGI application configured with the store `RAKAIA_STORE` names.

    Pass `store` to supply one directly and skip the setting entirely. This
    exists so the function can be *driven*: resolving the store internally left
    the only way to exercise this entry point being to reconfigure a
    process-wide cache, which is why it previously had no test coverage at all.
    `django_rakaia.replay` takes the same argument for the same reason.

    A raw ASGI app, not a Django view: mount it in `asgi.py` by dispatching on
    the path and stripping the prefix — `URLRouter`/`path("streams/", …)` does
    not strip it, so every request would reach the handler with the prefix
    still on the stream id (see docs/django-integration.md, "Protocol HTTP API").

    Example:
        # asgi.py
        from django.core.asgi import get_asgi_application
        from django_rakaia.integration import get_asgi_app

        django_app = get_asgi_application()
        protocol_app = get_asgi_app()

        async def application(scope, receive, send):
            if scope["type"] == "http" and scope["path"].startswith("/protocol/"):
                scope = {**scope, "path": scope["path"][len("/protocol") :]}
                return await protocol_app(scope, receive, send)
            return await django_app(scope, receive, send)
    """
    return create_app(
        store=store if store is not None else get_store(), options=options
    )
