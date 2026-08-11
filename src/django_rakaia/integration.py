from rakaia import ServerOptions, create_app

from .store import get_store


def get_asgi_app(options: ServerOptions | None = None) -> object:
    """
    Get the Rakaia ASGI application configured with the store `RAKAIA_STORE` names.

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
    return create_app(store=get_store(), options=options)
