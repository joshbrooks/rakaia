from django.conf import settings
from django.utils.module_loading import import_string

from rakaia import ServerOptions, create_app
from rakaia.protocol_server import (
    DEFAULT_MAX_BODY_BYTES,
    DEFAULT_READ_PAGE_SIZE,
    DEFAULT_SSE_MAX_SECONDS,
)
from rakaia.protocols import StreamServerStore

from .store import get_store


def get_asgi_app(
    options: ServerOptions | None = None, store: StreamServerStore | None = None
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
    if options is None:
        # The server options a Django deployment sets from its settings. Reading
        # a page returns `RAKAIA_READ_PAGE_SIZE` messages at most (#289, `None`
        # turns paging off); a body over `RAKAIA_MAX_BODY_BYTES` is a 413;
        # `RAKAIA_ALLOWED_ORIGINS` is the CORS list; `RAKAIA_AUTHORIZE` is the
        # access hook, a callable or its dotted path; and an SSE connection is
        # ended after `RAKAIA_SSE_MAX_SECONDS` so clients reconnect.
        authorize = getattr(settings, "RAKAIA_AUTHORIZE", None)
        if isinstance(authorize, str):
            authorize = import_string(authorize)
        options = ServerOptions(
            read_page_size=getattr(
                settings, "RAKAIA_READ_PAGE_SIZE", DEFAULT_READ_PAGE_SIZE
            ),
            max_body_bytes=getattr(
                settings, "RAKAIA_MAX_BODY_BYTES", DEFAULT_MAX_BODY_BYTES
            ),
            allowed_origins=tuple(getattr(settings, "RAKAIA_ALLOWED_ORIGINS", ("*",))),
            authorize=authorize,
            sse_max_seconds=getattr(
                settings, "RAKAIA_SSE_MAX_SECONDS", DEFAULT_SSE_MAX_SECONDS
            ),
        )
    return create_app(
        store=store if store is not None else get_store(), options=options
    )
