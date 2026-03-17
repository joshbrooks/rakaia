from rakaia import ServerOptions, create_app

from .store import get_store


def get_asgi_app(options: ServerOptions | None = None) -> object:
    """
    Get the Rakaia ASGI application configured with the Django global StreamStore.

    Example:
        from django_rakaia.integration import get_asgi_app

        # in asgi.py
        application = ProtocolTypeRouter({
            "http": URLRouter([
                path("streams/", get_asgi_app()),
                # ... other routes
            ]),
        })
    """
    return create_app(store=get_store(), options=options)
