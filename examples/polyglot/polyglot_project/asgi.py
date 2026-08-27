"""ASGI entry point for the polyglot sample: Django, plus the protocol server.

Two applications share one port. Anything under `/protocol/` is handled by
rakaia's own ASGI app — the framework-independent Durable Streams server — and
everything else is Django.

That mount is what makes live updates work over the file-backed store. Django's
own SSE endpoint (`django_rakaia.channels_views`) replays history from
`StreamEntry` rows and waits on the channel layer, and neither exists when the
log is a folder of files. The protocol server reads through the store interface
instead and blocks in `store.wait_for_messages(...)`, which the file-backed
store implements by watching the directory. So the page gets real SSE, over a
log no database has seen, and it works across processes — the stress command's
`--direct` mode now reaches open browsers, which it could not under an
in-memory channel layer.

The prefix is stripped by hand. `URLRouter` and `path()` do not strip it, so the
handler would receive `/protocol/translations/tet` and look for a stream of that
name; the streams are created as `/translations/tet`.
"""

import os

from django.core.asgi import get_asgi_application

os.environ.setdefault("DJANGO_SETTINGS_MODULE", "polyglot_project.settings")

django_app = get_asgi_application()

# Imported after `get_asgi_application()`: both of these read settings.
from django.conf import settings  # noqa: E402

from django_rakaia.integration import get_asgi_app  # noqa: E402

PROTOCOL_PREFIX = settings.POLYGLOT_PROTOCOL_PREFIX
protocol_app = get_asgi_app()


async def application(scope, receive, send):
    if scope["type"] == "http" and scope["path"].startswith(PROTOCOL_PREFIX + "/"):
        scope = {**scope, "path": scope["path"][len(PROTOCOL_PREFIX) :]}
        return await protocol_app(scope, receive, send)
    return await django_app(scope, receive, send)
