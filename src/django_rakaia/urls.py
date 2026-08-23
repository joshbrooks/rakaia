"""
URL configuration for Django Rakaia Data Streams dashboard.

Include these URLs in your project's main urls.py:

    from django.urls import path, include

    urlpatterns = [
        # ...
        path("streams/", include("django_rakaia.urls")),
    ]

Two things about this file are load-bearing and easy to undo by tidying it.

**Stream ids use the `path` converter, not `str`.** A stream name is the
server's business — the protocol says so, and offers ``/v1/stream/{path}`` as an
example scheme — so names contain slashes in practice: rakaia's own
``SCRATCH_PATH`` is ``_scratch/fold``, and the first production consumer names
every one of its stream families that way. ``str`` refuses a slash, so those
streams could be created, appended to and read back, but never addressed here
(#231).

**The route order is therefore load-bearing.** ``path`` is greedy where ``str``
is not, so the catch-all dashboard route has to come *last*: listed first, as it
used to be, it claims ``api/streams/orders/`` as a stream named
``api/streams/orders`` and the read API quietly starts returning HTML. Tests that
reverse a URL name agree with that mistake, which is why
``tests/test_django_rakaia/test_urls.py`` resolves real paths instead.

**The live-update route is conditional.** Importing it at module scope made
`channels` a hard dependency of the polling API, because Django imports the
URLconf before any setting is consulted (#230). It now loads through
`sse_gate.sse_import`, the same gate `apps.py` uses for its signal handlers.
"""

from django.urls import path

from .sse_gate import sse_import
from .views import stream_detail, stream_events_api, streams_api, streams_index

app_name = "django_rakaia"

urlpatterns = [
    # Dashboard index and the API, before the catch-all below.
    path("", streams_index, name="streams_index"),
    path("api/streams/", streams_api, name="streams_api"),
]

# SSE endpoint (Django Channels), only where this deployment wants it. Ahead of
# the read API's own route because both spell the same URL for a stream named
# `<name>/sse`: the endpoint wins, since the alternative makes SSE unreachable
# for every stream rather than making one unusual name unreachable here.
_stream_events_sse = sse_import("django_rakaia.channels_views", "stream_events_sse")
if _stream_events_sse is not None:
    urlpatterns.append(
        path(
            "api/streams/<path:stream_id>/sse/",
            _stream_events_sse,
            name="stream_events_sse",
        )
    )

urlpatterns += [
    path("api/streams/<path:stream_id>/", stream_events_api, name="stream_events_api"),
    # Last: `path` matches slashes, so this claims anything left over.
    path("<path:stream_id>/", stream_detail, name="stream_detail"),
]
