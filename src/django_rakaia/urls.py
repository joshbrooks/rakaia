"""
URL configuration for Django Rakaia Data Streams dashboard.

Include these URLs in your project's main urls.py:

    from django.urls import path, include

    urlpatterns = [
        # ...
        path("streams/", include("django_rakaia.urls")),
    ]
"""

from django.urls import path

from .channels_views import (
    stream_events_sse,
    translation_activity_sse_html,
    translation_streams_sse,
)
from .views import (
    stream_detail,
    stream_events_api,
    streams_api,
    streams_index,
    translation_create_htmx,
    translation_create_update_api,
    translations_api,
    translations_index,
    translations_table_htmx,
)

app_name = "django_rakaia"

urlpatterns = [
    # Main dashboard pages
    path("", streams_index, name="streams_index"),
    # Translation management
    path("translations/", translations_index, name="translations_index"),
    path("<str:stream_id>/", stream_detail, name="stream_detail"),
    # API endpoints
    path("api/streams/", streams_api, name="streams_api"),
    path("api/streams/<str:stream_id>/", stream_events_api, name="stream_events_api"),
    # SSE endpoints (Django Channels)
    path(
        "api/streams/<str:stream_id>/sse/", stream_events_sse, name="stream_events_sse"
    ),
    path(
        "api/translations/sse/", translation_streams_sse, name="translation_streams_sse"
    ),
    # Translation API endpoints (JSON, kept for non-browser consumers)
    path("api/translations/", translations_api, name="translations_api"),
    path(
        "api/translations/create/",
        translation_create_update_api,
        name="translation_create_update_api",
    ),
    # HTMX partials for the translations UI
    path(
        "api/translations/htmx/table/",
        translations_table_htmx,
        name="translations_table_htmx",
    ),
    path(
        "api/translations/htmx/create/",
        translation_create_htmx,
        name="translation_create_htmx",
    ),
    path(
        "api/translations/htmx/sse/",
        translation_activity_sse_html,
        name="translation_activity_sse_html",
    ),
]
