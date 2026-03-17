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

from .views import (
    stream_detail,
    stream_events_api,
    stream_events_sse,
    streams_api,
    streams_index,
)

app_name = "django_rakaia"

urlpatterns = [
    # Main dashboard pages
    path("", streams_index, name="streams_index"),
    path("<str:stream_id>/", stream_detail, name="stream_detail"),
    # API endpoints
    path("api/streams/", streams_api, name="streams_api"),
    path("api/streams/<str:stream_id>/", stream_events_api, name="stream_events_api"),
    path("api/streams/<str:stream_id>/sse/", stream_events_sse, name="stream_events_sse"),
]
