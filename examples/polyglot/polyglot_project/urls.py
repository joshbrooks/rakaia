"""URL configuration for the polyglot sample."""

from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", include("polyglot.urls")),
    # `django_rakaia.urls` is deliberately not included. Every route in it —
    # the dashboard, the read API, the SSE endpoint — reads `Stream` and
    # `StreamEntry` rows, and this app's log is a folder of files that no row
    # describes. Mounted, it would serve a permanently empty dashboard next to a
    # page that is visibly streaming. The protocol server on /protocol/ (see
    # asgi.py) is the read side here.
]
