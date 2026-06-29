"""URL configuration for the chat sample."""

from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    path("", include("chat.urls")),
    path("streams/", include("django_rakaia.urls", namespace="django_rakaia")),
]
