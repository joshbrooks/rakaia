"""
Test URL configuration for django_rakaia tests.
"""

from django.contrib import admin
from django.urls import include, path

urlpatterns = [
    path("admin/", admin.site.urls),
    path("streams/", include("django_rakaia.urls", namespace="django_rakaia")),
]
