"""URL configuration for the orders app."""

from django.urls import path

from . import views

app_name = "orders"

urlpatterns = [
    path("", views.index, name="index"),
    path("info/", views.info, name="info"),
    path("live/", views.live_index, name="live"),
    path("live/data/", views.live_data, name="live_data"),
    path("live/submit/", views.live_submit, name="live_submit"),
]
