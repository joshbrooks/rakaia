from django.urls import path

from . import views

app_name = "polyglot"

urlpatterns = [
    path("", views.landing, name="landing"),
    path(
        "translations/<int:pk>/update/",
        views.update_translation,
        name="update_translation",
    ),
]
