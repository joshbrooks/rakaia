from django.urls import path

from . import views

app_name = "chat"

urlpatterns = [
    path("", views.room_list, name="room_list"),
    path("rooms/<int:room_id>/", views.room_detail, name="room_detail"),
    path("rooms/<int:room_id>/post/", views.post_message, name="post_message"),
    path(
        "rooms/<int:room_id>/messages/<int:message_id>/update/",
        views.update_message,
        name="update_message",
    ),
    path(
        "rooms/<int:room_id>/messages/<int:message_id>/delete/",
        views.delete_message,
        name="delete_message",
    ),
]
