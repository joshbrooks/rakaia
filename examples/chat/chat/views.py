"""Minimal chat views.

The list and detail pages are sync HTML; message submission is a sync POST.
The real-time message feed in the browser comes from the SSE endpoint
exposed by django_rakaia at /streams/api/streams/<stream_id>/sse/.
"""

from __future__ import annotations

from django.contrib.auth import get_user_model
from django.http import HttpRequest, HttpResponse, HttpResponseRedirect
from django.shortcuts import get_object_or_404, render
from django.urls import reverse
from django.views.decorators.http import require_http_methods

from .models import ChatRoom, Message

User = get_user_model()


def _get_or_create_demo_user() -> User:
    """Sample apps shouldn't require authentication setup; use a demo user."""
    user, _ = User.objects.get_or_create(
        username="demo", defaults={"email": "demo@example.com"}
    )
    return user


def room_list(request: HttpRequest) -> HttpResponse:
    rooms = ChatRoom.objects.all().order_by("name")
    if request.method == "POST":
        name = request.POST.get("name", "").strip()
        if name:
            ChatRoom.objects.get_or_create(name=name)
        return HttpResponseRedirect(reverse("chat:room_list"))
    return render(request, "chat/room_list.html", {"rooms": rooms})


def room_detail(request: HttpRequest, room_id: int) -> HttpResponse:
    room = get_object_or_404(ChatRoom, pk=room_id)
    messages = room.messages.select_related("author").all()
    stream_id = f"room:{room_id}:messages"
    return render(
        request,
        "chat/room.html",
        {
            "room": room,
            "messages": messages,
            "stream_id": stream_id,
        },
    )


def _ajax_or_redirect(request: HttpRequest, room_id: int) -> HttpResponse:
    """Return 204 for fetch callers, otherwise redirect back to the room."""
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return HttpResponse(status=204)
    return HttpResponseRedirect(reverse("chat:room_detail", args=[room_id]))


@require_http_methods(["POST"])
def post_message(request: HttpRequest, room_id: int) -> HttpResponse:
    room = get_object_or_404(ChatRoom, pk=room_id)
    body = request.POST.get("body", "").strip()
    if body:
        author = _get_or_create_demo_user()
        Message.objects.create(room=room, author=author, body=body)
    return _ajax_or_redirect(request, room_id)


@require_http_methods(["POST"])
def update_message(
    request: HttpRequest, room_id: int, message_id: int
) -> HttpResponse:
    """Edit a message body. Triggers an `update` StreamEvent via @stream_model."""
    msg = get_object_or_404(Message, pk=message_id, room_id=room_id)
    body = (request.POST.get("body") or "").strip()
    if body:
        msg.body = body
        msg.save()  # @stream_model post_save → SSE update event
    return _ajax_or_redirect(request, room_id)


@require_http_methods(["POST"])
def delete_message(
    request: HttpRequest, room_id: int, message_id: int
) -> HttpResponse:
    """Delete a message. Triggers a `delete` StreamEvent via @stream_model."""
    msg = get_object_or_404(Message, pk=message_id, room_id=room_id)
    msg.delete()  # @stream_model post_delete → SSE delete event
    return _ajax_or_redirect(request, room_id)
