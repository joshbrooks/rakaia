"""Chat models showing how to use @stream_model with multi-stream events.

A `Message` belongs to a `ChatRoom` and is also authored by a `User`. Saving a
message emits a single StreamEvent that lands in two streams:

  * room:{room_id}:messages    — anyone watching the room
  * user:{author_id}:activity  — anyone watching the author

This is the canonical multi-stream pattern from the Durable Streams protocol.
"""

from __future__ import annotations

from dataclasses import dataclass

from django.contrib.auth import get_user_model
from django.db import models

from django_rakaia import stream_model

User = get_user_model()


class ChatRoom(models.Model):
    name = models.CharField(max_length=100, unique=True)
    created_at = models.DateTimeField(auto_now_add=True)

    def __str__(self) -> str:
        return self.name


@dataclass
class MessagePayload:
    """Serialized into the StreamEvent.data JSON column."""

    id: int
    room_id: int
    room_name: str
    author_id: int
    author_username: str
    body: str


@stream_model(
    stream_paths=lambda obj: [
        f"room:{obj.room_id}:messages",
        f"user:{obj.author_id}:activity",
    ],
    to_dataclass=lambda obj: MessagePayload(
        id=obj.id,
        room_id=obj.room_id,
        room_name=obj.room.name,
        author_id=obj.author_id,
        author_username=obj.author.username,
        body=obj.body,
    ),
)
class Message(models.Model):
    room = models.ForeignKey(
        ChatRoom, on_delete=models.CASCADE, related_name="messages"
    )
    author = models.ForeignKey(User, on_delete=models.CASCADE, related_name="messages")
    body = models.TextField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        ordering = ["created_at"]

    def __str__(self) -> str:
        return f"{self.author}: {self.body[:30]}"
