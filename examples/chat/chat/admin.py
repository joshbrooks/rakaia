"""Django admin registration for the chat sample.

Admin saves of `Message` go through the same `@stream_model` decorator as
saves from the views, so creating a message in `/admin/chat/message/add/`
emits the same multi-stream event as posting through the chat UI.
"""

from django.contrib import admin

from .models import ChatRoom, Message


@admin.register(ChatRoom)
class ChatRoomAdmin(admin.ModelAdmin):
    list_display = ("name", "message_count", "created_at")
    search_fields = ("name",)
    ordering = ("-created_at",)
    readonly_fields = ("created_at",)

    def message_count(self, obj: ChatRoom) -> int:
        return obj.messages.count()

    message_count.short_description = "Messages"  # type: ignore[attr-defined]


@admin.register(Message)
class MessageAdmin(admin.ModelAdmin):
    list_display = ("id", "room", "author", "body_preview", "created_at")
    list_filter = ("room", "author", "created_at")
    search_fields = ("body", "author__username", "room__name")
    autocomplete_fields = ("room", "author")
    ordering = ("-created_at",)
    readonly_fields = ("created_at",)

    def body_preview(self, obj: Message) -> str:
        return obj.body if len(obj.body) <= 60 else obj.body[:57] + "…"

    body_preview.short_description = "Body"  # type: ignore[attr-defined]
