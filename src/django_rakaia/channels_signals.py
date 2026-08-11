"""
Django Channels signal handlers for real-time SSE broadcasting.

Replaces the custom SSEManager/Unix socket/Redis broadcasting with
Django Channels' channel layer for cross-process event distribution.
"""

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.db.models.signals import post_save
from django.dispatch import receiver

from .models import StreamEntry, StreamEvent


def _sanitize_group_name(name: str) -> str:
    """Sanitize a group name for the channel layer.

    Channel layer group names only allow ASCII alphanumerics,
    hyphens, underscores, and periods. Replace colons with periods.
    """
    return name.replace(":", ".")


@receiver(post_save, sender=StreamEvent)
def handle_stream_event_created(sender, instance, created, **kwargs):  # noqa: ARG001
    """Broadcast translation events to the 'translations' channel group."""
    # Fixture loads (`loaddata`, `serialized_rollback`) are replayed history,
    # not live facts — broadcasting them sends phantom frames to connected
    # clients. See issue #80.
    if kwargs.get("raw"):
        return
    # `data` is any JSON value, not necessarily an object: a protocol append can
    # carry a list, a string or a number, and the durable store holds a
    # non-JSON payload as a string. Only an object can be a translation event,
    # and anything else used to raise `AttributeError` out of `post_save` — i.e.
    # fail the append, after the write had landed.
    if not created or not isinstance(instance.data, dict):
        return
    if not instance.data.get("translatable_id"):
        return

    channel_layer = get_channel_layer()
    if channel_layer is None:
        return

    message = {
        "type": "translation.event",
        "stream": {
            "id": instance.id,
            "user": instance.data.get("username", "Unknown"),
            "langcode": instance.data.get("langcode", ""),
            "url": instance.data.get("url", ""),
            "action": instance.event_type,
            "translatable": {
                "id": instance.data.get("translatable_id"),
                "msgid": instance.data.get("msgid", ""),
                "msgstr": instance.data.get("msgstr", ""),
                "langcode": instance.data.get("langcode", ""),
            },
            "created_at": instance.created_at.isoformat()
            if instance.created_at
            else None,
        },
    }

    async_to_sync(channel_layer.group_send)("translations", message)


@receiver(post_save, sender=StreamEntry)
def handle_stream_entry_created(sender, instance, created, **kwargs):  # noqa: ARG001
    """Broadcast stream events to the stream's channel group."""
    # As above: skip fixture rows. Beyond the phantom frame, the `instance.stream`
    # / `instance.event` dereferences below are separate queries that raise
    # DoesNotExist mid-`loaddata` if the parent rows are not restored yet.
    if kwargs.get("raw"):
        return
    if not created:
        return

    channel_layer = get_channel_layer()
    if channel_layer is None:
        return

    stream_id = instance.stream.stream_id
    message = {
        "type": "stream.event",
        "event": {
            "id": instance.event.id,
            "offset": instance.offset,
            "event_type": instance.event.event_type,
            "created_at": instance.event.created_at.isoformat()
            if instance.event.created_at
            else None,
            "data": instance.event.data,
        },
    }

    group_name = _sanitize_group_name(f"stream.{stream_id}")
    async_to_sync(channel_layer.group_send)(group_name, message)
