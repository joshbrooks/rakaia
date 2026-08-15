"""
Django Channels signal handlers for real-time SSE broadcasting.

Replaces the custom SSEManager/Unix socket/Redis broadcasting with
Django Channels' channel layer for cross-process event distribution.
"""

from collections.abc import Sequence

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.db.models.signals import post_save
from django.dispatch import receiver

from .models import StreamEntry, StreamEvent


def _sanitize_group_name(name: str) -> str:
    """Sanitize a group name for the channel layer.

    Channel layer group names allow only ASCII alphanumerics, hyphens,
    underscores and periods, and must be under 100 characters. Anything else
    becomes a period.

    This used to replace colons only, which was enough while every stream id
    looked like `user:1:projects`. A protocol-server path is `/orders`, and the
    slash raised `TypeError` from inside `post_save` — i.e. an append over HTTP
    failed at the broadcast, after the write. Names are truncated from the
    right so the distinguishing tail of a long path survives.
    """
    cleaned = "".join(
        c if (c.isascii() and (c.isalnum() or c in "-_.")) else "." for c in name
    )
    return cleaned[-99:]


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


def _frame(stream_id: str, entry: StreamEntry) -> dict:
    """The SSE frame for one appended entry.

    Sole definition of the wire shape. It used to live inline in the `post_save`
    receiver, which made it unreachable from any write path that does not fire
    `post_save` — see `broadcast_entries`.
    """
    return {
        "type": "stream.event",
        # The group name is a lossy sanitization — distinct stream ids can share
        # a group (every disallowed character becomes ".", and long names
        # truncate). The payload carries the exact id so a consumer can filter
        # out a group-mate's events instead of cross-delivering them.
        "stream_id": stream_id,
        "event": {
            "id": entry.event.id,
            "offset": entry.offset,
            "event_type": entry.event.event_type,
            "created_at": entry.event.created_at.isoformat()
            if entry.event.created_at
            else None,
            "data": entry.event.data,
        },
    }


def broadcast_entries(stream_id: str, entries: Sequence[StreamEntry]) -> None:
    """Publish `entries` to the stream's channel group, oldest first.

    The one implementation of "an append reaches subscribers". Two callers reach
    it, because there are two ways an entry gets written:

    * the `post_save` receiver below, for entries written through the ORM —
      `@stream_model` and anything else that does not go through the store;
    * `DjangoStreamStore` directly, for its own appends.

    The store has to call it rather than lean on the signal: `append_many`
    persists with `bulk_create`, which does not fire `post_save`, so every bulk
    append was silently invisible to subscribers (issue #82). Restoring the
    signal by saving rows one at a time would undo the reason `append_many`
    exists; publishing explicitly keeps the batch write a batch write.

    A no-op when the channel layer is absent, so a framework-tier consumer that
    never installed `channels` is unaffected.
    """
    if not entries:
        return

    channel_layer = get_channel_layer()
    if channel_layer is None:
        return

    group_name = _sanitize_group_name(f"stream.{stream_id}")
    send = async_to_sync(channel_layer.group_send)
    for entry in entries:
        send(group_name, _frame(stream_id, entry))


@receiver(post_save, sender=StreamEntry)
def handle_stream_entry_created(sender, instance, created, **kwargs):  # noqa: ARG001
    """Broadcast an ORM-written stream entry to the stream's channel group.

    Covers every entry written through the ORM: `@stream_model`, and
    `DjangoStreamStore.append`, which persists one entry with `create()`.

    It does *not* cover `DjangoStreamStore.append_many`, which persists with
    `bulk_create` — that path calls `broadcast_entries` itself. So the two
    mechanisms partition the write paths rather than overlapping, and no entry is
    published twice.
    """
    # Skip fixture rows. Beyond the phantom frame, the `instance.stream` /
    # `instance.event` dereferences are separate queries that raise DoesNotExist
    # mid-`loaddata` if the parent rows are not restored yet.
    if kwargs.get("raw"):
        return
    if not created:
        return

    broadcast_entries(instance.stream.stream_id, [instance])
