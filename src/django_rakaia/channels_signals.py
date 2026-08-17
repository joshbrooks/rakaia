"""
Django Channels signal handlers for real-time SSE broadcasting.

Replaces the custom SSEManager/Unix socket/Redis broadcasting with
Django Channels' channel layer for cross-process event distribution.
"""

import base64
import json
from collections.abc import Sequence
from typing import Any

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.db.models.signals import post_save
from django.dispatch import receiver

from .event_message import message_of
from .models import StreamEntry


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
        "event": frame_event(entry),
    }


def frame_event(entry: StreamEntry) -> dict[str, Any]:
    """The `event` object inside a frame — shared with the HTTP SSE view.

    `channels_views` streams the same events over a plain HTTP response rather
    than the channel layer. It built its own copy of this dict and inherited the
    same two defects (#153), so both now derive from one definition.
    """
    message = message_of(entry)
    data, encoding = _json_safe(message.data)
    event: dict[str, Any] = {
        "id": entry.event.id,
        "offset": entry.offset,
        # Derived from `message_of`, not read off the column. Reading
        # `event_type` directly published the raw `"append"` sentinel where
        # `read()` reports `""`, and reading `data` directly published a
        # base64-stored payload still encoded (#153).
        "event_type": message.label,
        "created_at": entry.event.created_at.isoformat()
        if entry.event.created_at
        else None,
        "data": data,
    }
    if encoding is not None:
        # Only present when `data` is not the JSON value itself, so a JSON
        # payload — the common case — keeps exactly the frame it always had and
        # no existing subscriber sees a new key. When it *is* present, it is
        # what lets a subscriber recover the same bytes `read()` returns, which
        # it previously could not (#153).
        event["payload_encoding"] = encoding
    return event


def _json_safe(payload: bytes) -> tuple[Any, str | None]:
    """The payload as `(value, encoding)`, in a form the channel layer can send.

    The frame is JSON on the wire, so raw bytes cannot ride in it. That is the
    root of #153: the old frame published the stored column and left the
    subscriber no way to know it was looking at base64. Re-encoding silently
    would repeat the bug, so the encoding travels with the value.
    """
    try:
        return json.loads(payload), None
    except (json.JSONDecodeError, UnicodeDecodeError):
        pass
    try:
        return payload.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        return base64.b64encode(payload).decode("ascii"), "base64"


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
