"""
Django Channels signal handlers for real-time SSE broadcasting.

Replaces the custom SSEManager/Unix socket/Redis broadcasting with
Django Channels' channel layer for cross-process event distribution.
"""

from collections.abc import Sequence
from typing import Any

from asgiref.sync import async_to_sync
from channels.layers import get_channel_layer
from django.db import transaction
from django.db.models.signals import post_save
from django.dispatch import receiver

from .event_message import event_view_of_entry
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

    The frame is JSON on the wire, so the payload rides as the stored
    `data`/`payload_encoding` pair rather than the decoded bytes `message_of`
    produces — see `payload_fields` for why passing the pair through is what
    makes the subscriber's inverse exact.

    One line, because the assembly is `event_view` now and shared with both HTTP
    surfaces. This function used to build the dict itself and published the
    *event's* `created_at` where they published the *entry's* — the same event
    with two timestamps, and the entry's is the one `read()` reports (#185).
    """
    return event_view_of_entry(entry, event_id=True, offset=True)


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
    # Frame now, publish after the write is permanent.
    #
    # Both publish paths run inside `transaction.atomic()`, so this used to
    # announce events that a later rollback erased — a subscriber was told about
    # an append that never happened, and had no way to find out (#157). The log
    # is the source of truth; if the row is not in it, nothing downstream should
    # believe it ever was.
    #
    # Framing happens here rather than in the callback because `_frame` reads the
    # entry's related rows, and deferring that would issue those queries after
    # the transaction closed — or fail outright once the objects are stale.
    #
    # Outside a transaction Django runs the callback immediately, so an append in
    # autocommit still publishes synchronously.
    #
    # The alias is taken from the entries, the way `write_enveloped_event` takes
    # it from the streams. An entry written to another database commits on *that*
    # connection, so queuing against the default one would be queuing on a
    # connection that is not in a transaction at all — Django then runs the
    # callback immediately and publishes a write that has not happened yet, which
    # is the very defect this defers to avoid (#157 on the path #159 opened up).
    using = entries[0]._state.db
    frames = [_frame(stream_id, entry) for entry in entries]

    def _send() -> None:
        send = async_to_sync(channel_layer.group_send)
        for frame in frames:
            send(group_name, frame)

    transaction.on_commit(_send, using=using)


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
