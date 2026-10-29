"""Durable per-consumer cursors backed by the ``ConsumerCursor`` model.

Thin persistence layer over `rakaia.subscription.poll`: load a consumer's stored
watermark, poll the stream for the delta, and — after the caller has applied the
messages — commit the new watermark. Splitting poll from commit keeps delivery
**at-least-once**: a crash between applying and committing re-delivers the batch
rather than skipping it.
"""

from __future__ import annotations

from rakaia.subscription import CursorStore, Poll, poll

from .models import ConsumerCursor


def load_cursor(
    consumer_id: str, stream_path: str, *, using: str | None = None
) -> str | None:
    """The consumer's last committed offset for `stream_path`, or None."""
    row = (
        ConsumerCursor.objects.using(using)
        .filter(consumer_id=consumer_id, stream_path=stream_path)
        .first()
    )
    return row.offset if row else None


def load_generation(
    consumer_id: str, stream_path: str, *, using: str | None = None
) -> str | None:
    """The stream stamp saved with the consumer's cursor, or None."""
    row = (
        ConsumerCursor.objects.using(using)
        .filter(consumer_id=consumer_id, stream_path=stream_path)
        .first()
    )
    return row.generation if row else None


def commit_generation(
    consumer_id: str, stream_path: str, generation: str, *, using: str | None = None
) -> None:
    """Save the stream's stamp on the consumer's existing cursor row."""
    ConsumerCursor.objects.using(using).filter(
        consumer_id=consumer_id, stream_path=stream_path
    ).update(generation=generation)


def commit_cursor(
    consumer_id: str, stream_path: str, offset: str, *, using: str | None = None
) -> None:
    """Persist `offset` as the consumer's watermark for `stream_path`.

    Call this only **after** the polled messages have been applied. ``using``
    names a database alias, as it does on the stores; the default alias is the
    ambient connection.
    """
    ConsumerCursor.objects.using(using).update_or_create(
        consumer_id=consumer_id,
        stream_path=stream_path,
        defaults={"offset": offset},
    )


def poll_consumer(store: CursorStore, consumer_id: str, stream_path: str) -> Poll:
    """Load this consumer's cursor and poll `stream_path` for the delta.

    Does **not** advance the cursor — apply ``result.messages``, then call
    `commit_cursor(consumer_id, stream_path, result.cursor)`. A ``rewound``
    result means the log was rebuilt beneath the cursor; reset derived state
    before applying, exactly as with the core `poll`. A ``replaced`` result means
    the stream was deleted and recreated since the cursor was saved.
    """
    cursor = load_cursor(consumer_id, stream_path)
    return poll(
        store,
        stream_path,
        cursor,
        generation=load_generation(consumer_id, stream_path),
    )
