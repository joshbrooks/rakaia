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


def load_cursor(consumer_id: str, stream_path: str) -> str | None:
    """The consumer's last committed offset for `stream_path`, or None."""
    row = ConsumerCursor.objects.filter(
        consumer_id=consumer_id, stream_path=stream_path
    ).first()
    return row.offset if row else None


def commit_cursor(consumer_id: str, stream_path: str, offset: str) -> None:
    """Persist `offset` as the consumer's watermark for `stream_path`.

    Call this only **after** the polled messages have been applied.
    """
    ConsumerCursor.objects.update_or_create(
        consumer_id=consumer_id,
        stream_path=stream_path,
        defaults={"offset": offset},
    )


def poll_consumer(store: CursorStore, consumer_id: str, stream_path: str) -> Poll:
    """Load this consumer's cursor and poll `stream_path` for the delta.

    Does **not** advance the cursor — apply ``result.messages``, then call
    `commit_cursor(consumer_id, stream_path, result.cursor)`. A ``rewound``
    result means the log was rebuilt beneath the cursor; reset derived state
    before applying, exactly as with the core `poll`.
    """
    cursor = load_cursor(consumer_id, stream_path)
    return poll(store, stream_path, cursor)
