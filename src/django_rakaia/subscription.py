"""Durable per-consumer cursors backed by the ``ConsumerCursor`` model.

Thin persistence layer over `rakaia.subscription.poll`: load a consumer's stored
watermark, poll the stream for the delta, and — after the caller has applied the
messages — commit the new watermark. Splitting poll from commit keeps delivery
**at-least-once**: a crash between applying and committing re-delivers the batch
rather than skipping it.

**One worker per ``(consumer, stream)``.** Two workers sharing a consumer name
read the same cursor and apply the same events twice. `django_consumer` holds a
lock on the cursor row for the whole pass (`locked_cursor`), so a second worker
waits for the first to finish. `commit_cursor` is also forward-only, so a worker
that somehow gets past the lock cannot move the watermark back over events that
were already applied.
"""

from __future__ import annotations

from collections.abc import Iterator
from contextlib import contextmanager

from django.db import transaction

from rakaia.offsets import after as offsets_after
from rakaia.subscription import CursorStore, Poll, poll

from .models import ConsumerCursor

#: What a cursor row holds before anything is committed. Offsets are never
#: empty, so it cannot be mistaken for a position. A row has to exist for a pass
#: to lock it, and the first pass is exactly when two workers are most likely to
#: start together.
_NO_POSITION = ""


class CursorMovedBackwards(RuntimeError):
    """Raised when a commit would put the watermark behind where it already is.

    Events at or below the committed position have been applied, and moving back
    over them applies them again, so nothing is written. Two workers under one
    consumer name is the usual cause. To start over deliberately, after a
    rebuild, delete the consumer's ``ConsumerCursor`` row.
    """


def load_cursor(
    consumer_id: str, stream_path: str, *, using: str | None = None
) -> str | None:
    """The consumer's last committed offset for `stream_path`, or None."""
    row = (
        ConsumerCursor.objects.using(using)
        .filter(consumer_id=consumer_id, stream_path=stream_path)
        .first()
    )
    if row is None or row.offset == _NO_POSITION:
        return None
    return row.offset


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
    consumer_id: str,
    stream_path: str,
    offset: str,
    *,
    using: str | None = None,
    generation: str | None = None,
) -> None:
    """Persist `offset` as the consumer's watermark for `stream_path`.

    Call this only **after** the polled messages have been applied. ``using``
    names a database alias, as it does on the stores; the default alias is the
    ambient connection. Forward-only: an `offset` behind the committed one raises
    `CursorMovedBackwards` and writes nothing; committing the same offset again is
    allowed. Pass ``generation=result.generation`` from the poll to
    save the stream's stamp with the cursor; without it a later delete and
    recreate cannot be reported as ``replaced``.
    """
    with transaction.atomic(using=using):
        row = _locked_row(consumer_id, stream_path, using=using)
        behind = (
            row.offset != _NO_POSITION
            and offset != row.offset
            and not offsets_after(offset, row.offset)
        )
        if behind:
            raise CursorMovedBackwards(
                f"consumer {consumer_id!r} on {stream_path!r} tried to commit "
                f"{offset!r}, behind its committed position {row.offset!r}. "
                "Events up to that position were already applied; another "
                "worker under the same consumer name is the usual cause."
            )
        row.offset = offset
        fields = ["offset", "updated_at"]
        if generation is not None:
            row.generation = generation
            fields.append("generation")
        row.save(update_fields=fields)


@contextmanager
def locked_cursor(
    consumer_id: str, stream_path: str, *, using: str | None = None
) -> Iterator[None]:
    """Hold a database lock on the cursor row, in one transaction, for a pass.

    A second worker for the same ``(consumer, stream)`` waits at this line until
    the first leaves the block. Everything the pass writes on this alias, the
    cursor and any outcomes alike, lands together when the block exits, or not at
    all if an exception leaves it. There is no limit on a pass: it is one
    transaction over the whole backlog, so a long first run holds the lock
    throughout and a crash loses that pass's progress. The lock is a real row lock
    only on a backend with ``SELECT ... FOR UPDATE``; on SQLite the pass's first
    write takes the database-wide write lock instead, held until the pass ends.
    """
    with transaction.atomic(using=using):
        _locked_row(consumer_id, stream_path, using=using)
        yield


def _locked_row(
    consumer_id: str, stream_path: str, *, using: str | None
) -> ConsumerCursor:
    """The cursor row, locked, created empty if it is not there yet.

    Must be called inside ``transaction.atomic(using=using)``.
    """
    ConsumerCursor.objects.using(using).get_or_create(
        consumer_id=consumer_id,
        stream_path=stream_path,
        defaults={"offset": _NO_POSITION},
    )
    return (
        ConsumerCursor.objects.using(using)
        .select_for_update()
        .get(consumer_id=consumer_id, stream_path=stream_path)
    )


def poll_consumer(store: CursorStore, consumer_id: str, stream_path: str) -> Poll:
    """Load this consumer's cursor and poll `stream_path` for the delta.

    Does **not** advance the cursor — apply ``result.messages``, then call
    `commit_cursor(consumer_id, stream_path, result.cursor,
    generation=result.generation)`. A ``rewound``
    result means the log was rebuilt beneath the cursor; rebuild derived state,
    then delete the ``ConsumerCursor`` row to start over, because `commit_cursor`
    raises `CursorMovedBackwards` for an offset behind the saved one. A ``replaced`` result means
    the stream was deleted and recreated since the cursor was saved, which is
    only detectable when the stamp was committed with the cursor.
    """
    cursor = load_cursor(consumer_id, stream_path)
    return poll(
        store,
        stream_path,
        cursor,
        generation=load_generation(consumer_id, stream_path),
    )
