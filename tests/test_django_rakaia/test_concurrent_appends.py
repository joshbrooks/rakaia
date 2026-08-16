"""Two connections racing to append to one stream.

Everything else in this suite tests the durable store from a single
connection, which cannot observe the property offset allocation actually
promises: that two writers hitting the same stream at the same moment get
*different* offsets. That promise rests on the ``select_for_update()`` in
``Stream.get_next_offset_block`` — and on SQLite that call compiles to nothing
at all, because Django emits ``FOR UPDATE`` only when
``connection.features.has_select_for_update`` is true and the SQLite backend
leaves it false.

So these tests are skipped on SQLite. Not because they are slow or awkward
there, but because SQLite genuinely cannot answer the question: it has no row
locks, and it serialises writers with a whole-database write lock that hides
the defect behind a lock-timeout error instead of surfacing it as a duplicate
offset. Run them with ``RAKAIA_TEST_DB=postgres`` (the ``test-postgres`` CI
job) to get a real answer.

They also require ``django_db(transaction=True)``. Under a plain ``django_db``
test pytest-django wraps the whole test in one transaction on one connection
and rolls it back, so a second connection could never see the rows the first
one wrote, and the race would not exist to begin with.
"""

from __future__ import annotations

import threading
import time
from typing import Any

import pytest
from django.db import connection, connections, transaction

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import Stream, StreamEntry

requires_row_locks = pytest.mark.skipif(
    not connection.features.has_select_for_update,
    reason=(
        "backend has no row locks (has_select_for_update is False), so "
        "select_for_update() compiles to a plain SELECT and the race cannot "
        "be observed -- run with RAKAIA_TEST_DB=postgres"
    ),
)

PATH = "race"


def _run(*targets: Any) -> None:
    """Run each callable on its own thread and re-raise the first failure.

    Each thread gets its own database connection, which is the whole point;
    each also has to close it, or the test database cannot be torn down.
    """
    errors: list[BaseException] = []

    def wrap(fn: Any) -> Any:
        def inner() -> None:
            try:
                fn()
            except BaseException as exc:  # noqa: BLE001 - re-raised below
                errors.append(exc)
            finally:
                connections.close_all()

        return inner

    threads = [threading.Thread(target=wrap(t)) for t in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
    for t in threads:
        assert not t.is_alive(), "a racing thread deadlocked"
    if errors:
        raise errors[0]


@requires_row_locks
@pytest.mark.django_db(transaction=True)
def test_a_second_writer_blocks_on_the_offset_watermark() -> None:
    """The forced-interleave case: B must not read A's watermark mid-flight.

    Thread A allocates an offset and then *holds its transaction open* while
    thread B tries to allocate one. If the watermark row is genuinely locked,
    B blocks until A commits and then reads the advanced high-water mark. If
    it is not, B reads the pre-A value and hands out the offset A already
    took -- two events at the same offset in the same stream, which the
    unique constraint would reject at insert time in production.

    **The first append is load-bearing.** Without it this test passes even with
    the `select_for_update()` removed, and so proves nothing: on a stream that
    has never allocated, the watermark row does not exist yet, so B's
    `get_or_create` races A's *insert* instead of reading a stale value. It
    blocks on the unique key, retries, and reads the committed high-water mark
    -- serialised by the constraint rather than by the lock. Only once the row
    exists is the lock the thing standing between the two writers. Verified by
    mutation: removing the lock leaves this failing and left the original
    passing.
    """
    store = DjangoStreamStore()
    store.create(PATH)
    store.append(PATH, b'{"seed": true}')

    got: dict[str, int] = {}
    a_allocated = threading.Event()
    b_started = threading.Event()

    def writer_a() -> None:
        with transaction.atomic():
            stream = Stream.objects.get(stream_id=PATH)
            got["a"] = stream.get_next_offset_block(1)
            a_allocated.set()
            # Stay inside the transaction until B has had a fair chance to
            # reach the lock. The sleep is what makes the interleave real
            # rather than accidental.
            assert b_started.wait(timeout=10)
            time.sleep(0.5)

    def writer_b() -> None:
        assert a_allocated.wait(timeout=10)
        b_started.set()
        with transaction.atomic():
            stream = Stream.objects.get(stream_id=PATH)
            got["b"] = stream.get_next_offset_block(1)

    _run(writer_a, writer_b)

    # Offset 1 belongs to the seed append, so the two racers must get 2 and 3.
    assert sorted(got.values()) == [2, 3], (
        f"two connections were handed overlapping offsets: {got}. "
        "The watermark row lock did not hold."
    )


@requires_row_locks
@pytest.mark.django_db(transaction=True)
def test_many_concurrent_appends_produce_no_duplicate_offsets() -> None:
    """The realistic case: four writers appending to one stream at once.

    No barriers, no orchestration -- just contention. Every append must land
    at its own offset, and the set of offsets must be exactly 1..N with no
    gaps, because allocation is a contiguous reservation and not a
    best-effort guess.
    """
    store = DjangoStreamStore()
    store.create(PATH)

    writers = 4
    per_writer = 15
    ready = threading.Barrier(writers)

    def writer(n: int) -> Any:
        def inner() -> None:
            # Start together, so the appends actually overlap.
            ready.wait(timeout=10)
            for i in range(per_writer):
                store.append(PATH, f'{{"w": {n}, "i": {i}}}'.encode())

        return inner

    _run(*[writer(n) for n in range(writers)])

    offsets = sorted(
        StreamEntry.objects.filter(stream__stream_id=PATH).values_list(
            "offset", flat=True
        )
    )
    total = writers * per_writer
    assert offsets == list(range(1, total + 1)), (
        f"expected offsets 1..{total} with no duplicates or gaps, got "
        f"{len(offsets)} offsets ending {offsets[-5:] if offsets else []}"
    )
