"""Waiting readers on the Postgres store wake on `NOTIFY` instead of polling (#290).

Long-poll and SSE over `DjangoStreamStore` used to re-read the stream every
50 ms, so each waiting device cost about eighty queries a second while nothing
happened. With `RAKAIA_NOTIFY_READERS` on, a write sends a notification on
commit and one listening connection per process wakes the readers waiting on
that stream.

The waits need a real second connection and committed writes, so everything
here is `transaction=True`, and the wake-up cases run only on Postgres.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Iterator
from typing import Any

import pytest
from asgiref.sync import sync_to_async
from django.db import connection, connections
from django.test import override_settings

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.wakeups import Listener, listener_for, stop_listeners
from rakaia import AppendOptions
from rakaia.protocol_server import _wait_for_messages
from tests.asgi_client import asgi_client

pytestmark = pytest.mark.django_db(transaction=True)

JSON = {"content-type": "application/json"}

on_postgres = pytest.mark.skipif(
    connection.vendor != "postgresql",
    reason="LISTEN/NOTIFY is Postgres only -- run with RAKAIA_TEST_DB=postgres",
)


@pytest.fixture(autouse=True)
def _close_listeners() -> Iterator[None]:
    yield
    stop_listeners()


def _count_peeks(store: DjangoStreamStore) -> list[str]:
    """Record every `peek` the store is asked for."""
    calls: list[str] = []
    real = store.peek

    def peek(path: str, offset: str | None) -> Any:
        calls.append(path)
        return real(path, offset)

    store.peek = peek  # type: ignore[method-assign]
    return calls


async def _append_soon(store: DjangoStreamStore, path: str) -> None:
    """Append from another thread, so another connection, as another process would."""
    await asyncio.sleep(0.2)
    await sync_to_async(store.append, thread_sensitive=False)(path, b'{"n": 1}')


async def _listening(store: DjangoStreamStore, path: str) -> None:
    """Wait until the store's listener is up, as a long-running server would be."""
    deadline = time.monotonic() + 5
    while store.new_messages_event(path) is None:
        assert time.monotonic() < deadline, "listener never started"
        await asyncio.sleep(0.01)


@on_postgres
@override_settings(RAKAIA_NOTIFY_READERS=True)
async def test_a_waiting_reader_makes_no_query_while_nothing_changes() -> None:
    store = DjangoStreamStore()
    await store.run_sync(store.create, "s", content_type="application/json")
    await _listening(store, "s")
    head = await store.run_sync(store.get_current_offset, "s")
    peeks = _count_peeks(store)

    messages, timed_out, _ = await _wait_for_messages(store, "s", head, 0.6)

    # One read on arrival and one as the wait ends; none in between.
    assert (messages, timed_out) == ([], True)
    assert len(peeks) == 2, f"the reader polled: {len(peeks)} reads in 0.6 s"


@on_postgres
@override_settings(RAKAIA_NOTIFY_READERS=True)
async def test_a_reader_woken_once_is_quiet_on_its_next_wait() -> None:
    """The event that woke it is used up, not left set for the next wait."""
    store = DjangoStreamStore()
    await store.run_sync(store.create, "s", content_type="application/json")
    await _listening(store, "s")
    head = await store.run_sync(store.get_current_offset, "s")
    writer = asyncio.create_task(_append_soon(store, "s"))
    started = time.monotonic()
    _, timed_out, _ = await _wait_for_messages(store, "s", head, 5.0)
    await writer
    assert not timed_out and time.monotonic() - started < 2.0, "never woken"
    head = await store.run_sync(store.get_current_offset, "s")
    peeks = _count_peeks(store)

    await _wait_for_messages(store, "s", head, 0.6)

    assert len(peeks) == 2, f"the reader polled: {len(peeks)} reads in 0.6 s"


@on_postgres
@override_settings(RAKAIA_NOTIFY_READERS=True)
async def test_an_append_to_several_streams_wakes_readers_on_each() -> None:
    store = DjangoStreamStore()
    for path in ("a", "b"):
        await store.run_sync(store.create, path, content_type="application/json")
    await _listening(store, "a")
    heads = [await store.run_sync(store.get_current_offset, p) for p in ("a", "b")]

    async def append_to_both() -> None:
        await asyncio.sleep(0.2)
        await sync_to_async(store.append_to_many, thread_sensitive=False)(
            ["a", "b"], {"n": 1}
        )

    started = time.monotonic()
    writer = asyncio.create_task(append_to_both())
    results = await asyncio.gather(
        *(
            _wait_for_messages(store, p, head, 5.0)
            for p, head in zip(("a", "b"), heads, strict=True)
        )
    )
    await writer

    assert [timed_out for _, timed_out, _ in results] == [False, False]
    assert time.monotonic() - started < 2.0


def _end_the_listening_connection() -> int:
    with connections["default"].cursor() as cursor:
        cursor.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
            " WHERE query = 'LISTEN rakaia_streams'"
            " AND datname = current_database() AND pid <> pg_backend_pid()"
        )
        return len(cursor.fetchall())


@on_postgres
@override_settings(RAKAIA_NOTIFY_READERS=True)
async def test_losing_the_listening_connection_wakes_every_waiter() -> None:
    """They read again rather than sleep through what the dead connection missed."""
    store = DjangoStreamStore()
    for path in ("a", "b"):
        await store.run_sync(store.create, path, content_type="application/json")
    await _listening(store, "a")
    events = [store.new_messages_event(p) for p in ("a", "b")]

    ended = await sync_to_async(_end_the_listening_connection, thread_sensitive=False)()

    assert ended == 1
    await asyncio.wait_for(
        asyncio.gather(*(e.wait() for e in events if e is not None)), timeout=3.0
    )
    assert None not in events
    await _listening(store, "a")  # and it comes back


@on_postgres
async def test_an_append_that_closes_arrives_even_if_its_notification_is_lost() -> None:
    """A writer without the setting sends nothing; the reader's last look at the
    end of its wait must find the message, not only that the stream closed."""
    store = DjangoStreamStore()
    with override_settings(RAKAIA_NOTIFY_READERS=True):
        async with asgi_client(store, long_poll_timeout=0.8) as client:
            await client.put("/s", headers=JSON)
            await client.post("/s", content=b'{"n":1}', headers=JSON)
            tail = (await client.get("/s")).headers["stream-next-offset"]
            await _listening(store, "/s")

            async def close_with_a_message() -> None:
                await asyncio.sleep(0.2)
                with override_settings(RAKAIA_NOTIFY_READERS=False):
                    await sync_to_async(store.append, thread_sensitive=False)(
                        "/s", b'{"n":2}', AppendOptions(close=True)
                    )

            read, _ = await asyncio.gather(
                client.get("/s", params={"offset": tail, "live": "long-poll"}),
                close_with_a_message(),
            )

    assert read.status_code == 200
    assert read.json() == [{"n": 2}]


@on_postgres
@override_settings(RAKAIA_NOTIFY_READERS=True)
async def test_an_append_from_another_connection_wakes_the_reader() -> None:
    store = DjangoStreamStore()
    await store.run_sync(store.create, "s", content_type="application/json")
    await _listening(store, "s")
    head = await store.run_sync(store.get_current_offset, "s")

    async def append_soon() -> None:
        await asyncio.sleep(0.2)
        # Not thread-sensitive: a different thread, so a different connection,
        # as a write from another process would be.
        await sync_to_async(store.append, thread_sensitive=False)("s", b'{"n": 1}')

    started = time.monotonic()
    writer = asyncio.create_task(append_soon())
    messages, timed_out, _ = await _wait_for_messages(store, "s", head, 5.0)
    await writer

    assert not timed_out
    assert [m.data for m in messages] == [b'{"n": 1}']
    assert time.monotonic() - started < 2.0


@on_postgres
@override_settings(RAKAIA_NOTIFY_READERS=True)
async def test_closing_the_stream_wakes_the_reader() -> None:
    store = DjangoStreamStore()
    await store.run_sync(store.create, "s", content_type="application/json")
    await _listening(store, "s")
    head = await store.run_sync(store.get_current_offset, "s")

    async def close_soon() -> None:
        await asyncio.sleep(0.2)
        await sync_to_async(store.close_stream, thread_sensitive=False)("s")

    started = time.monotonic()
    closer = asyncio.create_task(close_soon())
    _, _, closed = await _wait_for_messages(store, "s", head, 5.0)
    await closer

    assert closed
    assert time.monotonic() - started < 2.0


async def test_without_the_setting_the_store_offers_no_wakeups() -> None:
    """An app that has not opted in keeps polling and opens no connection."""
    store = DjangoStreamStore()
    assert store.new_messages_event("s") is None
    assert listener_for(None) is None


@on_postgres
@override_settings(RAKAIA_NOTIFY_READERS=True)
async def test_deleting_the_stream_wakes_the_reader() -> None:
    store = DjangoStreamStore()
    await store.run_sync(store.create, "s", content_type="application/json")
    await _listening(store, "s")
    head = await store.run_sync(store.get_current_offset, "s")

    async def delete_soon() -> None:
        await asyncio.sleep(0.2)
        await sync_to_async(store.delete, thread_sensitive=False)("s")

    started = time.monotonic()
    deleter = asyncio.create_task(delete_soon())
    messages, timed_out, _ = await _wait_for_messages(store, "s", head, 5.0)
    await deleter

    # A stream gone mid-wait reads as an ordinary timeout, but an early one.
    assert (messages, timed_out) == ([], True)
    assert time.monotonic() - started < 2.0


@pytest.mark.skipif(connection.vendor == "postgresql", reason="the non-Postgres case")
@override_settings(RAKAIA_NOTIFY_READERS=True)
async def test_a_database_without_notify_keeps_polling() -> None:
    store = DjangoStreamStore()
    assert store.new_messages_event("s") is None
    assert listener_for(None) is None


class _Psycopg2Listener(Listener):
    """The same listener on psycopg2, the driver Partisipa runs.

    Django here uses psycopg 3, so only the listening connection is swapped;
    the writes still notify through Django.
    """

    def _connect(self) -> Any:
        psycopg2 = pytest.importorskip("psycopg2")
        params = connection.settings_dict
        return psycopg2.connect(
            host=params["HOST"],
            port=params["PORT"],
            user=params["USER"],
            password=params["PASSWORD"],
            dbname=connection.get_connection_params()["dbname"],
        )


@on_postgres
@override_settings(RAKAIA_NOTIFY_READERS=True)
async def test_the_listener_also_hears_appends_on_psycopg2() -> None:
    pytest.importorskip("psycopg2")
    store = DjangoStreamStore()
    await store.run_sync(store.create, "s", content_type="application/json")
    listener = _Psycopg2Listener("default", asyncio.get_running_loop())
    try:
        deadline = time.monotonic() + 5
        while (event := listener.event_for("s")) is None:
            assert time.monotonic() < deadline, "listener never started"
            await asyncio.sleep(0.01)

        await sync_to_async(store.append, thread_sensitive=False)("s", b'{"n": 1}')

        await asyncio.wait_for(event.wait(), timeout=2.0)
    finally:
        listener.stop()
