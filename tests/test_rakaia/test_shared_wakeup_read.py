"""Readers woken together at the same offset share one read.

A thousand devices waiting at the end of a stream are woken by one append. Each
used to read the stream on its own, so one new message cost a thousand
identical reads. Readers woken by the same change at the same offset now share
one; a reader at another offset, or one that was not woken, still reads alone.
"""

from __future__ import annotations

import asyncio
from typing import Any

import httpx

from rakaia import AppendOptions, StreamStore, create_app
from rakaia.protocol_server import ServerOptions
from tests.live_wait import wait_for_messages as _wait_for_messages

WAITERS = 1000


class CountingStore(StreamStore):
    """Counts the reads a server makes, and pauses each one for a moment, as a
    database would, so the woken readers all arrive while it is in flight."""

    def __init__(self) -> None:
        super().__init__()
        self.reads = 0

    async def run_sync(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        if getattr(fn, "__name__", "") in ("peek", "read"):
            self.reads += 1
            await asyncio.sleep(0.01)
        return await super().run_sync(fn, *args, **kwargs)


async def _store() -> CountingStore:
    store = CountingStore()
    await store.run_sync(store.create, "/s", content_type="application/json")
    return store


async def _settle() -> None:
    """Let every started task reach its wait."""
    for _ in range(5):
        await asyncio.sleep(0.05)


async def test_a_thousand_long_polls_share_one_read_after_an_append() -> None:
    store = await _store()
    head = store.get_current_offset("/s")
    assert head is not None
    waiters = [
        asyncio.create_task(_wait_for_messages(store, "/s", head, 5.0))
        for _ in range(WAITERS)
    ]
    await _settle()
    store.reads = 0

    store.append("/s", b'{"n":1}')
    results = await asyncio.gather(*waiters)

    assert all([m.data for m in r[0]] == [b'{"n":1}'] for r in results)
    assert store.reads == 1, f"{store.reads} reads for one append"


class LateSignalStore(StreamStore):
    """Holds each wake-up until `release`, as a notification arriving after
    its commit does on Postgres."""

    def __init__(self) -> None:
        super().__init__()
        self.held: list[str] = []

    def _notify_waiters(self, path: str) -> None:
        self.held.append(path)

    def release(self) -> None:
        for path in self.held:
            super()._notify_waiters(path)
        self.held = []


async def test_readers_woken_together_at_different_offsets_do_not_share() -> None:
    """A reader that read after an append but before its wake-up waits at a
    later offset on the same event as one that read before it."""
    store = LateSignalStore()
    store.create("/s", content_type="application/json")
    first = store.get_current_offset("/s")
    assert first is not None
    early = asyncio.create_task(_wait_for_messages(store, "/s", first, 5.0))
    await _settle()
    store.append("/s", b'{"n":1}')
    second = store.get_current_offset("/s")
    assert second is not None
    late = asyncio.create_task(_wait_for_messages(store, "/s", second, 5.0))
    await _settle()
    store.append("/s", b'{"n":2}')

    store.release()

    assert [m.data for m in (await early)[0]] == [b'{"n":1}', b'{"n":2}']
    assert [m.data for m in (await late)[0]] == [b'{"n":2}']


class SseConnection:
    """One SSE request driven by hand, held open until the test ends it."""

    def __init__(self, app: Any) -> None:
        self.app = app
        self.body = b""
        self.gone = asyncio.Event()
        self._asked = False
        self.start_query = b"offset=-1&live=sse"

    async def _receive(self) -> dict[str, Any]:
        if not self._asked:
            self._asked = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await self.gone.wait()
        return {"type": "http.disconnect"}

    async def _send(self, message: dict[str, Any]) -> None:
        self.body += message.get("body", b"")

    def start(self) -> asyncio.Task[None]:
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/s",
            "query_string": self.start_query,
            "headers": [],
        }
        return asyncio.create_task(self.app(scope, self._receive, self._send))


async def test_a_thousand_sse_connections_share_one_read_after_an_append() -> None:
    store = await _store()
    app = create_app(store=store, options=ServerOptions(long_poll_timeout=30))
    connections = [SseConnection(app) for _ in range(WAITERS)]
    handlers = [c.start() for c in connections]
    await _settle()
    store.reads = 0

    store.append("/s", b'{"n":1}')
    await _settle()

    try:
        assert all(b'{"n":1}' in c.body for c in connections)
        assert store.reads == 1, f"{store.reads} reads for one append"
    finally:
        for c in connections:
            c.gone.set()
        await asyncio.gather(*handlers)


async def test_sse_sends_the_message_that_closed_a_stream_whose_signal_was_lost() -> (
    None
):
    """With no wake-up, the connection's wait times out; it must read before it
    reports the stream closed, or the closing message is never sent."""
    store = LateSignalStore()
    store.create("/s", content_type="application/json")
    store.append("/s", b'{"n":1}')
    app = create_app(
        store=store, options=ServerOptions(long_poll_timeout=0.2, sse_max_seconds=3)
    )
    connection = SseConnection(app)
    handler = connection.start()
    await _settle()

    store.append("/s", b'{"n":2}', AppendOptions(close=True))  # signal held forever
    await asyncio.wait_for(handler, timeout=3)

    assert b'{"n":2}' in connection.body


async def test_a_reader_leaving_does_not_cancel_the_read_it_shares() -> None:
    store = await _store()
    head = store.get_current_offset("/s")
    assert head is not None
    first = asyncio.create_task(_wait_for_messages(store, "/s", head, 5.0))
    second = asyncio.create_task(_wait_for_messages(store, "/s", head, 5.0))
    await _settle()
    store.append("/s", b'{"n":1}')
    await asyncio.sleep(0.003)  # the first has started the shared read (10 ms)

    first.cancel()

    assert [m.data for m in (await second)[0]] == [b'{"n":1}']


async def test_sse_and_long_poll_readers_woken_together_share_one_read() -> None:
    """Both modes wait the same way, so they read the same page."""
    store = await _store()
    head = store.get_current_offset("/s")
    assert head is not None
    app = create_app(store=store, options=ServerOptions(long_poll_timeout=30))
    connection = SseConnection(app)
    handler = connection.start()
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )
    long_poll = asyncio.create_task(client.get(f"/s?offset={head}&live=long-poll"))
    await _settle()
    store.reads = 0

    store.append("/s", b'{"n":1}')
    await _settle()

    try:
        assert (await long_poll).json() == [{"n": 1}]
        assert b'{"n":1}' in connection.body
        assert store.reads == 1, f"{store.reads} reads for one append"
    finally:
        connection.gone.set()
        await handler
        await client.aclose()


class SlowReadStore(StreamStore):
    """Takes its snapshot at once and answers 0.3 s later, as a query would."""

    def __init__(self) -> None:
        super().__init__()
        self.slow = False

    async def run_sync(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        answer = fn(*args, **kwargs)
        if self.slow and getattr(fn, "__name__", "") == "read":
            await asyncio.sleep(0.3)
        return answer


class SlowClient(SseConnection):
    """Holds its first send until `release` is set, as a client on a slow link."""

    def __init__(self, app: Any) -> None:
        super().__init__(app)
        self.release = asyncio.Event()

    async def _send(self, message: dict[str, Any]) -> None:
        if message.get("body"):
            await self.release.wait()
        await super()._send(message)


async def test_a_reader_that_joins_late_does_not_miss_a_second_append() -> None:
    """B is still sending when the first append wakes A, and joins A's read
    after a second append has landed past it."""
    store = SlowReadStore()
    store.create("/s", content_type="application/json")
    app = create_app(store=store, options=ServerOptions(long_poll_timeout=5))
    a, b = SseConnection(app), SlowClient(app)
    handlers = [a.start(), b.start()]
    await asyncio.sleep(0.1)
    store.slow = True
    store.append("/s", b'{"n":1}')  # A wakes and starts the read
    await asyncio.sleep(0.1)  # its snapshot is taken; the answer is in flight
    store.append("/s", b'{"n":2}')
    b.release.set()  # B finishes sending, then reads
    await asyncio.sleep(1.0)

    try:
        assert b'{"n":2}' in a.body
        assert b'{"n":2}' in b.body
    finally:
        a.gone.set()
        b.gone.set()
        await asyncio.gather(*handlers)
