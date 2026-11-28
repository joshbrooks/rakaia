"""Long-poll and SSE wait for a stream to change the same way (#395).

Each mode used to carry its own copy of the waiting rules, and the copies had
drifted: a woken long-poll ignored the page size, an SSE connection on a store
that cannot signal read each message twice, and SSE looked the stream up after
reading it, so an append that also closed could be reported closed unsent.
"""

from __future__ import annotations

import asyncio
from typing import Any

from rakaia import AppendOptions, StreamStore, create_app
from rakaia.live_read import LiveRead
from rakaia.protocol_server import ServerOptions
from tests.asgi_client import asgi_client
from tests.test_rakaia.test_shared_wakeup_read import SseConnection, _settle


async def test_a_long_poll_woken_by_a_burst_answers_one_page() -> None:
    store = StreamStore()
    store.create("/s", content_type="application/json")
    tail = store.get_current_offset("/s")

    async def burst() -> None:
        await asyncio.sleep(0.05)
        for n in range(5):
            store.append("/s", b'{"n":%d}' % n)

    async with asgi_client(store, long_poll_timeout=5.0, read_page_size=2) as client:
        response, _ = await asyncio.gather(
            client.get(f"/s?offset={tail}&live=long-poll"), burst()
        )

    assert response.status_code == 200
    assert response.json() == [{"n": 0}, {"n": 1}]
    assert "stream-up-to-date" not in response.headers


class PollingStore(StreamStore):
    """A store that cannot signal, counting the reads that found messages."""

    def __init__(self) -> None:
        super().__init__()
        self.reads_with_messages = 0

    def new_messages_event(self, _path: str) -> Any:
        return None

    async def run_sync(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        answer = await super().run_sync(fn, *args, **kwargs)
        name = getattr(fn, "__name__", "")
        found = answer[0] if name == "read" else answer if name == "peek" else None
        if found:
            self.reads_with_messages += 1
        return answer


async def test_sse_on_a_store_that_polls_reads_each_message_once() -> None:
    store = PollingStore()
    store.create("/s", content_type="application/json")
    app = create_app(store=store, options=ServerOptions(long_poll_timeout=5))
    connection = SseConnection(app)
    handler = connection.start()
    await _settle()

    store.append("/s", b'{"n":1}')
    await _settle()

    try:
        assert b'{"n":1}' in connection.body
        assert store.reads_with_messages == 1
    finally:
        connection.gone.set()
        await handler


class CloseDuringReadStore(StreamStore):
    """Once armed, appends a closing message just after a read takes its
    snapshot, so it lands between the read and anything after it."""

    def __init__(self) -> None:
        super().__init__()
        self.armed = False

    async def run_sync(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        answer = fn(*args, **kwargs)
        if self.armed and getattr(fn, "__name__", "") == "read":
            self.armed = False
            self.append("/s", b'{"n":2}', AppendOptions(close=True))
        return answer


async def test_sse_sends_a_message_whose_append_closed_the_stream_mid_read() -> None:
    store = CloseDuringReadStore()
    store.create("/s", content_type="application/json")
    store.append("/s", b'{"n":1}')
    store.armed = True
    app = create_app(
        store=store, options=ServerOptions(long_poll_timeout=5, sse_max_seconds=3)
    )
    connection = SseConnection(app)

    await asyncio.wait_for(connection.start(), timeout=3)

    assert b'{"n":1}' in connection.body
    assert b'{"n":2}' in connection.body


class ReadCountingStore(StreamStore):
    """Counts the calls that read messages, found or not."""

    def __init__(self) -> None:
        super().__init__()
        self.reads = 0

    async def run_sync(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        if getattr(fn, "__name__", "") in ("read", "peek"):
            self.reads += 1
        return await super().run_sync(fn, *args, **kwargs)


async def test_an_idle_sse_keep_alive_does_not_read_messages() -> None:
    """A keep-alive looks the stream up, which catches a lost signal, but
    reads its messages only when the tail has moved (each idle connection used
    to read at every keep-alive)."""
    store = ReadCountingStore()
    store.create("/s", content_type="application/json")
    app = create_app(store=store, options=ServerOptions(long_poll_timeout=0.05))
    connection = SseConnection(app)
    handler = connection.start()
    await _settle()
    store.reads = 0

    await asyncio.sleep(0.5)  # about ten keep-alives

    try:
        assert connection.body.count(b"event: control") > 5
        assert store.reads == 0
    finally:
        connection.gone.set()
        await handler


class CloseAfterFirstCallStore(StreamStore):
    """Once armed, appends a closing message just after the next `get` or
    `read` takes its snapshot, so it lands in the middle of one look."""

    def __init__(self) -> None:
        super().__init__()
        self.armed = False

    async def run_sync(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        answer = fn(*args, **kwargs)
        if self.armed and getattr(fn, "__name__", "") in ("get", "read"):
            self.armed = False
            self.append("/s", b'{"n":1}', AppendOptions(close=True))
        return answer


async def test_a_closing_append_in_the_middle_of_a_look_is_delivered() -> None:
    """The look finds the stream before its messages: the other way round, an
    empty read followed by a closed stream answers closed with the message
    unsent."""
    store = CloseAfterFirstCallStore()
    store.create("/s", content_type="application/json")
    tail = store.get_current_offset("/s")
    assert tail is not None
    store.armed = True

    page = await LiveRead(store, "/s", tail, limit=None).next_page(2.0)

    assert page is not None
    assert [m.data for m in page.messages] == [b'{"n":1}']


class TouchCountingStore(StreamStore):
    """Counts the calls that extend a stream's TTL window: `touch`, and
    `read`, which extends it as it reads."""

    def __init__(self) -> None:
        super().__init__()
        self.touches = 0

    async def run_sync(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        if getattr(fn, "__name__", "") in ("touch", "read"):
            self.touches += 1
        return await super().run_sync(fn, *args, **kwargs)


async def test_opening_an_sse_connection_at_the_tail_extends_the_ttl() -> None:
    """Opening a connection is a read, even when there is nothing to send."""
    store = TouchCountingStore()
    store.create("/s", content_type="application/json", ttl_seconds=60)
    store.append("/s", b'{"n":1}')
    tail = store.get_current_offset("/s")
    app = create_app(store=store, options=ServerOptions(long_poll_timeout=5))
    connection = SseConnection(app)
    connection.start_query = f"offset={tail}&live=sse".encode()
    handler = connection.start()
    await _settle()

    try:
        assert store.touches == 1
    finally:
        connection.gone.set()
        await handler
