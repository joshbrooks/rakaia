"""An SSE connection ends when its client leaves, and on a timer.

The measured failure: the handler never read `receive`, so after a client
dropped it kept polling the store for a socket that was gone (about 30 `get`
calls a second per orphan on the in-memory store) and a server asked to stop
could not.
"""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Iterator
from typing import Any

import pytest

from rakaia import StreamStore, create_app
from rakaia.protocol_server import DEFAULT_SSE_MAX_SECONDS, ServerOptions


@pytest.fixture(autouse=True)
def fail_instead_of_hanging() -> Iterator[None]:
    """Turn a stuck handler into a failure.

    `asyncio.wait_for` cannot interrupt a handler that spins without awaiting, and
    a spinning loop blocks the one thread the event loop runs on. A signal is
    delivered between bytecodes, so it still gets through.
    """

    def give_up(*_: object) -> None:
        raise TimeoutError("the SSE handler never gave control back")

    previous = signal.signal(signal.SIGALRM, give_up)
    signal.alarm(15)
    try:
        yield
    finally:
        signal.alarm(0)
        signal.signal(signal.SIGALRM, previous)


class CountingStore(StreamStore):
    """A store that counts how often the SSE loop asks it about a stream."""

    def __init__(self) -> None:
        super().__init__()
        self.get_calls = 0

    def get(self, path: str, *args: Any, **kwargs: Any) -> Any:
        self.get_calls += 1
        return super().get(path, *args, **kwargs)


class Connection:
    """One SSE request driven by hand, so the test controls when the client leaves."""

    def __init__(self, store: StreamStore, **options: Any) -> None:
        opts = ServerOptions(long_poll_timeout=0.05)
        for name, value in options.items():
            setattr(opts, name, value)
        self.app = create_app(store=store, options=opts)
        self.sent: list[dict[str, Any]] = []
        self.gone = asyncio.Event()

    async def _receive(self) -> dict[str, Any]:
        if not hasattr(self, "_asked"):
            self._asked = True
            return {"type": "http.request", "body": b"", "more_body": False}
        await self.gone.wait()
        return {"type": "http.disconnect"}

    async def _send(self, message: dict[str, Any]) -> None:
        self.sent.append(message)

    def start(self) -> asyncio.Task[None]:
        scope = {
            "type": "http",
            "method": "GET",
            "path": "/s",
            "query_string": b"offset=-1&live=sse",
            "headers": [],
        }
        return asyncio.create_task(self.app(scope, self._receive, self._send))

    def ended_cleanly(self) -> bool:
        last = self.sent[-1]
        return last["type"] == "http.response.body" and not last.get("more_body")


async def _store_with_stream() -> CountingStore:
    store = CountingStore()
    await store.run_sync(store.create, "/s", content_type="application/json")
    return store


async def test_the_handler_stops_polling_when_the_client_disconnects():
    store = await _store_with_stream()
    conn = Connection(store)
    handler = conn.start()
    await asyncio.sleep(0.3)
    assert store.get_calls > 2, "the connection should be polling while open"

    conn.gone.set()
    await asyncio.wait_for(handler, timeout=2)  # fails, not hangs, if it never ends
    after_exit = store.get_calls
    await asyncio.sleep(0.3)

    assert store.get_calls == after_exit


async def test_the_connection_is_ended_after_the_configured_time():
    store = await _store_with_stream()
    conn = Connection(store, sse_max_seconds=0.3)

    await asyncio.wait_for(conn.start(), timeout=3)

    assert conn.ended_cleanly()
    assert not conn.gone.is_set()  # the server ended it, the client did not leave


async def test_with_no_time_limit_the_connection_stays_open():
    store = await _store_with_stream()
    conn = Connection(store, sse_max_seconds=None)
    handler = conn.start()
    await asyncio.sleep(0.5)
    assert not handler.done()
    conn.gone.set()
    await asyncio.wait_for(handler, timeout=2)


def test_the_default_is_about_sixty_seconds():
    assert ServerOptions().sse_max_seconds == DEFAULT_SSE_MAX_SECONDS == 60.0
