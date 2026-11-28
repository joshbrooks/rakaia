"""Waiting for a stream to change, for long-poll and SSE alike (#395).

`LiveRead` follows one reader along one stream. `next_page(wait)` answers the
next page after the position the reader has reached, waiting up to `wait`
seconds for one when the reader is caught up. Long-poll calls it once; SSE calls
it in a loop. Every rule about waking readers lives here and nowhere else.

A look takes the store's signal for the next change first, then the stream, then
the messages, in that order. The signal first, so a change landing during the
look has already set it. The stream before the messages, so an append that also
closes, landing between the two, is never reported closed with its message
unsent. A wait that times out looks once more before it answers, so a lost
signal makes a message late by one wait, never lost. Readers woken by the same
signal at the same position share one look.
"""

from __future__ import annotations

import asyncio
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from typing import Any

from .protocols import SignalsNewMessages, StreamServerStore
from .types import Stream, StreamMessage, StreamNotFound

__all__ = ["LiveRead", "Page"]

# How often a reader re-looks at a store that cannot signal new messages. A
# file or database log can be appended to by another process, which no event in
# this one would see, so it is polled; 50ms keeps catch-up latency well under
# what the protocol expects without making an idle waiter expensive (#290).
POLL_INTERVAL_SECONDS = 0.05


@dataclass(frozen=True)
class Page:
    """One look at a stream: the stream as found, then the messages after the
    reader's position. `up_to_date` is False when the page limit cut it short.

    An empty page means the stream is closed at the reader's position
    (`stream.closed`) or the wait ran out with nothing new.
    """

    stream: Stream
    messages: list[StreamMessage]
    up_to_date: bool


# The looks in flight for readers woken together: one per store, path, offset,
# page limit, the signal that woke them and the signal that will wake them next.
_shared_looks: dict[
    tuple[int, str, str, int | None, asyncio.Event, asyncio.Event | None],
    asyncio.Future[Any],
] = {}


async def _once_for_all(
    key: tuple[int, str, str, int | None, asyncio.Event, asyncio.Event | None],
    look: Callable[[], Awaitable[Any]],
) -> Any:
    """Run `look()` once for every reader with this key.

    The look is shielded, so one reader leaving does not cancel it for the
    others. The next signal is in the key: a reader arriving after a second
    change has replaced the signal the first readers took would otherwise share
    a look that predates that change, then wait on a signal nothing will set.
    """
    shared = _shared_looks.get(key)
    if shared is None:
        shared = asyncio.ensure_future(look())
        _shared_looks[key] = shared

        def forget(done: asyncio.Future[Any]) -> None:
            _shared_looks.pop(key, None)
            if not done.cancelled():
                done.exception()  # retrieved, even if every reader has left

        shared.add_done_callback(forget)
    return await asyncio.shield(shared)


class LiveRead:
    """One reader's position on one stream, and the signal it took last."""

    def __init__(
        self, store: StreamServerStore, path: str, offset: str, *, limit: int | None
    ) -> None:
        self.store = store
        self.path = path
        self.offset = offset
        self.limit = limit
        self._caught_up = False
        self._signal: asyncio.Event | None = None
        self._looked = False

    async def next_page(self, wait: float) -> Page | None:
        """The next page after `offset`, which then moves past it.

        Looks at once, unless the last page left the reader caught up, in which
        case it waits for a change first. Answers as soon as a look finds
        messages or a closed stream, or with an empty page once `wait` runs out.
        None when the stream has gone; `StreamNotFound` if it was absent on the
        very first look.
        """
        deadline = time.monotonic() + wait
        woken_by: asyncio.Event | None = None
        while True:
            if self._caught_up:
                # A wait that runs out still looks once more, which stands in
                # for a signal that was lost.
                woken_by = await self._wait(deadline - time.monotonic())
            page = await self._look(woken_by)
            if page is None:
                return None
            self._advance(page)
            if page.messages or page.stream.closed:
                return page
            if time.monotonic() >= deadline:
                return page

    async def _wait(self, remaining: float) -> asyncio.Event | None:
        """Wait for the signal taken before the last look; the signal if it
        fired, None if the wait ran out or the store polls."""
        signal = self._signal
        if remaining <= 0:
            return None
        if signal is None:
            await asyncio.sleep(min(POLL_INTERVAL_SECONDS, remaining))
            return None
        try:
            await asyncio.wait_for(signal.wait(), timeout=remaining)
        except TimeoutError:
            return None
        return signal

    async def _look(self, woken_by: asyncio.Event | None) -> Page | None:
        store, path, offset = self.store, self.path, self.offset
        signal = (
            store.new_messages_event(path)
            if isinstance(store, SignalsNewMessages)
            else None
        )
        self._signal = signal
        first = not self._looked
        self._looked = True

        async def look() -> Page | None:
            stream = await store.run_sync(store.get, path)
            if stream is None:
                if first:
                    raise StreamNotFound(f"Stream not found: {path}")
                return None
            if stream.current_offset == offset:
                return Page(stream, [], True)
            try:
                messages, up_to_date = await store.run_sync(
                    store.read, path, offset, limit=self.limit
                )
            except StreamNotFound:
                return None  # gone between the two
            return Page(stream, messages, up_to_date)

        if woken_by is None:
            return await look()
        key = (id(store), path, offset, self.limit, woken_by, signal)
        return await _once_for_all(key, look)

    def _advance(self, page: Page) -> None:
        if page.messages:
            self.offset = page.messages[-1].offset
        else:
            # Nothing after the position: it is the tail, in the store's own
            # spelling (`-1` on an empty stream becomes its first offset).
            self.offset = page.stream.current_offset
        self._caught_up = page.up_to_date
