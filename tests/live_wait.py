"""The one-shot long-poll wait, answered the way the store tests check it."""

from __future__ import annotations

from rakaia.live_read import LiveRead
from rakaia.protocols import StreamServerStore
from rakaia.types import StreamMessage


async def wait_for_messages(
    store: StreamServerStore, path: str, offset: str, timeout_seconds: float
) -> tuple[list[StreamMessage], bool, bool]:
    """`(messages, timed_out, closed)` for one wait of up to `timeout_seconds`.

    A stream that goes mid-wait reads as a timeout, as a long-poll answers it.
    """
    page = await LiveRead(store, path, offset, limit=None).next_page(timeout_seconds)
    if page is None:
        return [], True, False
    if page.messages:
        return page.messages, False, False
    return [], not page.stream.closed, page.stream.closed
