"""Offsets that say which copy of a stream they came from.

A client keeps its offset across restarts and resumes from it. If the stream
is deleted and created again, or the database restored from a backup, the same
offset can name a different event, and the client silently skips or repeats
changes. `StampedOffsetStore` wraps a store so that every offset it hands out
reads ``<generation>.<app stamp>_<offset>``: the stream's generation, which
changes when it is created again, and a stamp the app supplies, which the app
changes when it is restored. An offset carrying another stamp, or one past the
stream's tail, raises `OffsetGone`, which the protocol server answers with
``410 Gone``, telling the client to read again from ``-1``.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Iterable
from dataclasses import replace
from typing import Any

from .offsets import MAX_OFFSET_LENGTH, after, is_syntactically_valid
from .protocols import SignalsNewMessages, StreamServerStore
from .types import (
    AppendResult,
    CloseResult,
    InvalidOffset,
    OffsetGone,
    Stream,
    StreamMessage,
    StreamNotFound,
)

__all__ = ["StampedOffsetStore"]

# What a stamped offset leaves for the wrapped store's own offset, which is
# 20 characters on the Django store and 33 on the in-memory one.
_ROOM_FOR_THE_STORES_OFFSET = 64

# Offsets the server passes through untouched: the start of the stream.
_FROM_THE_START = (None, "", "-1")


class StampedOffsetStore:
    """A `StreamServerStore` whose offsets carry the stream's generation and an
    app stamp, so an offset from another copy of the stream answers 410.

    `stamp(path)` returns the app's part. It is called on every read and write,
    inside `run_sync`, so it may query the database.

    An incoming offset is split at its **first** ``_``: everything before it is
    the stamp, everything after is the wrapped store's own offset, which may
    contain ``_`` itself (the in-memory store's does). So neither the app stamp
    nor the generation may contain ``_``, or anything else an offset may not
    carry, and building an offset from one raises `ValueError` rather than
    handing out an offset that would later split in the wrong place. The wrapped store must report
    `get_generation`. Waiting readers are woken by the wrapped store's
    notifications when it has them.
    """

    def __init__(
        self, store: StreamServerStore, *, stamp: Callable[[str], str]
    ) -> None:
        generation: Callable[[str], str | None] | None = getattr(
            store, "get_generation", None
        )
        if generation is None:
            raise TypeError(f"{type(store).__name__} has no get_generation")
        self._generation = generation
        self.store = store
        self.stamp = stamp

    # -- the stamp ------------------------------------------------------------

    def _prefix(self, path: str) -> str:
        """``<generation>.<app stamp>`` for `path`; raises if it is absent.

        The generation is looked up first, so the app's stamp is never asked
        for a stream that does not exist.
        """
        generation = self._generation(path)
        if generation is None:
            raise StreamNotFound(f"Stream not found: {path}")
        if "_" in generation or not is_syntactically_valid(generation):
            raise ValueError(f"generation {generation!r} cannot be part of an offset")
        app = self.stamp(path)
        if not app or "_" in app or not is_syntactically_valid(app):
            raise ValueError(f"app stamp {app!r} cannot be part of an offset")
        prefix = f"{generation}.{app}"
        if len(prefix) + 1 + _ROOM_FOR_THE_STORES_OFFSET >= MAX_OFFSET_LENGTH:
            raise ValueError(f"app stamp {app!r} is too long to be part of an offset")
        return prefix

    def _out(self, prefix: str, offset: str) -> str:
        return f"{prefix}_{offset}"

    def _in(self, path: str, offset: str | None) -> tuple[str, str | None]:
        """The prefix for `path` and the wrapped store's offset for `offset`.

        Raises `InvalidOffset` for an offset with no stamp and `OffsetGone` for
        one with another stamp or past the tail.
        """
        prefix = self._prefix(path)
        if offset in _FROM_THE_START:
            return prefix, offset
        assert offset is not None
        theirs, sep, position = offset.partition("_")
        if not sep or not theirs or not position:
            raise InvalidOffset(f"Offset has no stamp: {offset}")
        if theirs != prefix:
            raise OffsetGone(f"Offset is from another copy of this stream: {offset}")
        stream = self.store.get(path)
        if stream is not None and after(position, stream.current_offset):
            raise OffsetGone(f"Offset is past the end of this stream: {offset}")
        return prefix, position

    def _messages_out(
        self, prefix: str, messages: list[StreamMessage]
    ) -> list[StreamMessage]:
        return [replace(m, offset=self._out(prefix, m.offset)) for m in messages]

    # -- the store ------------------------------------------------------------

    async def run_sync(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        return await self.store.run_sync(fn, *args, **kwargs)

    def new_messages_event(self, path: str) -> asyncio.Event | None:
        if isinstance(self.store, SignalsNewMessages):
            return self.store.new_messages_event(path)
        return None

    def get_generation(self, path: str) -> str | None:
        return self._generation(path)

    def has(self, path: str) -> bool:
        return self.store.has(path)

    def create(self, path: str, **kwargs: Any) -> Any:
        return self.store.create(path, **kwargs)

    def get(self, path: str) -> Stream | None:
        stream = self.store.get(path)
        if stream is None:
            return None
        return replace(
            stream, current_offset=self._out(self._prefix(path), stream.current_offset)
        )

    def get_current_offset(self, path: str) -> str | None:
        stream = self.get(path)
        return None if stream is None else stream.current_offset

    def read(
        self, path: str, offset: str | None = None, *, limit: int | None = None
    ) -> tuple[list[StreamMessage], bool]:
        prefix, inner = self._in(path, offset)
        messages, up_to_date = self.store.read(path, inner, limit=limit)
        return self._messages_out(prefix, messages), up_to_date

    def peek(self, path: str, offset: str | None) -> list[StreamMessage]:
        prefix, inner = self._in(path, offset)
        return self._messages_out(prefix, self.store.peek(path, inner))

    # Every write works out its stamp first: a stamp that fails after the write
    # had landed would answer an error for an append that happened, and a client
    # retrying it would append twice. The one exception is a stream created
    # between the two, which has no stamp until after the write.

    def append(self, path: str, data: bytes, options: Any = None) -> AppendResult:
        prefix = self._prefix_if_present(path)
        return self._result_out(path, prefix, self.store.append(path, data, options))

    def append_many(
        self, path: str, events: Iterable[tuple[bytes, Any]]
    ) -> list[AppendResult]:
        prefix = self._prefix_if_present(path)
        results = self.store.append_many(path, events)
        return [self._result_out(path, prefix, r) for r in results]

    def _prefix_if_present(self, path: str) -> str | None:
        """The prefix, or None for an absent stream, which the write then
        reports in its own way."""
        try:
            return self._prefix(path)
        except StreamNotFound:
            return None

    def _result_out(
        self, path: str, prefix: str | None, result: AppendResult
    ) -> AppendResult:
        if result.message is None:
            return result
        if prefix is None:
            # Absent before the write, there now: created in between.
            prefix = self._prefix(path)
        return replace(result, message=self._messages_out(prefix, [result.message])[0])

    def close_stream(self, path: str, **kwargs: Any) -> CloseResult | None:
        prefix = self._prefix_if_present(path)
        result = self.store.close_stream(path, **kwargs)
        if result is None or not result.final_offset:
            return result
        if prefix is None:
            prefix = self._prefix(path)  # created between the two
        return replace(result, final_offset=self._out(prefix, result.final_offset))

    def touch(self, path: str) -> None:
        self.store.touch(path)

    def delete(self, path: str) -> bool:
        return self.store.delete(path)

    def check_protocol_delete(self, path: str) -> None:
        self.store.check_protocol_delete(path)
