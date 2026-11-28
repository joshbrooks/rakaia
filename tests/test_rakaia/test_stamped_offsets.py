"""Offsets that say which copy of a stream they came from, answering 410 for another.

A device keeps its offset across restarts. If the stream is rebuilt, or the
server restored from a backup, the same offset can name a different event, and
the device would silently skip or repeat changes. `StampedOffsetStore` puts the
stream's generation and a stamp the app supplies in front of every offset it
hands out, and answers 410 for an offset carrying another one, which tells a
client to read again from the start.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import pytest

from rakaia import StampedOffsetStore, StreamNotFound, StreamStore
from rakaia.protocols import SignalsNewMessages
from tests.asgi_client import asgi_client

JSON = {"content-type": "application/json"}


class AppStamp:
    """The app's half of the stamp, which a restore would rotate."""

    def __init__(self) -> None:
        self.value = "app1"

    def __call__(self, _path: str) -> str:
        return self.value


def _stamped() -> tuple[StampedOffsetStore, StreamStore, AppStamp]:
    inner = StreamStore()
    app_stamp = AppStamp()
    return StampedOffsetStore(inner, stamp=app_stamp), inner, app_stamp


async def _offset_after_first_append(client: Any) -> str:
    await client.put("/s", headers=JSON)
    await client.post("/s", content=b'{"n":1}', headers=JSON)
    read = await client.get("/s", params={"offset": "-1"})
    assert read.status_code == 200
    return read.headers["stream-next-offset"]


async def test_an_offset_read_before_a_rebuild_answers_410_not_changes() -> None:
    store, _, _ = _stamped()
    async with asgi_client(store) as client:
        offset = await _offset_after_first_append(client)

        await client.delete("/s")
        await client.put("/s", headers=JSON)
        await client.post("/s", content=b'{"n":1}', headers=JSON)
        await client.post("/s", content=b'{"n":2}', headers=JSON)
        read = await client.get("/s", params={"offset": offset})

    assert read.status_code == 410


async def test_an_offset_under_another_app_stamp_answers_410() -> None:
    store, _, app_stamp = _stamped()
    async with asgi_client(store) as client:
        offset = await _offset_after_first_append(client)
        app_stamp.value = "app2"
        read = await client.get("/s", params={"offset": offset})

    assert read.status_code == 410


async def test_an_offset_past_the_tail_answers_410() -> None:
    """A restore can bring back an older copy under the same stamps; an offset
    past its tail is the sign."""
    store, inner, _ = _stamped()
    async with asgi_client(store) as client:
        offset = await _offset_after_first_append(client)
        stamp, _, _ = offset.partition("_")
        await client.post("/s", content=b'{"n":2}', headers=JSON)
        tail = inner.get_current_offset("/s")
        assert tail is not None
        read = await client.get("/s", params={"offset": f"{stamp}_{tail}0"})
        ok = await client.get("/s", params={"offset": offset})

    assert read.status_code == 410
    assert ok.status_code == 200
    assert ok.json() == [{"n": 2}]


async def test_offsets_carry_the_generation_and_the_app_stamp() -> None:
    store, inner, _ = _stamped()
    async with asgi_client(store) as client:
        offset = await _offset_after_first_append(client)

    generation = inner.get_generation("/s")
    assert offset == f"{generation}.app1_{inner.get_current_offset('/s')}"


async def test_an_offset_with_no_stamp_separator_answers_400() -> None:
    """Malformed, as opposed to another stamp's: with no ``_`` there is no stamp
    to compare. (A wrapped store's own offset may contain ``_``, and then reads
    as another stamp, which is 410.)"""
    store, _, _ = _stamped()
    async with asgi_client(store) as client:
        await _offset_after_first_append(client)
        read = await client.get("/s", params={"offset": "nostamp"})

    assert read.status_code == 400


async def test_appends_and_closes_answer_with_stamped_offsets() -> None:
    store, inner, _ = _stamped()
    async with asgi_client(store) as client:
        await client.put("/s", headers=JSON)
        posted = await client.post("/s", content=b'{"n":1}', headers=JSON)
        head = await client.head("/s")

    prefix = f"{inner.get_generation('/s')}.app1_"
    assert posted.headers["stream-next-offset"].startswith(prefix)
    assert head.headers["stream-next-offset"].startswith(prefix)


async def test_a_long_poll_with_a_stamped_offset_wakes_on_an_append() -> None:
    store, _, _ = _stamped()
    async with asgi_client(store, long_poll_timeout=5) as client:
        offset = await _offset_after_first_append(client)

        async def append_soon() -> None:
            await asyncio.sleep(0.1)
            await client.post("/s", content=b'{"n":2}', headers=JSON)

        writer = asyncio.create_task(append_soon())
        read = await client.get("/s", params={"offset": offset, "live": "long-poll"})
        await writer

    assert read.status_code == 200
    assert read.json() == [{"n": 2}]
    assert read.headers["stream-next-offset"].startswith(offset.partition("_")[0])


async def test_waiting_readers_are_woken_by_the_wrapped_store() -> None:
    """Not polled: the wrapper hands back the wrapped store's own event."""
    store, inner, _ = _stamped()
    inner.create("/s")
    assert isinstance(store, SignalsNewMessages)
    assert store.new_messages_event("/s") is inner.new_messages_event("/s")


async def test_sse_with_another_stamp_answers_410_before_streaming() -> None:
    store, _, app_stamp = _stamped()
    async with asgi_client(store) as client:
        offset = await _offset_after_first_append(client)
        app_stamp.value = "app2"
        read = await client.get("/s", params={"offset": offset, "live": "sse"})

    assert read.status_code == 410


async def test_sse_control_events_carry_stamped_offsets() -> None:
    store, _, _ = _stamped()
    async with asgi_client(
        store, long_poll_timeout=0.05, sse_max_seconds=0.2
    ) as client:
        offset = await _offset_after_first_append(client)
        read = await client.get("/s", params={"offset": "-1", "live": "sse"})

    controls = [
        json.loads(line.removeprefix("data:"))
        for block in read.text.split("\n\n")
        if block.startswith("event: control")
        for line in block.splitlines()
        if line.startswith("data:")
    ]
    assert controls
    assert all(c["streamNextOffset"] == offset for c in controls)


@pytest.mark.parametrize("bad", ["has_underscore", "has/slash", "", "has space"])
def test_an_app_stamp_that_cannot_sit_in_an_offset_is_refused(bad: str) -> None:
    store = StampedOffsetStore(StreamStore(), stamp=lambda _path: bad)
    store.create("/s")
    with pytest.raises(ValueError):
        store.get("/s")


def test_a_generation_with_an_underscore_is_refused() -> None:
    """The first ``_`` ends the stamp, so a generation holding one would split
    in the wrong place; a third-party store's generation is not ours to trust."""
    inner = StreamStore()
    inner.create("/s")
    inner.adopt_generation("/s", "gen_with_underscore")
    store = StampedOffsetStore(inner, stamp=lambda _path: "app1")
    with pytest.raises(ValueError, match="generation"):
        store.get("/s")


async def test_closing_answers_with_a_stamped_final_offset() -> None:
    store, inner, _ = _stamped()
    async with asgi_client(store) as client:
        await client.put("/s", headers=JSON)
        await client.post("/s", content=b'{"n":1}', headers=JSON)
        closed = await client.post("/s", headers={**JSON, "stream-closed": "true"})
        resumed = await client.get(
            "/s", params={"offset": closed.headers["stream-next-offset"]}
        )

    assert closed.headers["stream-next-offset"].startswith(
        f"{inner.get_generation('/s')}.app1_"
    )
    assert resumed.status_code == 200


@pytest.mark.parametrize("token", ["_abc", "x_"])
async def test_an_offset_with_an_empty_stamp_or_position_answers_400(
    token: str,
) -> None:
    store, _, _ = _stamped()
    async with asgi_client(store) as client:
        await _offset_after_first_append(client)
        read = await client.get("/s", params={"offset": token})

    assert read.status_code == 400


async def test_a_stamp_that_fails_writes_nothing() -> None:
    """The stamp is worked out before the write, so a client that retries after
    the error does not append twice."""
    inner = StreamStore()
    inner.create("/s", content_type="application/json")

    def broken(_path: str) -> str:
        raise RuntimeError("the app's stamp is unavailable")

    store = StampedOffsetStore(inner, stamp=broken)
    with pytest.raises(RuntimeError):
        store.append("/s", b'{"n":1}')
    with pytest.raises(RuntimeError):
        store.close_stream("/s")

    assert inner.read("/s", "-1")[0] == []
    stream = inner.get("/s")
    assert stream is not None and not stream.closed


class CountingTouches(StreamStore):
    def __init__(self) -> None:
        super().__init__()
        self.touches = 0

    def touch(self, path: str) -> None:
        self.touches += 1
        super().touch(path)


async def test_reads_still_extend_the_wrapped_stores_ttl_window() -> None:
    inner = CountingTouches()
    store = StampedOffsetStore(inner, stamp=lambda _path: "app1")
    async with asgi_client(store) as client:
        await client.put("/s", headers=JSON)
        await client.get("/s", params={"offset": "now"})

    assert inner.touches >= 1


async def test_a_permanent_wrapped_store_still_refuses_a_client_delete() -> None:
    store = StampedOffsetStore(StreamStore(permanent=True), stamp=lambda _path: "app1")
    async with asgi_client(store) as client:
        await client.put("/s", headers=JSON)
        deleted = await client.delete("/s")

    assert deleted.status_code == 405


class CreatedWhileStamping(StreamStore):
    """Creates the stream the moment the wrapper asks for its generation, as a
    PUT landing just before a first append would."""

    def get_generation(self, path: str) -> str | None:
        if not self.has(path):
            generation = None
            self.create(path, content_type="application/json")
            return generation
        return super().get_generation(path)


def test_an_append_racing_the_streams_creation_still_answers_a_stamped_offset() -> None:
    inner = CreatedWhileStamping()
    store = StampedOffsetStore(inner, stamp=lambda _path: "app1")

    result = store.append("/s", b'{"n":1}')

    assert result.message is not None
    assert result.message.offset.startswith(f"{inner.get_generation('/s')}.app1_")


def test_a_stamp_too_long_for_an_offset_is_refused() -> None:
    """An offset must stay under the protocol's 256 characters, or every resume
    from it is refused."""
    store = StampedOffsetStore(StreamStore(), stamp=lambda _path: "x" * 230)
    store.create("/s")
    with pytest.raises(ValueError, match="too long"):
        store.get("/s")


def test_a_stamp_is_not_asked_for_an_absent_stream() -> None:
    """An absent stream is reported as absent, whatever the app's stamp does."""
    asked: list[str] = []

    def stamp(path: str) -> str:
        asked.append(path)
        return "app1"

    store = StampedOffsetStore(StreamStore(), stamp=stamp)
    assert store.get("/nope") is None
    with pytest.raises(StreamNotFound):
        store.read("/nope", "-1")
    assert asked == []


def test_a_close_racing_the_streams_creation_still_answers_a_stamped_offset() -> None:
    inner = CreatedWhileStamping()
    store = StampedOffsetStore(inner, stamp=lambda _path: "app1")

    result = store.close_stream("/s")

    assert result is not None
    assert result.final_offset.startswith(f"{inner.get_generation('/s')}.app1_")


def test_the_longest_stamp_allowed_leaves_room_for_every_stores_own_offset() -> None:
    """157 characters is the most a stamp may be: with a 32-character generation
    and the in-memory store's 33-character offsets the result stays under 256."""
    for length, allowed in ((157, True), (158, False)):
        store = StampedOffsetStore(StreamStore(), stamp=lambda _path, n=length: "x" * n)
        store.create("/s")
        if not allowed:
            with pytest.raises(ValueError, match="too long"):
                store.get("/s")
            continue
        store.append("/s", b"x")
        stream = store.get("/s")
        assert stream is not None
        assert len(stream.current_offset) < 256
