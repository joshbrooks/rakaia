"""A stream's stored events reshaped on the way out, per stream.

Partisipa's translations stream stores events like ``translation:set``, but a
device wants State Protocol change messages (``insert``, ``update``,
``delete``). `ServerOptions.reshape_events` maps a stream path to a function
from one stored JSON event to what is sent for it, or None to send nothing; the
event's offset still counts, so a reader moves past it.
"""

from __future__ import annotations

import json
from typing import Any

from rakaia import StreamStore
from tests.asgi_client import asgi_client

JSON = {"content-type": "application/json"}


def change_message(event: dict[str, Any]) -> dict[str, Any] | None:
    """The spike's translation event to change message (partisipa-import#3518)."""
    if event["type"] not in ("translation:set", "translation:cleared"):
        return None
    key = f"{event['langcode']}|{event['msgid']}"
    if event["type"] == "translation:cleared":
        return {"type": "translation", "key": key, "headers": {"operation": "delete"}}
    operation = "update" if event.get("replaced") is not None else "insert"
    return {
        "type": "translation",
        "key": key,
        "value": {"msgid": event["msgid"], "msgstr": event["msgstr"]},
        "headers": {"operation": operation},
    }


SET = {"type": "translation:set", "langcode": "tet", "msgid": "Yes", "msgstr": "Sin"}
REPLACED = {**SET, "msgstr": "Loos", "replaced": "Sin"}
OTHER = {"type": "translation:audited", "langcode": "tet", "msgid": "Yes"}


def _client(**options: Any) -> Any:
    return asgi_client(
        StreamStore(), reshape_events={"/translations": change_message}, **options
    )


async def _append(client: Any, *events: dict[str, Any]) -> None:
    await client.put("/translations", headers=JSON)
    for event in events:
        await client.post("/translations", content=json.dumps(event), headers=JSON)


async def test_a_translation_set_event_is_served_as_an_insert_or_update() -> None:
    async with _client() as client:
        await _append(client, SET, REPLACED)
        read = await client.get("/translations", params={"offset": "-1"})

    assert read.status_code == 200
    assert [m["headers"]["operation"] for m in read.json()] == ["insert", "update"]
    assert read.json()[1]["value"] == {"msgid": "Yes", "msgstr": "Loos"}


async def test_an_event_reshaped_to_nothing_is_skipped_but_its_offset_counts() -> None:
    async with _client() as client:
        await _append(client, SET, OTHER)
        read = await client.get("/translations", params={"offset": "-1"})
        tail = await client.get("/translations", params={"offset": "now"})

    assert [m["headers"]["operation"] for m in read.json()] == ["insert"]
    assert read.headers["stream-next-offset"] == tail.headers["stream-next-offset"]


async def test_a_page_of_skipped_events_still_moves_the_reader_on() -> None:
    """A page whose events are all skipped answers an empty list, and its next
    offset is past them, not the tail, so the next page is still read."""
    async with _client(read_page_size=2) as client:
        await _append(client, OTHER, OTHER, SET)
        first = await client.get("/translations", params={"offset": "-1"})
        second = await client.get(
            "/translations", params={"offset": first.headers["stream-next-offset"]}
        )

    assert first.json() == []
    assert "stream-up-to-date" not in first.headers
    assert [m["headers"]["operation"] for m in second.json()] == ["insert"]


async def test_other_streams_are_served_as_stored() -> None:
    async with _client() as client:
        await client.put("/other", headers=JSON)
        await client.post("/other", content=json.dumps(SET), headers=JSON)
        read = await client.get("/other", params={"offset": "-1"})

    assert read.json() == [SET]


async def test_sse_sends_change_messages_and_moves_past_skipped_events() -> None:
    # No keep-alive before the connection ends, so the last control event is
    # the catch-up's own, which must carry the reader past the skipped event.
    async with _client(long_poll_timeout=5, sse_max_seconds=0.2) as client:
        await _append(client, SET, OTHER)
        tail = (await client.get("/translations", params={"offset": "now"})).headers[
            "stream-next-offset"
        ]
        read = await client.get("/translations", params={"offset": "-1", "live": "sse"})

    blocks = [b for b in read.text.split("\n\n") if b.strip()]
    data = [
        json.loads(line.removeprefix("data:"))
        for b in blocks
        if b.startswith("event: data")
        for line in b.splitlines()
        if line.startswith("data:")
    ]
    controls = [
        json.loads(line.removeprefix("data:"))
        for b in blocks
        if b.startswith("event: control")
        for line in b.splitlines()
        if line.startswith("data:")
    ]
    assert [m["headers"]["operation"] for batch in data for m in batch] == ["insert"]
    assert controls[-1]["streamNextOffset"] == tail


async def test_sse_moves_past_a_page_whose_last_event_is_skipped() -> None:
    """A page cut short after skipped events must tell the reader the offset of
    the last of them, not the tail, or a reconnect would skip the next page."""
    async with _client(
        read_page_size=2, long_poll_timeout=5, sse_max_seconds=0.3
    ) as client:
        await _append(client, OTHER, OTHER, SET)
        tail = (await client.get("/translations", params={"offset": "now"})).headers[
            "stream-next-offset"
        ]
        read = await client.get("/translations", params={"offset": "-1", "live": "sse"})

    blocks = [b for b in read.text.split("\n\n") if b.strip()]
    first_data = next(i for i, b in enumerate(blocks) if b.startswith("event: data"))
    told_before_the_insert = [
        json.loads(line.removeprefix("data:"))["streamNextOffset"]
        for b in blocks[:first_data]
        if b.startswith("event: control")
        for line in b.splitlines()
        if line.startswith("data:")
    ]
    data = json.loads(blocks[first_data].splitlines()[1].removeprefix("data:"))
    assert told_before_the_insert
    assert tail not in told_before_the_insert
    assert [m["headers"]["operation"] for m in data] == ["insert"]


async def test_reshaped_reads_are_not_cached() -> None:
    """A changed function must not leave an old-shaped body in a cache that
    revalidates by ETag."""
    async with _client() as client:
        await _append(client, SET)
        reshaped = await client.get("/translations", params={"offset": "-1"})
        await client.put("/other", headers=JSON)
        await client.post("/other", content=json.dumps(SET), headers=JSON)
        as_stored = await client.get("/other", params={"offset": "-1"})

    assert reshaped.headers.get("cache-control") == "no-store"
    assert as_stored.headers.get("cache-control") != "no-store"


async def test_a_stream_that_is_not_json_is_sent_as_stored() -> None:
    async with asgi_client(
        StreamStore(), reshape_events={"/bytes": change_message}
    ) as client:
        await client.put("/bytes", headers={"content-type": "application/octet-stream"})
        await client.post(
            "/bytes",
            content=b"raw",
            headers={"content-type": "application/octet-stream"},
        )
        read = await client.get("/bytes", params={"offset": "-1"})

    assert read.content == b"raw"
