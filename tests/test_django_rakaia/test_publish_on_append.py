"""An append reaches subscribers because the *store* published it (issue #82).

`append_many`'s docstring promises it is "semantically identical to calling
`append` once per item". That is true of the rows it writes and false of the
subscribers it reaches, because the two stores implement "an append reaches
subscribers" by different mechanisms and only one of them is named anywhere:

* the in-memory store publishes **inside** `append`;
* the durable store did not publish at all — publication was a `post_save`
  receiver on `StreamEntry`, and `append_many` writes with `bulk_create`, which
  does not fire `post_save`.

So every bulk append was invisible to SSE subscribers. The semantics that broke
were never in the interface, which is why nothing caught it: no docstring claimed
them, no contract asserted them, and the row-level behaviour the docstring *did*
describe was correct throughout.

The fix is to make publication part of what a store does when it appends, rather
than a side effect of how it happened to write rows. These tests pin that: they
drive the **store**, not the ORM, and assert what a subscriber receives.
"""

from __future__ import annotations

import asyncio
import contextlib
import json

import pytest
from channels.layers import get_channel_layer

from django_rakaia.django_store import DjangoStreamStore
from rakaia import AppendOptions

pytestmark = pytest.mark.django_db(transaction=True)

GROUP = "stream.bulk"
PATH = "bulk"


async def _subscribe() -> tuple[object, str]:
    layer = get_channel_layer()
    assert layer is not None
    channel = await layer.new_channel()
    await layer.group_add(GROUP, channel)
    return layer, channel


async def _drain(layer, channel, *, expected: int, timeout: float = 3.0) -> list[dict]:
    """Collect up to `expected` frames, giving up quickly rather than hanging."""
    received: list[dict] = []

    async def _consume():
        while len(received) < expected:
            received.append(await layer.receive(channel))

    with contextlib.suppress(TimeoutError):
        await asyncio.wait_for(_consume(), timeout=timeout)
    return received


def _offsets(frames: list[dict]) -> list[str]:
    out = []
    for frame in frames:
        event = frame.get("event") or {}
        out.append(str(event.get("offset")))
    return out


class TestAppendPublishes:
    """The case that already worked — kept so the fix is a widening, not a swap."""

    async def test_a_single_append_reaches_a_subscriber(self):
        layer, channel = await _subscribe()
        store = DjangoStreamStore()

        await store.run_sync(store.create, PATH)
        await store.run_sync(
            store.append, PATH, b'{"n": 1}', AppendOptions(label="create")
        )

        frames = await _drain(layer, channel, expected=1)
        assert len(frames) == 1


class TestAppendManyPublishes:
    """The RED core: zero frames today."""

    async def test_a_bulk_append_reaches_a_subscriber(self):
        layer, channel = await _subscribe()
        store = DjangoStreamStore()

        await store.run_sync(store.create, PATH)
        await store.run_sync(
            store.append_many,
            PATH,
            [
                (b'{"n": 1}', AppendOptions(label="create")),
                (b'{"n": 2}', AppendOptions(label="update")),
                (b'{"n": 3}', AppendOptions(label="update")),
            ],
        )

        frames = await _drain(layer, channel, expected=3)
        assert len(frames) == 3, (
            f"bulk append reached {len(frames)} subscriber(s), expected 3 — "
            "bulk_create does not fire post_save"
        )

    async def test_every_appended_event_is_delivered_in_order(self):
        layer, channel = await _subscribe()
        store = DjangoStreamStore()

        await store.run_sync(store.create, PATH)
        await store.run_sync(
            store.append_many,
            PATH,
            [
                (json.dumps({"n": n}).encode(), AppendOptions(label="u"))
                for n in range(3)
            ],
        )

        frames = await _drain(layer, channel, expected=3)
        payloads = [f["event"]["data"]["n"] for f in frames]
        assert payloads == [0, 1, 2]

    async def test_offsets_are_the_ones_the_store_assigned(self):
        """The frame carries the raw entry offset, matching the shape the
        `post_save` receiver has always sent — not the zero-padded protocol
        form `read()` returns. Pinned so the bulk path cannot drift from the
        single-append path that live SSE consumers already parse."""
        from django_rakaia.models import StreamEntry

        layer, channel = await _subscribe()
        store = DjangoStreamStore()

        await store.run_sync(store.create, PATH)
        await store.run_sync(
            store.append_many,
            PATH,
            [(b'{"n": 1}', AppendOptions(label="u")) for _ in range(2)],
        )

        frames = await _drain(layer, channel, expected=2)
        assigned = await store.run_sync(
            lambda: list(
                StreamEntry.objects.filter(stream__stream_id=PATH)
                .order_by("offset")
                .values_list("offset", flat=True)
            )
        )
        assert _offsets(frames) == [str(o) for o in assigned]

    async def test_an_empty_batch_publishes_nothing(self):
        layer, channel = await _subscribe()
        store = DjangoStreamStore()

        await store.run_sync(store.create, PATH)
        await store.run_sync(store.append_many, PATH, [])

        assert await _drain(layer, channel, expected=1, timeout=0.3) == []


class TestParityBetweenTheTwoPaths:
    """The invariant `append_many`'s docstring claims, now asserted."""

    async def test_one_bulk_append_delivers_what_n_single_appends_deliver(self):
        layer, channel = await _subscribe()
        store = DjangoStreamStore()

        await store.run_sync(store.create, "singles")
        await store.run_sync(store.create, "bulk")

        # Only the `bulk` path is subscribed; count frames for each in turn.
        await store.run_sync(
            store.append_many,
            PATH,
            [(b'{"n": 1}', AppendOptions(label="u")) for _ in range(2)],
        )
        bulk_frames = await _drain(layer, channel, expected=2)

        for _ in range(2):
            await store.run_sync(
                store.append, PATH, b'{"n": 1}', AppendOptions(label="u")
            )
        single_frames = await _drain(layer, channel, expected=2)

        assert len(bulk_frames) == len(single_frames) == 2
