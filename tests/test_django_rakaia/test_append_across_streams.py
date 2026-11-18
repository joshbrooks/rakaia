"""Appends to two streams inside one caller-owned transaction land or vanish together.

There is no single call that appends several events to several streams, so a
caller who needs that wraps separate `append`/`append_many` calls in its own
`transaction.atomic()`. This holds the store to that: when the second append
raises, or the caller raises after both, every row the first one wrote rolls back
with it — events, entries, tags, the offset watermark, the stream's `last_seq`
and its producer state — and nothing reaches a live subscriber.

`transaction=True`, so the outer `atomic()` is the test's own and not one
pytest-django opened and would roll back anyway.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import patch

import pytest
from django.db import transaction

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.models import (
    Stream,
    StreamEntry,
    StreamEvent,
    StreamEventTag,
    StreamOffsetWatermark,
    StreamProducer,
)
from rakaia.types import AppendOptions, SequenceConflict

pytestmark = pytest.mark.django_db(transaction=True)


class _Boom(Exception):
    pass


def _snapshot(using: str = "default") -> dict[str, Any]:
    """Every row an append can write or change, in a comparable form."""
    return {
        "events": sorted(
            StreamEvent.objects.using(using).values_list("id", "event_type")
        ),
        "entries": sorted(
            StreamEntry.objects.using(using).values_list("stream__stream_id", "offset")
        ),
        "tags": sorted(
            StreamEventTag.objects.using(using).values_list("event_id", "tag")
        ),
        "watermarks": sorted(
            StreamOffsetWatermark.objects.using(using).values_list(
                "stream_path", "high", "last_event_ts"
            )
        ),
        "streams": sorted(
            Stream.objects.using(using).values_list("stream_id", "last_seq")
        ),
        "producers": sorted(
            StreamProducer.objects.using(using).values_list(
                "stream__stream_id", "producer_id", "epoch", "last_seq"
            )
        ),
    }


def _opts(seq: str, producer_seq: int, label: str) -> AppendOptions:
    return AppendOptions(
        seq=seq,
        producer_id="p",
        producer_epoch=0,
        producer_seq=producer_seq,
        label=label,
        tags=("t",),
    )


def _seeded(using: str | None = None) -> DjangoStreamStore:
    """Two streams, each with one committed append, so every row exists."""
    store = DjangoStreamStore(using=using)
    store.create("submission:1")
    store.create("projects")
    store.append("submission:1", b'{"v": 0}', _opts("a", 0, "seeded"))
    store.append("projects", b'{"v": 0}', _opts("a", 0, "seeded"))
    return store


@pytest.fixture
def store() -> DjangoStreamStore:
    return _seeded()


@pytest.fixture
def no_broadcast():
    """Fail on any frame sent to a live subscriber."""
    sent: list[tuple[str, dict]] = []

    class _Layer:
        async def group_send(self, group: str, frame: dict) -> None:
            sent.append((group, frame))

    with patch(
        "django_rakaia.channels_signals.get_channel_layer", return_value=_Layer()
    ):
        yield sent


def test_second_append_refused_rolls_back_the_first(store, no_broadcast):
    before = _snapshot()

    with pytest.raises(SequenceConflict), transaction.atomic():
        store.append("submission:1", b'{"v": 1}', _opts("b", 1, "edited"))
        # A `Stream-Seq` that does not move forward is refused by the store.
        store.append("projects", b'{"v": 1}', _opts("a", 1, "registered"))

    assert _snapshot() == before
    assert no_broadcast == []


def test_caller_raising_after_both_appends_rolls_back_both(store, no_broadcast):
    before = _snapshot()

    with pytest.raises(_Boom), transaction.atomic():
        store.append_many(
            "submission:1",
            [(b'{"v": 1}', _opts("b", 1, "edited")), (b'{"v": 2}', _opts("c", 2, "x"))],
        )
        store.append("projects", b'{"v": 1}', _opts("b", 1, "registered"))
        raise _Boom

    assert _snapshot() == before
    assert no_broadcast == []


def test_a_new_streams_first_append_rolls_back_its_watermark(store):
    """The watermark row is created on a path's first append; it must go too."""
    store.create("fresh")
    before = _snapshot()
    assert not StreamOffsetWatermark.objects.filter(stream_path="fresh").exists()

    with pytest.raises(_Boom), transaction.atomic():
        store.append("fresh", b'{"v": 1}', _opts("a", 0, "first"))
        store.append("projects", b'{"v": 1}', _opts("b", 1, "registered"))
        raise _Boom

    assert _snapshot() == before
    store.append("fresh", b'{"v": 1}')
    assert StreamEntry.objects.get(stream__stream_id="fresh").offset == 1


def test_both_appends_land_when_nothing_raises(store, no_broadcast):
    """The control: the same calls commit, so the rollbacks above are not vacuous."""
    before = _snapshot()

    with transaction.atomic():
        store.append("submission:1", b'{"v": 1}', _opts("b", 1, "edited"))
        store.append("projects", b'{"v": 1}', _opts("b", 1, "registered"))

    after = _snapshot()
    for key in before:
        assert after[key] != before[key], key
    assert len(no_broadcast) == 2


@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
def test_a_store_on_another_alias_rolls_back_with_an_atomic_on_that_alias():
    store = _seeded("overlay")
    before = _snapshot("overlay")

    with pytest.raises(_Boom), transaction.atomic(using="overlay"):
        store.append("submission:1", b'{"v": 1}', _opts("b", 1, "edited"))
        store.append("projects", b'{"v": 1}', _opts("b", 1, "registered"))
        raise _Boom

    assert _snapshot("overlay") == before


@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
def test_an_atomic_on_the_wrong_alias_does_not_hold_the_appends():
    """The trap: a bare `atomic()` binds to `default`, so it cannot roll back a
    store on another alias; each append commits on its own as it returns."""
    store = _seeded("overlay")
    before = _snapshot("overlay")

    with pytest.raises(_Boom), transaction.atomic():
        store.append("submission:1", b'{"v": 1}', _opts("b", 1, "edited"))
        raise _Boom

    assert _snapshot("overlay")["entries"] != before["entries"]
