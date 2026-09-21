"""The event time rakaia stamps is taken under the offset lock, and never goes
backwards within a stream (#284).

`@stream_model` used to read `time.time()` before `write_enveloped_event` took
the offset lock, so two saves racing for one stream could get positions in one
order and times in the other -- and a clock stepped backwards dated a later
event before an earlier one outright. That matters to
`merge_replay(order_key=ENVELOPE_TS)`, which interleaves streams by this time.

The stamp is now `max(the stream's last stamp, now)`, advanced on the offset
watermark in the same locked save as the high mark. These tests drive the clock
by hand, so they run on SQLite: what they pin is the clamp, not the lock, and
the lock is `test_concurrent_appends.py`'s job.

A producer-supplied `event_ts` is outside the rule on purpose: stored verbatim,
and not recorded as the stream's last stamp.
"""

from __future__ import annotations

import pytest
from django.db import transaction

from django_rakaia.decorators import create_stream_event
from django_rakaia.django_store import DjangoStreamStore, write_enveloped_event
from django_rakaia.models import Stream, StreamOffsetWatermark

from .models import AreaData

pytestmark = pytest.mark.django_db

T0 = 2_000_000_000.0


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> list[float]:
    now = [T0]
    monkeypatch.setattr("time.time", lambda: now[0])
    return now


class _Instance:
    """Stand-in for a model instance; `create_stream_event` only passes it on."""


def _save(paths: str | list[str]) -> float:
    """One `@stream_model`-style save; returns the event time it was given."""
    event = create_stream_event(
        stream_paths=paths,
        to_dataclass=lambda _i: AreaData(id=1, name="a"),
        instance=_Instance(),  # type: ignore[arg-type]
        action="update",
    )
    assert event.event_ts is not None
    return event.event_ts


def test_a_save_after_the_clock_steps_back_is_not_dated_earlier(
    clock: list[float],
) -> None:
    first = _save("areas")
    clock[0] -= 100
    second = _save("areas")
    clock[0] += 150
    third = _save("areas")
    assert (first, second, third) == (T0, T0, T0 + 50)


@pytest.mark.usefixtures("clock")
def test_the_stamp_is_recorded_beside_the_high_mark() -> None:
    _save("areas")
    watermark = StreamOffsetWatermark.objects.get(stream_path="areas")
    assert (watermark.high, watermark.last_event_ts) == (1, T0)


def test_a_fan_out_brings_every_stream_up_to_the_events_time(
    clock: list[float],
) -> None:
    # `ahead` has already seen T0; `behind` has not. One event lands in both
    # with the clock stepped back, so it takes `ahead`'s time -- and `behind`
    # must now remember that time too, or its next event could be dated
    # before this one.
    _save("ahead")
    clock[0] -= 100
    shared = _save(["ahead", "behind"])
    assert shared == T0
    clock[0] -= 10
    assert _save("behind") == T0


@pytest.mark.usefixtures("clock")
def test_a_save_into_no_streams_still_gets_a_time() -> None:
    assert _save([]) == T0


@pytest.mark.django_db(transaction=True, databases=["default", "overlay"])
def test_a_fan_out_brings_lagging_streams_up_on_their_own_database(
    clock: list[float],
) -> None:
    # The catch-up write must follow the streams to their alias; sent to the
    # default database it would update nothing, and `behind` would stay behind.
    ahead = Stream.objects.using("overlay").create(stream_id="ahead")
    behind = Stream.objects.using("overlay").create(stream_id="behind")
    # Callers hold a transaction on the streams' alias; the offset lock needs one.
    with transaction.atomic(using="overlay"):
        write_enveloped_event([ahead], {"id": 1}, stamp_event_ts=True)
    clock[0] -= 100
    with transaction.atomic(using="overlay"):
        write_enveloped_event([ahead, behind], {"id": 2}, stamp_event_ts=True)
    watermark = StreamOffsetWatermark.objects.using("overlay").get(stream_path="behind")
    assert watermark.last_event_ts == T0


@pytest.mark.usefixtures("clock")
def test_a_supplied_time_is_kept_and_does_not_move_the_stamp() -> None:
    stream = Stream.objects.create(stream_id="areas")
    event, _ = write_enveloped_event(
        [stream], {"id": 1}, event_ts=3_000_000_000.0, stamp_event_ts=True
    )
    assert event.event_ts == 3_000_000_000.0
    assert _save("areas") == T0


@pytest.mark.usefixtures("clock")
def test_a_raw_protocol_append_still_stores_no_time() -> None:
    # The durable store's protocol append is not stamped: NULL means "the
    # producer set no time", and readers fall back to the insert time.
    store = DjangoStreamStore()
    store.create("raw")
    store.append("raw", b'{"id": 1}')
    event = Stream.objects.get(stream_id="raw").entries.get().event
    assert event.event_ts is None
