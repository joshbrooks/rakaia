"""Executable proof for the django_rakaia findings of the 2026-08-17 architecture review.

Same contract as `tests/test_rakaia/test_architecture_spikes.py`: every test here
**fails today**, is marked `xfail(strict=True)`, and asserts a property the code
already claims somewhere — in a docstring, in `docs/glossary.md`, or in a sibling
implementation. When a finding is fixed its test reports XPASS(strict), which is
a failure telling you to delete the marker.

Epic: #152.
"""

from __future__ import annotations

import base64
from typing import Any

import pytest
from django.db import transaction

from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.event_message import decode_payload
from django_rakaia.models import StreamEntry, StreamEvent
from rakaia.types import AppendOptions

pytestmark = pytest.mark.django_db


class _RecordingChannelLayer:
    """A channel layer that records what was published, and when.

    `broadcast_entries` is documented as a no-op when the channel layer is
    absent, so `get_channel_layer` is already a seam — this is its second
    adapter.
    """

    def __init__(self) -> None:
        self.sent: list[tuple[str, dict[str, Any]]] = []

    async def group_send(self, group: str, message: dict[str, Any]) -> None:
        self.sent.append((group, message))


@pytest.fixture
def recording_layer(monkeypatch: pytest.MonkeyPatch) -> _RecordingChannelLayer:
    layer = _RecordingChannelLayer()
    import django_rakaia.channels_signals as cs

    monkeypatch.setattr(cs, "get_channel_layer", lambda: layer)
    return layer


# ---------------------------------------------------------------------------
# Finding 1 — "a stored row as an event" has six implementations
# ---------------------------------------------------------------------------


def test_a_subscriber_and_a_reader_see_the_same_event_label(
    recording_layer: _RecordingChannelLayer,
) -> None:
    """The envelope label means the same thing to every reader of the log.

    `store.read()` inverts the `"append"` sentinel to `""` -- an append carries
    no envelope label -- and documents that it does so to match the in-memory
    store. The SSE frame skips that inversion, so the same event is described
    two different ways depending on which door you came through.
    """
    store = DjangoStreamStore()
    store.create("labels")
    store.append("labels", b'{"x": 1}')

    message = store.read("labels")[0][0]
    assert recording_layer.sent, "nothing was published"
    _group, frame = recording_layer.sent[-1]

    assert frame["event"]["event_type"] == message.label


def test_a_subscriber_can_recover_the_payload_a_reader_sees(
    recording_layer: _RecordingChannelLayer,
) -> None:
    """A subscriber must be able to reconstruct exactly what `read()` returns.

    The frame is JSON on the wire, so a payload that is not valid UTF-8 cannot
    ride in it as bytes -- which is the root of #153. The old frame published
    the stored column and told the subscriber nothing, so base64 was
    indistinguishable from text that happened to look like base64. The frame now
    carries the encoding alongside the value, so the bytes are recoverable.
    """
    payload = b"\xff\xfe binary, not utf-8"
    store = DjangoStreamStore()
    store.create("payloads", content_type="application/octet-stream")
    store.append("payloads", payload)

    message = store.read("payloads")[0][0]
    assert message.data == payload, "precondition: read() returns the original bytes"

    _group, frame = recording_layer.sent[-1]
    published = frame["event"]["data"]
    encoding = frame["event"].get("payload_encoding")

    assert encoding == "base64", "the subscriber is told how to read the value"
    assert base64.b64decode(published) == message.data


@pytest.mark.parametrize(
    ("content_type", "payload"),
    [
        # The case the first cut of the fix got wrong: a text body that happens
        # to parse as JSON. Decoding the stored value and re-deriving an
        # encoding from the bytes republished these as JSON values, so the
        # subscriber reconstructed `{"a": 1}`, `1.5` and `7` -- losing the
        # newline, the trailing zero and the surrounding spaces `read()` keeps.
        ("text/plain", b'{"a": 1}\n'),
        ("text/plain", b'{"a":  1}'),
        ("text/plain", b"1.50"),
        ("text/plain", b"  7  "),
        # The cases that already worked, kept here so the law is stated once
        # rather than per storage shape.
        ("text/plain", b"an ordinary line"),
        ("application/octet-stream", b"\xff\xfe binary, not utf-8"),
        (None, b'{"x": 1}'),
    ],
)
def test_a_subscriber_reconstructs_a_readers_bytes_for_any_payload(
    recording_layer: _RecordingChannelLayer,
    content_type: str | None,
    payload: bytes,
) -> None:
    """`decode_payload` over the frame yields exactly what `read()` returns.

    The one law the frame owes a subscriber, stated for every storage shape at
    once rather than per shape. The frame cannot carry bytes, so it carries the
    stored `data`/`payload_encoding` pair and the subscriber runs the same
    inverse a reader does -- which only holds if the frame passes the stored
    pair through rather than decoding and guessing an encoding back (#153).
    """
    store = DjangoStreamStore()
    store.create("roundtrip", content_type=content_type)
    store.append("roundtrip", payload)

    read_bytes = store.read("roundtrip")[0][0].data
    assert read_bytes == payload, "precondition: read() is byte-exact"

    _group, frame = recording_layer.sent[-1]
    recovered = decode_payload(
        frame["event"]["data"], frame["event"].get("payload_encoding")
    )

    assert recovered == read_bytes


def test_a_json_subscriber_still_receives_the_plain_json_value(
    recording_layer: _RecordingChannelLayer,
) -> None:
    """The common case is unchanged, and stays unencoded.

    Carrying the encoding must not push ordinary JSON payloads through base64 --
    that would be a wire-format break for every existing subscriber.
    """
    store = DjangoStreamStore()
    store.create("plain")
    store.append("plain", b'{"x": 1}')

    _group, frame = recording_layer.sent[-1]

    assert frame["event"]["data"] == {"x": 1}
    assert "payload_encoding" not in frame["event"], (
        "an ordinary JSON payload must keep exactly the frame it always had"
    )


# ---------------------------------------------------------------------------
# Finding 2 — the batch door skips admission
# ---------------------------------------------------------------------------


def test_a_batch_from_a_fenced_producer_is_refused() -> None:
    """Producer fencing is a property of the stream, not of which door you used.

    A newer epoch has taken over the stream; the displaced producer must not be
    able to write. `append` enforces this through `decide_append`; `append_many`
    does not consult it at all.
    """
    store = DjangoStreamStore()
    store.create("fenced")

    # Epoch 2 takes the stream. A producer's first sequence is 0, so this
    # append is genuinely accepted and the epoch is genuinely established --
    # an earlier version of this test started at 1, which was itself refused
    # for a sequence gap and so proved less than it claimed.
    taken = store.append(
        "fenced",
        b'{"n": 1}',
        AppendOptions(producer_id="p", producer_epoch=2, producer_seq=0),
    )
    assert taken.message is not None, "precondition: epoch 2 holds the stream"

    # The displaced epoch-1 producer now tries a batch.
    results = store.append_many(
        "fenced",
        [
            (
                b'{"n": 99}',
                AppendOptions(producer_id="p", producer_epoch=1, producer_seq=1),
            )
        ],
    )

    assert results[0].message is None, (
        "a stale-epoch batch was written; producer fencing does not cover append_many"
    )


# ---------------------------------------------------------------------------
# Finding 5 — publication happens before commit
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason=(
        "FINDING 5 (#157): both publish paths run inside transaction.atomic() "
        "and there is no on_commit anywhere in src/, so a rolled-back append "
        "has already told every subscriber about an event that does not exist."
    ),
)
@pytest.mark.django_db(transaction=True)
def test_a_rolled_back_append_is_never_published(
    recording_layer: _RecordingChannelLayer,
) -> None:
    """Subscribers are told about events, and a rolled-back append is not one.

    The log is the source of truth. If the row is not in it, nothing downstream
    should believe it ever was.
    """
    store = DjangoStreamStore()
    store.create("rollback")
    recording_layer.sent.clear()

    class Rollback(Exception):
        pass

    with pytest.raises(Rollback), transaction.atomic():
        store.append("rollback", b'{"doomed": true}')
        raise Rollback

    assert StreamEntry.objects.filter(stream__stream_id="rollback").count() == 0
    assert recording_layer.sent == [], (
        f"{len(recording_layer.sent)} frame(s) published for an append that "
        "was rolled back"
    )


# ---------------------------------------------------------------------------
# Finding 7 — the alias seam stops half-way
# ---------------------------------------------------------------------------


@pytest.mark.django_db(databases=["default", "overlay"])
def test_a_save_to_another_database_records_its_event_there() -> None:
    """A save and the event it emits are one write, so they share a database.

    `assert_no_live_writes` names "a receiver that saves without a `using=`" as
    the exact leak it exists to detect -- and the receiver in question is this
    package's own `@stream_model`.
    """
    from .models import Area

    before_default = StreamEvent.objects.using("default").count()

    area = Area(name="overlay-area")
    area.save(using="overlay")

    after_default = StreamEvent.objects.using("default").count()
    overlay_events = StreamEvent.objects.using("overlay").count()

    assert after_default == before_default, (
        "a save routed to 'overlay' wrote its stream event to 'default'"
    )
    assert overlay_events == 1

    # The whole write has to land together, not just the event row. The entry
    # and the offset high-water are part of the same save.
    from django_rakaia.models import StreamEntry as _Entry
    from django_rakaia.models import StreamOffsetWatermark as _Watermark

    path = f"area:{area.id}:projects"
    assert _Entry.objects.using("overlay").filter(stream__stream_id=path).count() == 1
    assert _Watermark.objects.using("overlay").filter(stream_path=path).exists(), (
        "the offset high-water was not allocated on 'overlay'"
    )
    assert not _Watermark.objects.using("default").filter(stream_path=path).exists(), (
        "offsets for an 'overlay' stream were allocated from the 'default' "
        "high-water -- a rebuild would read its offsets from the live database"
    )


# ---------------------------------------------------------------------------
# Finding 8 — the skip rule and the diff rule can disagree
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason=(
        "FINDING 8 (#160): canonical_value lives in verification.py and takes "
        "its normalizers as an argument, but DjangoExecutor(skip_unchanged=True) "
        "can only use DEFAULT_NORMALIZERS -- there is no way to construct an "
        "executor that agrees with a diff run under custom normalizers. Both "
        "docstrings flag the divergence; neither can close it from where it sits."
    ),
)
def test_the_skip_rule_can_be_given_the_normalizers_a_diff_uses() -> None:
    """ "Unchanged" must mean one thing on the write path and the verify path.

    `diff_effects_against_rows` accepts `normalizers=`; the executor's
    `skip_unchanged` comparison does not. So a projection verified as identical
    under one equality rule is rewritten under another.
    """
    import inspect

    from django_rakaia.effect_executor import DjangoExecutor

    parameters = inspect.signature(DjangoExecutor.__init__).parameters
    assert "normalizers" in parameters, (
        "DjangoExecutor cannot be given the normalizer set a diff uses, so the "
        "two equality rules cannot be made to agree"
    )


def test_a_partly_fenced_batch_keeps_one_result_per_item() -> None:
    """Refusing an item must not shift every later item's answer onto the wrong input.

    `append_many` promises one result per input item, in input order. Now that
    the fence can refuse an individual item mid-batch, the refusal has to keep
    its slot -- dropping it would silently re-pair each subsequent result with
    the wrong item (#154).
    """
    store = DjangoStreamStore()
    store.create("mixed")
    store.append(
        "mixed",
        b'{"n": 0}',
        AppendOptions(producer_id="p", producer_epoch=2, producer_seq=0),
    )

    results = store.append_many(
        "mixed",
        [
            # accepted: unfenced
            (b'{"n": 1}', None),
            # refused: epoch 1 is stale, epoch 2 holds the stream
            (
                b'{"n": 2}',
                AppendOptions(producer_id="p", producer_epoch=1, producer_seq=9),
            ),
            # accepted: the current epoch, advancing its sequence
            (
                b'{"n": 3}',
                AppendOptions(producer_id="p", producer_epoch=2, producer_seq=1),
            ),
        ],
    )

    assert len(results) == 3
    assert results[0].message is not None
    assert results[1].message is None, "the stale-epoch item must be refused"
    assert results[2].message is not None
    assert results[0].message.data == b'{"n": 1}'
    assert results[2].message.data == b'{"n": 3}', (
        "the accepted item after a refusal was paired with the wrong input"
    )


def test_a_batch_advances_producer_state_for_the_next_writer() -> None:
    """Fencing state established by a batch must fence what comes after it.

    Asking the fence but never recording the answer leaves the next write judged
    against the pre-batch state. The visible consequence is not just a
    mis-refused replay -- it is that a producer which legitimately continues
    after its own batch is rejected for a sequence gap it did not create.
    """
    from django_rakaia.models import StreamProducer

    store = DjangoStreamStore()
    store.create("advance")

    store.append_many(
        "advance",
        [
            (
                b'{"n": 1}',
                AppendOptions(producer_id="p", producer_epoch=1, producer_seq=0),
            ),
            (
                b'{"n": 2}',
                AppendOptions(producer_id="p", producer_epoch=1, producer_seq=1),
            ),
        ],
    )

    row = StreamProducer.objects.get(producer_id="p")
    assert (row.epoch, row.last_seq) == (1, 1), (
        "the batch did not record the fencing state it established"
    )

    # Re-sending the batch's last item is a duplicate, not a gap.
    replayed = store.append(
        "advance",
        b'{"n": 2 again}',
        AppendOptions(producer_id="p", producer_epoch=1, producer_seq=1),
    )
    assert replayed.message is None
    assert replayed.producer_result is not None
    assert replayed.producer_result.status == "duplicate", (
        f"expected a duplicate, got {replayed.producer_result.status} -- the "
        "next writer is being judged against the pre-batch state"
    )

    # And the producer can carry on from where its batch left off.
    carried_on = store.append(
        "advance",
        b'{"n": 3}',
        AppendOptions(producer_id="p", producer_epoch=1, producer_seq=2),
    )
    assert carried_on.message is not None, (
        "a producer continuing after its own batch was refused"
    )
