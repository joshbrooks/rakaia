"""The producer status/header table, as a table.

A refused fenced write turns into a specific HTTP response: 204 for a duplicate,
403 for a stale epoch, 400 for a bad epoch opening, 409 for a gap or a closed
stream — each with its own headers. That mapping was written out three times in
`_handle_append`, once for the close-only path, once for the closed-stream path
and once for the append-result path, in the largest function of the largest file.
Roughly a hundred lines saying the same thing, where two of the copies were
byte-identical for three of the five arms and the third differed in ways that
were hard to tell were deliberate.

The exception half of this mapping was already deepened: commit `168c5e2`
replaced substring-matched error strings with named failures and a single
`_status_for` lookup. The union-typed half never followed, so the two halves of
"what status does this become" lived in completely different shapes.

Before this, asserting "a sequence gap returns 409 with `Producer-Expected-Seq`"
meant a full ASGI round trip against a live store. Here it is a function call.
"""

from __future__ import annotations

import pytest

from rakaia.handler import producer_response
from rakaia.types import (
    PRODUCER_EPOCH_HEADER,
    PRODUCER_EXPECTED_SEQ_HEADER,
    PRODUCER_RECEIVED_SEQ_HEADER,
    PRODUCER_SEQ_HEADER,
    ProducerAccepted,
    ProducerDuplicate,
    ProducerInvalidEpochSeq,
    ProducerSequenceGap,
    ProducerStaleEpoch,
    ProducerState,
    ProducerStreamClosed,
)


class TestAcceptedIsNotARefusal:
    """`None` means "no refusal response" — the caller sends its own success."""

    def test_accepted_yields_no_response(self):
        accepted = ProducerAccepted(
            producer_id="p",
            proposed_state=ProducerState(epoch=0, last_seq=0, last_updated=0.0),
        )
        assert producer_response(accepted, producer_epoch=0) is None

    def test_no_producer_result_yields_no_response(self):
        assert producer_response(None, producer_epoch=0) is None


class TestTheRefusalTable:
    def test_a_duplicate_is_204_echoing_the_producer_position(self):
        status, body, headers = producer_response(
            ProducerDuplicate(last_seq=4), producer_epoch=2
        )
        assert status == 204
        assert body == b""
        assert headers[PRODUCER_EPOCH_HEADER] == "2"
        assert headers[PRODUCER_SEQ_HEADER] == "4"

    def test_a_stale_epoch_is_403_naming_the_current_epoch(self):
        status, body, headers = producer_response(
            ProducerStaleEpoch(current_epoch=9), producer_epoch=3
        )
        assert status == 403
        assert body == b"Stale producer epoch"
        assert headers[PRODUCER_EPOCH_HEADER] == "9", (
            "must report the epoch in force, not the one the client sent"
        )

    def test_an_invalid_epoch_opening_is_400(self):
        status, body, headers = producer_response(
            ProducerInvalidEpochSeq(), producer_epoch=1
        )
        assert status == 400
        assert body == b"New epoch must start with sequence 0"

    def test_a_sequence_gap_is_409_with_both_sequences(self):
        status, body, headers = producer_response(
            ProducerSequenceGap(expected_seq=1, received_seq=5), producer_epoch=0
        )
        assert status == 409
        assert body == b"Producer sequence gap"
        assert headers[PRODUCER_EXPECTED_SEQ_HEADER] == "1"
        assert headers[PRODUCER_RECEIVED_SEQ_HEADER] == "5"

    def test_a_closed_stream_is_409_flagged_closed(self):
        status, body, headers = producer_response(
            ProducerStreamClosed(), producer_epoch=0, offset="42"
        )
        assert status == 409
        assert body == b"Stream is closed"
        assert headers["Stream-Closed"] == "true"
        assert headers["Stream-Next-Offset"] == "42"


class TestTheVaryingParts:
    """What differed between the three copies, now parameters rather than
    divergence."""

    def test_a_duplicate_carries_the_offset_when_one_is_known(self):
        _, _, headers = producer_response(
            ProducerDuplicate(last_seq=0), producer_epoch=0, offset="17"
        )
        assert headers["Stream-Next-Offset"] == "17"

    def test_a_duplicate_omits_the_offset_when_none_is_known(self):
        """The append-result path has no offset to report; it must not invent
        an empty header."""
        _, _, headers = producer_response(
            ProducerDuplicate(last_seq=0), producer_epoch=0
        )
        assert "Stream-Next-Offset" not in headers

    def test_a_duplicate_is_flagged_closed_when_the_stream_is(self):
        _, _, headers = producer_response(
            ProducerDuplicate(last_seq=0), producer_epoch=0, stream_closed=True
        )
        assert headers["Stream-Closed"] == "true"

    def test_a_duplicate_is_not_flagged_closed_otherwise(self):
        _, _, headers = producer_response(
            ProducerDuplicate(last_seq=0), producer_epoch=0
        )
        assert "Stream-Closed" not in headers


class TestOnlyARefusalCarriesAnErrorBody:
    """The 204 arm must stay bodiless — a 204 with a body is malformed, and it
    is the one arm the caller sends with `send_response` rather than
    `_send_error`."""

    def test_the_duplicate_arm_has_an_empty_body(self):
        _, body, _ = producer_response(ProducerDuplicate(last_seq=0), producer_epoch=0)
        assert body == b""

    @pytest.mark.parametrize(
        "result",
        [
            ProducerStaleEpoch(current_epoch=1),
            ProducerInvalidEpochSeq(),
            ProducerSequenceGap(expected_seq=0, received_seq=3),
            ProducerStreamClosed(),
        ],
        ids=["stale", "invalid", "gap", "closed"],
    )
    def test_every_other_arm_has_a_body(self, result):
        status, body, _ = producer_response(result, producer_epoch=0)
        assert body, f"{type(result).__name__} must explain itself"
        assert status >= 400


class TestEveryResultTypeIsHandled:
    """A new `ProducerValidationResult` without an arm must fail here rather
    than fall through to a 500 at runtime — the same guarantee `_status_for`'s
    closed-set test gives the exception half."""

    def test_no_refusal_type_falls_through(self):
        import rakaia.types as types

        refusals = [
            cls
            for name, cls in vars(types).items()
            if name.startswith("Producer")
            and isinstance(cls, type)
            and name not in ("ProducerAccepted", "ProducerState")
            and name != "ProducerValidationResult"
        ]
        assert refusals, "sanity: the refusal types should be discoverable"
        for cls in refusals:
            instance = _build(cls)
            assert producer_response(instance, producer_epoch=0) is not None, (
                f"{cls.__name__} has no response arm"
            )


def _build(cls):
    """Instantiate a refusal type with whatever fields it declares."""
    import dataclasses

    if not dataclasses.is_dataclass(cls):
        return cls()
    kwargs = {}
    for f in dataclasses.fields(cls):
        if f.default is not dataclasses.MISSING or (
            f.default_factory is not dataclasses.MISSING  # type: ignore[misc]
        ):
            continue
        kwargs[f.name] = 0 if f.type in ("int", int) else "x"
    return cls(**kwargs)
