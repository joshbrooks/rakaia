"""Producer fencing: the rules, as a pure function.

A producer identifies itself with `(Producer-Id, Producer-Epoch, Producer-Seq)`.
From its last known state and the tuple it presents, exactly one outcome
follows — accept, duplicate, stale epoch, bad epoch start, or sequence gap.

Those rules live here rather than inside a store so both stores decide
identically. The in-memory store keeps producer state in a dict and the durable
store in a table; neither difference reaches the rules, which see only the last
state and the incoming tuple.

The function never mutates. On acceptance it returns the state the caller
should commit *after* the append succeeds, so a failed write cannot advance a
producer's sequence.
"""

from __future__ import annotations

from .types import (
    PRODUCER_STATE_TTL_SECONDS,
    ProducerAccepted,
    ProducerDuplicate,
    ProducerInvalidEpochSeq,
    ProducerSequenceGap,
    ProducerStaleEpoch,
    ProducerState,
    ProducerValidationResult,
)


def is_producer_state_expired(state: ProducerState, now: float) -> bool:
    """Whether `state` has aged out and should be treated as absent.

    Expiring state means a long-abandoned producer id starts fresh (at seq 0)
    rather than pinning its sequence forever.
    """
    return now - state.last_updated > PRODUCER_STATE_TTL_SECONDS


def validate_producer(
    state: ProducerState | None,
    producer_id: str,
    epoch: int,
    seq: int,
    now: float,
) -> ProducerValidationResult:
    """Decide the outcome for one producer-fenced write.

    `state` is the producer's last committed state, or `None` if it is unknown
    (never seen, or expired — the caller applies `is_producer_state_expired`).

    The rules:

    - Unknown producer: must open at seq 0, else it is a gap.
    - Lower epoch than known: stale — another writer has fenced this one out.
    - Higher epoch: a new epoch must also open at seq 0.
    - Same epoch, seq at or below the last: a duplicate (a retry), not an error.
    - Same epoch, exactly one above: accepted.
    - Anything further ahead: a gap.
    """
    if state is None or is_producer_state_expired(state, now):
        if seq != 0:
            return ProducerSequenceGap(expected_seq=0, received_seq=seq)
        return ProducerAccepted(
            is_new=True,
            producer_id=producer_id,
            proposed_state=ProducerState(epoch=epoch, last_seq=0, last_updated=now),
        )

    if epoch < state.epoch:
        return ProducerStaleEpoch(current_epoch=state.epoch)

    if epoch > state.epoch:
        if seq != 0:
            return ProducerInvalidEpochSeq()
        return ProducerAccepted(
            is_new=True,
            producer_id=producer_id,
            proposed_state=ProducerState(epoch=epoch, last_seq=0, last_updated=now),
        )

    if seq <= state.last_seq:
        return ProducerDuplicate(last_seq=state.last_seq)

    if seq == state.last_seq + 1:
        return ProducerAccepted(
            is_new=False,
            producer_id=producer_id,
            proposed_state=ProducerState(epoch=epoch, last_seq=seq, last_updated=now),
        )

    return ProducerSequenceGap(expected_seq=state.last_seq + 1, received_seq=seq)
