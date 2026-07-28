#!/usr/bin/env python3
"""`protocol_streams` — the raw Durable Streams protocol, no Django in sight.

Every other example mounts rakaia in Django and drives the event-sourcing layer
(handlers, projections, replay). This one exercises the *other half* of rakaia:
the zero-dependency `StreamStore` and the pure-protocol primitives that sit
underneath all of that — append/read, no-op suppression, **producer fencing**,
**stream close**, **subscriber cursors** (`poll`), and the CDN cursor helpers.

It imports nothing but `rakaia` and the stdlib, and runs as a plain script:

    just protocol-demo
    # or: uv run python examples/protocol_streams/demo.py

Each section prints what it proves and asserts the outcome, so a regression in
the protocol layer turns this from "All protocol checks passed ✓" into a stack
trace.
"""

from __future__ import annotations

import json

from rakaia import (
    AppendOptions,
    CursorOptions,
    ProducerAccepted,
    ProducerDuplicate,
    ProducerInvalidEpochSeq,
    ProducerSequenceGap,
    ProducerStaleEpoch,
    StreamStore,
    append_if_changed,
    calculate_cursor,
    generate_response_cursor,
    poll,
    snapshots_equal,
)

STREAM = "sensors/rack-4"


def _hdr(title: str) -> None:
    print(f"\n{title}")
    print("-" * 60)


def _json(**fields: object) -> bytes:
    return json.dumps(fields).encode()


def section_append_and_read(store: StreamStore) -> None:
    _hdr("[1] APPEND -> READ: the store is an ordered, offset-addressed log")
    store.create(STREAM)
    first = store.append(STREAM, _json(temp=20.0)).message
    second = store.append(STREAM, _json(temp=21.5)).message
    assert first is not None and second is not None

    messages, up_to_date = store.read(STREAM)
    assert [json.loads(m.data)["temp"] for m in messages] == [20.0, 21.5]
    assert up_to_date
    # Offsets are opaque but strictly increasing, so a reader can resume.
    assert second.offset > first.offset
    # A partial read from an offset returns only what came after it.
    tail, _ = store.read(STREAM, first.offset)
    assert [json.loads(m.data)["temp"] for m in tail] == [21.5]
    print(f"    2 messages, offsets {first.offset} < {second.offset}")
    print("    read(offset=first) returns only the tail — resumable reads ✓")


def section_append_if_changed(store: StreamStore) -> None:
    _hdr("[2] NO-OP SUPPRESSION: append_if_changed skips an unchanged snapshot")
    path = "sensors/door"
    store.create(path)

    # First reading for the subject: no prior state, so it always lands.
    current = None
    appended = append_if_changed(store, path, _json(state="closed"), current=current)
    assert appended is True
    current = {"state": "closed"}

    # An identical save is suppressed — the audit log doesn't grow on no-ops,
    # the way django-pghistory's `IS DISTINCT FROM` trigger wouldn't fire.
    suppressed = append_if_changed(store, path, _json(state="closed"), current=current)
    assert suppressed is False
    assert snapshots_equal({"state": "closed"}, current)

    # A real change lands again.
    changed = append_if_changed(store, path, _json(state="open"), current=current)
    assert changed is True

    messages, _ = store.read(path)
    assert [json.loads(m.data)["state"] for m in messages] == ["closed", "open"]
    print("    3 saves (closed, closed, open) -> 2 events; the no-op vanished ✓")


def section_producer_fencing(store: StreamStore) -> None:
    _hdr("[3] PRODUCER FENCING: idempotent, gap-detecting, zombie-proof writes")
    path = "sensors/fenced"
    store.create(path)

    def send(producer_id: str, epoch: int, seq: int) -> object:
        result = store.append(
            path,
            _json(epoch=epoch, seq=seq),
            AppendOptions(
                producer_id=producer_id, producer_epoch=epoch, producer_seq=seq
            ),
        )
        return result.producer_result

    P = "writer-A"

    # A fresh producer must open its epoch at seq=0.
    assert isinstance(send(P, epoch=1, seq=0), ProducerAccepted)
    assert isinstance(send(P, epoch=1, seq=1), ProducerAccepted)

    # Re-sending an already-seen (epoch, seq) is a DUPLICATE, not a second write
    # — this is what makes a network retry safe (idempotent append).
    dup = send(P, epoch=1, seq=1)
    assert isinstance(dup, ProducerDuplicate) and dup.last_seq == 1

    # Skipping a sequence number is refused: the writer lost a message.
    gap = send(P, epoch=1, seq=5)
    assert isinstance(gap, ProducerSequenceGap)
    assert gap.expected_seq == 2 and gap.received_seq == 5

    # Fencing: a NEW epoch (a restarted/failed-over writer) must restart at seq=0.
    assert isinstance(send(P, epoch=2, seq=0), ProducerAccepted)
    # ...and a straggling write from the OLD epoch is a zombie — rejected so a
    # partitioned old primary can't corrupt the log after failover.
    zombie = send(P, epoch=1, seq=2)
    assert isinstance(zombie, ProducerStaleEpoch) and zombie.current_epoch == 2
    # A new epoch that forgets to restart its sequence is rejected too.
    assert isinstance(send(P, epoch=3, seq=9), ProducerInvalidEpochSeq)

    # Only the ACCEPTED writes materialised (2 in epoch 1 + 1 in epoch 2).
    messages, _ = store.read(path)
    assert len(messages) == 3
    print("    accepted x3; duplicate, gap, stale-epoch, invalid-epoch all fenced")
    print("    only the 3 accepted appends are in the log ✓")


def section_close(store: StreamStore) -> None:
    _hdr("[4] CLOSE: a closed stream is sealed against further appends")
    path = "sensors/decommissioned"
    store.create(path)
    store.append(path, _json(temp=19.0))

    result = store.close_stream(path)
    assert result is not None
    assert result.already_closed is False
    # Closing again is idempotent, and reports it was already closed.
    assert store.close_stream(path).already_closed is True

    # Appends after close are refused (the AppendResult flags the closure).
    after = store.append(path, _json(temp=99.0))
    assert after.stream_closed is True
    assert after.message is None
    messages, _ = store.read(path)
    assert len(messages) == 1  # the post-close append never landed
    print(
        f"    closed at final_offset={result.final_offset}; further appends refused ✓"
    )


def section_subscriber_cursors(store: StreamStore) -> None:
    _hdr("[5] SUBSCRIBER CURSORS: poll() reads a stream incrementally")
    path = "sensors/consumer"
    store.create(path)
    store.append(path, _json(n=1))
    store.append(path, _json(n=2))

    # First poll has no cursor: it's `fresh` and returns everything.
    p1 = poll(store, path, cursor=None)
    assert p1.status == "fresh"
    assert [json.loads(m.data)["n"] for m in p1.messages] == [1, 2]

    # Persist the watermark, poll again with nothing new -> `caught_up`.
    p2 = poll(store, path, cursor=p1.cursor)
    assert p2.status == "caught_up" and p2.messages == []

    # New data arrives; the next poll `advanced`s and returns only the delta.
    store.append(path, _json(n=3))
    p3 = poll(store, path, cursor=p2.cursor)
    assert p3.status == "advanced"
    assert [json.loads(m.data)["n"] for m in p3.messages] == [3]

    # Defensive `rewound`: a cursor that sorts *past* the head (a truncated or
    # rebuilt log, or a cursor carried over from another stream) triggers a
    # full re-read so the consumer can reset derived state.
    bogus = "9999999999999999_9999999999999999"
    p4 = poll(store, path, cursor=bogus)
    assert p4.status == "rewound" and p4.rewound
    assert [json.loads(m.data)["n"] for m in p4.messages] == [1, 2, 3]

    # An absent stream is reported, not raised.
    assert poll(store, "sensors/nope", cursor=None).status == "absent"
    print("    fresh -> caught_up -> advanced(delta only) -> rewound(re-read) ✓")


def section_cdn_cursor() -> None:
    _hdr("[6] CDN CURSOR: interval cursors collapse cache stampedes (protocol 8.1)")
    opts = CursorOptions(interval_seconds=20)
    now = calculate_cursor(opts)
    assert now.isdigit()

    # A client already at/ahead of the current interval gets pushed forward
    # (never backwards) so a CDN never loops A->B->A.
    ahead = str(int(now) + 100)
    forward = generate_response_cursor(ahead, opts)
    assert int(forward) > int(ahead)
    # A stale client cursor is snapped up to the current interval.
    assert generate_response_cursor("0", opts) == now
    print(f"    interval cursor now={now}; monotonic progression enforced ✓")


def main() -> None:
    print("=" * 60)
    print("  Rakaia protocol layer — StreamStore + pure-protocol primitives")
    print("=" * 60)
    store = StreamStore()
    section_append_and_read(store)
    section_append_if_changed(store)
    section_producer_fencing(store)
    section_close(store)
    section_subscriber_cursors(store)
    section_cdn_cursor()
    print("\nAll protocol checks passed ✓")


if __name__ == "__main__":
    main()
