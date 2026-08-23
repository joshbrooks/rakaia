"""Moving a stream from one backend to another, and saying what survived.

Switching `RAKAIA_STORE` under a live stream does not move anything: the app
starts reading an empty log while every saved cursor still looks valid. Moving
between backends is a *copy*, and this is it.

**Best effort means measured, not promised.** Whether the copy preserves offsets
depends on facts this module does not control — chiefly whether the two stores
issue the same offset *format*, and whether the source's own offsets are an
unbroken run from the first. The in-memory store counts bytes and the other two
count entries, so that pair can never line up; two entry-counting stores line up
whenever the source has no gaps, and a stream that has been deleted and
recreated has gaps by design (#34).

So nothing here guesses. The copy runs, and then the offsets it produced are
compared against the offsets it read. `Migration.offsets_preserved` is the
result of that comparison, and `Migration.cursors_valid` says the thing a caller
actually wants to know: whether a subscriber's saved position still means the
same event on the other side. When it is False, consumers must be reset — and
they must be reset *knowingly*, which is the whole reason this returns a report
instead of None.

What cannot cross the public store API at all is listed in `Migration.notes`
rather than silently dropped: producer fencing state has no public setter, and a
sliding TTL window necessarily restarts on the copy.
"""

from __future__ import annotations

from collections.abc import Iterator, Sequence
from dataclasses import dataclass, field
from typing import Any

from .protocols import ReadableStore, StreamServerStore, WritableStore
from .types import AppendOptions, StreamMessage

__all__ = ["Migration", "migrate_all", "migrate_stream"]

_DEFAULT_BATCH = 500


@dataclass(frozen=True)
class Migration:
    """What one stream's copy achieved, and what it could not carry."""

    path: str
    """The stream that was copied."""

    events: int
    """How many messages were written to the target."""

    offsets_preserved: bool
    """Whether every event kept the offset it had in the source.

    Established by comparing the target's offsets with the source's after the
    copy, not by reasoning about the two backends beforehand.
    """

    head_preserved: bool
    """Whether the target's head offset matches the source's.

    Distinct from `offsets_preserved`: a source whose head sits above its last
    event — a stream recreated at a path and not yet re-appended to — cannot
    have that head reproduced by copying events, so a cursor parked at the head
    would not survive even though every event kept its offset.
    """

    notes: tuple[str, ...] = field(default_factory=tuple)
    """Everything the copy could not carry, in plain words. Empty is the good
    case; it is never used to report a failure, which raises instead."""

    @property
    def cursors_valid(self) -> bool:
        """Whether a subscriber's saved position still means the same event.

        The question a caller is actually asking. Both halves have to hold: an
        offset must name the event it used to name, and the head must be where
        it was, or a caught-up consumer resumes as `rewound`.
        """
        return self.offsets_preserved and self.head_preserved


def _batched(items: Sequence[Any], size: int) -> Iterator[Sequence[Any]]:
    for start in range(0, len(items), size):
        yield items[start : start + size]


def _options_for(message: StreamMessage, *, seq: str | None = None) -> AppendOptions:
    """The envelope to re-append this message with.

    `event_ts` is carried explicitly. It is the one timestamp that is part of the
    event rather than of the transport, so a copy that let it default would move
    every event's logical time to the moment of the migration and silently
    reorder any `merge_replay(order_key=ENVELOPE_TS)` built on it.
    """
    return AppendOptions(
        label=message.label,
        metadata=message.metadata,
        event_ts=message.event_ts,
        seq=seq,
    )


def _payload_for(message: StreamMessage, *, json_mode: bool) -> bytes:
    """The bytes to append so the target stores this message unchanged.

    In JSON mode the protocol flattens a posted array one level, and the stored
    payloads are already the flattened elements — so an element that is *itself*
    an array would be flattened a second time by a naive copy, turning one event
    into several. Wrapping each payload in a one-element array cancels exactly
    the flatten the target will apply, which leaves every other payload
    untouched and fixes the array-valued ones.
    """
    if not json_mode:
        return message.data
    return b"[" + message.data + b"]"


def migrate_stream(
    source: ReadableStore,
    target: WritableStore,
    path: str,
    *,
    batch_size: int = _DEFAULT_BATCH,
) -> Migration:
    """Copy `path` from `source` to `target`, returning what survived.

    The target stream must not already hold events: a copy into a populated
    stream would interleave two logs and there is no correct offset for the
    result. That is refused rather than merged.

    Raises:
        StreamNotFound: if `path` is absent from `source`.
        ValueError: if the target already holds events for `path`.
    """
    messages, _ = source.read(path)
    source_meta = source.get(path) if isinstance(source, StreamServerStore) else None
    notes: list[str] = []

    if target.has(path) and target.read(path)[0]:
        raise ValueError(
            f"Target already holds events for {path!r}. A copy into a populated "
            f"stream would interleave two logs; delete the target stream first."
        )

    json_mode = _create_target(target, path, source_meta, notes)

    for chunk in _batched(messages, batch_size):
        # `seq` rides on the very last event so the target inherits the source's
        # `Stream-Seq` fence. Set earlier it would refuse the events after it,
        # since the rule is strictly-increasing and the intermediate values were
        # never recorded.
        last_seq = getattr(source_meta, "last_seq", None)
        target.append_many(
            path,
            [
                (
                    _payload_for(m, json_mode=json_mode),
                    _options_for(
                        m,
                        seq=(
                            last_seq
                            if last_seq is not None and m is messages[-1]
                            else None
                        ),
                    ),
                )
                for m in chunk
            ],
        )

    written, _ = target.read(path)
    offsets_preserved = [m.offset for m in written] == [m.offset for m in messages]
    head_preserved = _head_preserved(source, target, path)

    _close_if_needed(target, path, source_meta, notes)
    _note_what_cannot_cross(source_meta, notes)
    if not offsets_preserved:
        notes.append(
            "Offsets changed, so every saved cursor for this stream is stale — "
            "reset consumers rather than letting them resume."
        )
        if _target_retired_these_offsets(messages, written):
            notes.append(
                "The target has held this path before and retired its offsets: "
                "a store never reissues an offset it has already handed out "
                "(#34), so a stream cannot be copied back into the store it "
                "was deleted from and keep its numbering. Copy to a store that "
                "has never held this path, or accept the renumbering."
            )
    elif not head_preserved:
        notes.append(
            "Events kept their offsets but the head did not: the source's head "
            "sits above its last event, which a copy cannot reproduce. A "
            "consumer parked at the head will report `rewound`."
        )

    return Migration(
        path=path,
        events=len(written),
        offsets_preserved=offsets_preserved,
        head_preserved=head_preserved,
        notes=tuple(notes),
    )


def _target_retired_these_offsets(
    source: Sequence[StreamMessage], written: Sequence[StreamMessage]
) -> bool:
    """Whether the target renumbered *upwards*, which names one cause.

    Every copied offset landing above every source offset is the signature of a
    target that has held this path before: it resumes above the high mark it
    retired rather than starting again at one. Worth separating from the other
    reasons a copy can renumber, because it is the one a caller is most likely
    to have caused on purpose and least likely to expect.
    """
    if not source or not written:
        return False
    return min(m.offset for m in written) > max(m.offset for m in source)


def _create_target(
    target: WritableStore, path: str, source_meta: Any, notes: list[str]
) -> bool:
    """Create the target stream, carrying what configuration it can take.

    Returns whether the target is a JSON-mode stream, which decides how payloads
    have to be framed on the way in.
    """
    if source_meta is None:
        target.create(path)
        if not isinstance(target, StreamServerStore):
            return False
        got = target.get(path)
        return _is_json(getattr(got, "content_type", None))

    if not isinstance(target, StreamServerStore):
        target.create(path)
        notes.append(
            "The target store takes no stream configuration, so the content "
            "type, TTL and expiry were not carried."
        )
        return False

    target.create(
        path,
        content_type=source_meta.content_type,
        ttl_seconds=source_meta.ttl_seconds,
        expires_at=source_meta.expires_at,
    )
    return _is_json(source_meta.content_type)


def _is_json(content_type: str | None) -> bool:
    from .json_mode import is_json_content_type

    return is_json_content_type(content_type)


def _head_preserved(source: ReadableStore, target: WritableStore, path: str) -> bool:
    """Whether both stores report the same head, when both can be asked."""
    get_source = getattr(source, "get_current_offset", None)
    get_target = getattr(target, "get_current_offset", None)
    if get_source is None or get_target is None:
        return False
    return get_source(path) == get_target(path)


def _close_if_needed(
    target: WritableStore, path: str, source_meta: Any, notes: list[str]
) -> None:
    """Close the target last, if the source was closed.

    Last because a closed stream refuses appends: closing before the copy would
    refuse the very events being copied.
    """
    if source_meta is None or not getattr(source_meta, "closed", False):
        return
    if not isinstance(target, StreamServerStore):
        notes.append(
            "The source stream was closed and the target store cannot close a "
            "stream, so the copy is open where the original was not."
        )
        return
    target.close_stream(path)
    if source_meta.closed_by is not None:
        notes.append(
            "The stream was closed, but not by the producer that closed the "
            "original: a close cannot be attributed through the public API, so "
            "a retry of that producer's closing append will not be recognised "
            "as a duplicate."
        )


def _note_what_cannot_cross(source_meta: Any, notes: list[str]) -> None:
    if source_meta is None:
        return
    if getattr(source_meta, "producers", None):
        notes.append(
            "Producer fencing state was not carried — there is no public way to "
            "set it. A producer resuming against the copy starts as new, so its "
            "first append must begin a fresh epoch at sequence 0."
        )
    if getattr(source_meta, "ttl_seconds", None) is not None:
        notes.append(
            "The stream has a sliding TTL window, which restarts from the copy: "
            "the target expires later than the source would have."
        )


def migrate_all(
    source: ReadableStore,
    target: WritableStore,
    *,
    batch_size: int = _DEFAULT_BATCH,
) -> list[Migration]:
    """Copy every stream `source` can list, in listing order.

    Listing is not part of any store protocol — `WritableStore` describes one
    stream at a time — so this needs a source that happens to offer
    `list_paths()`. Both stores in this package do. A source that does not is a
    clear failure here rather than an empty result, which would read as "there
    was nothing to move".
    """
    list_paths = getattr(source, "list_paths", None)
    if list_paths is None:
        raise TypeError(
            f"{type(source).__name__} cannot list its streams, so there is "
            f"nothing for migrate_all to iterate. Copy known paths with "
            f"migrate_stream instead."
        )
    return [
        migrate_stream(source, target, path, batch_size=batch_size)
        for path in list_paths()
    ]
