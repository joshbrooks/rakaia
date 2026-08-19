"""
Deltas: partial-update and part (array) events — an event that says *what
changed* instead of re-carrying the whole subject.

Every event rakaia has projected so far is a **full snapshot**: `project_latest`
says as much ("Because every event is a full snapshot, 'latest' needs no
reducer"). That holds up until one field of a large form changes. The first
production consumer's forms carry repeated child collections (FormKit
"repeaters") of dozens of rows; editing one cell re-appends every row, and 62% of
its re-saves re-carried the blob with no content change at all.

A **delta** event carries only the change. Five operations, one per shape of
change, mirroring the one-type-per-operation split in `effects.py`:

* :class:`SetField` / :class:`ClearField` — the partial update. Set or remove one
  location in the payload.
* :class:`AddPart` / :class:`RemovePart` / :class:`MovePart` — the array ops.
  Insert, delete or reorder one row of a repeated collection.

They travel together in one **patch event**: envelope ``label="patch"``, payload
``{"ops": [...]}``. One save is one event however many things it touched, so
atomicity is preserved and the audit log still reads one row per save.

Two design points are load-bearing and are pinned by tests rather than left to
convention:

**Parts have identity, positions do not.** ADR 0001 rejects positional index as
identity and requires a stable child id — noting that when the source has none,
one must be *assigned at ingestion*. :class:`AddPart` is that assignment: it
carries the ``part_id`` the producer mints and stamps it into the folded row
under `PART_ID_KEY`. That is what makes :class:`MovePart` expressible at all,
and it closes the caveat the ADR left open: under full snapshots
``[A,B,C] -> [C,A,B]`` is indistinguishable from "every slot's content changed",
so no projection can recover "C moved". With a move event it is one reorder of
three stable rows. ``index`` on an add or a move is a *command parameter* — a
position in the collection at the moment of the edit — never an identity.

**A delta is meaningless without a base, and this module refuses to guess one.**
`fold_snapshot` raises :class:`NoBaseSnapshotError` when a patch arrives with no
preceding snapshot, and :class:`DeltaConflictError` when an op does not fit the
state it is applied to (clearing an absent key, moving an unknown part). Both
mean the same thing: the fold is not standing where the producer stood. Applying
the patch anyway would produce a state nobody ever saved — the one failure a
system whose selling point is "replay gives the answer that was correct at the
time" cannot afford to make quietly. This is the cost deltas buy their size
saving with, and it is why `fold_snapshot` takes an explicit ``base=``: an
incremental tail read must supply the state it is resuming from.

Nothing here is a new registration kind. A delta is a *payload shape* a handler
decodes, consistent with ADR 0004 — the extension point stays the content-routed
staged handler.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import Any

from .types import StreamMessage

# =============================================================================
# Constants
# =============================================================================

#: The envelope label a patch event carries. Chosen rather than reusing
#: ``update`` so that a consumer reading a stream can tell a snapshot from a
#: delta without decoding the payload, and so `label_marker` keeps mapping
#: ``update`` to ``~`` unchanged.
PATCH_LABEL = "patch"

#: The key under which a part's stable id is stamped into its folded row.
#:
#: Underscore-prefixed so it cannot collide with a form field, and *reserved*:
#: :class:`AddPart` rejects a value that already carries it, because a payload
#: setting its own part id would let two rows claim one identity.
PART_ID_KEY = "_part"

#: The payload key holding a patch's operation list.
OPS_KEY = "ops"


# =============================================================================
# Errors
# =============================================================================


class DeltaError(Exception):
    """Base for the two ways a delta fails."""


class DeltaConflictError(DeltaError):
    """An operation does not fit the state it was applied to.

    Clearing a key that is absent, adding a part id that already exists, moving
    or removing one that does not, indexing past the end of a collection. Every
    case means the same thing — the state being folded is not the state the
    producer patched against — and every case is raised rather than skipped: a
    delta silently dropped converges on a state that was never saved, which is
    indistinguishable from a correct fold at the point where anyone looks.
    """


class NoBaseSnapshotError(DeltaError):
    """A patch was folded with nothing to apply it to.

    Either the message window opened mid-stream (a tail read starting after the
    last snapshot) or the subject's latest event was a tombstone. Pass
    ``fold_snapshot(..., base=…)`` with the state being resumed from.
    """


# =============================================================================
# The operations
# =============================================================================
#
# One frozen dataclass per operation, each carrying only the fields its own
# operation uses — the same reasoning as `effects.py`: a `part_id` on a
# `SetField` or a `value` on a `MovePart` is then a type error rather than a
# runtime check.


@dataclass(frozen=True)
class SetField:
    """Set one location in the payload to ``value``.

    ``path`` is a tuple of segments walked from the root. A segment addressing a
    member of a repeated collection is that member's **part id**, not its index
    — ``("rows", "p2", "n")`` means "field ``n`` of the part ``p2``", and stays
    correct across every reorder.

    The parent of the target must already exist: a set through a missing branch
    raises rather than autovivifying it, so a patch written against one schema
    cannot silently invent a branch in another.
    """

    path: tuple[str, ...]
    value: Any

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("SetField has an empty path; it must name a location.")


@dataclass(frozen=True)
class ClearField:
    """Remove one location from the payload. ``path`` reads as in :class:`SetField`.

    Distinct from ``SetField(path, None)`` on purpose: a form field explicitly
    answered "none" and a field that is not present are different facts, and a
    projection that cannot tell them apart cannot round-trip its own state.
    """

    path: tuple[str, ...]

    def __post_init__(self) -> None:
        if not self.path:
            raise ValueError("ClearField has an empty path; it must name a location.")


@dataclass(frozen=True)
class AddPart:
    """Insert a row into the repeated collection at ``key``.

    ``index`` is where it lands in the collection *as it stands now* — a command
    parameter, not an identity. ``part_id`` is the identity: minted by the
    producer, stamped into the stored row under `PART_ID_KEY`, and the thing
    every later op names. The collection is created when ``key`` is absent, so
    the first add needs no prior array.
    """

    key: str
    index: int
    part_id: str
    value: dict[str, Any]

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ValueError(f"AddPart index must be >= 0, got {self.index}.")
        if not self.part_id:
            raise ValueError("AddPart needs a non-empty part_id.")
        if PART_ID_KEY in self.value:
            raise ValueError(
                f"AddPart value carries the reserved key {PART_ID_KEY!r}; the "
                f"part id comes from part_id=, so a payload setting its own "
                f"would let two rows claim one identity."
            )


@dataclass(frozen=True)
class RemovePart:
    """Remove the part ``part_id`` from the collection at ``key``.

    By id, never by position — which is what keeps a remove correct when it is
    folded after a reorder it did not know about.
    """

    key: str
    part_id: str


@dataclass(frozen=True)
class MovePart:
    """Move the part ``part_id`` to position ``index`` within ``key``.

    ``index`` is the destination in the collection **with the moved part taken
    out** — the position a drag-and-drop UI reports, and the reading that makes
    moving a part to the position it already occupies a no-op rather than an
    off-by-one.
    """

    key: str
    part_id: str
    index: int

    def __post_init__(self) -> None:
        if self.index < 0:
            raise ValueError(f"MovePart index must be >= 0, got {self.index}.")


#: Anything a patch event may carry.
Delta = SetField | ClearField | AddPart | RemovePart | MovePart


# =============================================================================
# Applying one delta
# =============================================================================


def _find_part(rows: list[Any], part_id: str) -> int:
    for i, row in enumerate(rows):
        if isinstance(row, dict) and row.get(PART_ID_KEY) == part_id:
            return i
    return -1


def _collection(state: dict[str, Any], key: str, op: str) -> list[Any]:
    rows = state.get(key)
    if not isinstance(rows, list):
        raise DeltaConflictError(
            f"{op}: {key!r} is not a collection in the state being folded "
            f"({'absent' if rows is None else type(rows).__name__})."
        )
    return rows


def _descend(state: dict[str, Any], path: tuple[str, ...]) -> tuple[Any, str]:
    """Walk `path[:-1]`, copying each container on the way, and return the
    mutable parent plus the final segment.

    Copy-on-descend rather than a deep copy: the fold is a chain of applications
    over states a caller may still hold (the ``base=``), so no input is ever
    mutated, but untouched subtrees stay shared instead of being cloned per op.
    """
    parent: Any = state
    for depth, segment in enumerate(path[:-1]):
        walked = "/".join(path[: depth + 1])
        if isinstance(parent, list):
            i = _find_part(parent, segment)
            if i < 0:
                raise DeltaConflictError(
                    f"no part {segment!r} in the collection at /{walked}."
                )
            child = dict(parent[i])
            parent[i] = child
            parent = child
            continue
        if not isinstance(parent, dict) or segment not in parent:
            raise DeltaConflictError(
                f"path /{walked} does not exist in the state being folded; a "
                f"set through a missing branch is refused rather than creating it."
            )
        value = parent[segment]
        if isinstance(value, dict):
            child = dict(value)
        elif isinstance(value, list):
            child = list(value)
        else:
            child = value
        if not isinstance(child, (dict, list)):
            raise DeltaConflictError(
                f"path /{walked} is a {type(child).__name__}, not a container."
            )
        parent[segment] = child
        parent = child
    return parent, path[-1]


def apply_delta(state: dict[str, Any], delta: Delta) -> dict[str, Any]:
    """Return ``state`` with ``delta`` applied. Pure: the input is not mutated.

    Raises :class:`DeltaConflictError` when the op does not fit the state.
    """
    new = dict(state)

    if isinstance(delta, (SetField, ClearField)):
        parent, last = _descend(new, delta.path)
        if isinstance(parent, list):
            # The path's last segment is a part id, so it addresses a whole row
            # rather than a field of one. Replacing or dropping a row is what
            # AddPart/RemovePart are for — and routing it here would lose the
            # part's identity checks, so it is refused rather than approximated.
            raise DeltaConflictError(
                f"path /{'/'.join(delta.path)} addresses a whole part of "
                f"/{'/'.join(delta.path[:-1])}; name a field inside it, or use "
                f"AddPart/RemovePart to add or drop the row itself."
            )
        if isinstance(delta, SetField):
            parent[last] = delta.value
        else:
            if last not in parent:
                raise DeltaConflictError(
                    f"cannot clear /{'/'.join(delta.path)}: it is not present in "
                    f"the state being folded."
                )
            del parent[last]
        return new

    if isinstance(delta, AddPart):
        rows = list(new.get(delta.key) or [])
        if delta.key in new and not isinstance(new[delta.key], list):
            raise DeltaConflictError(
                f"AddPart: {delta.key!r} is a "
                f"{type(new[delta.key]).__name__}, not a collection."
            )
        if _find_part(rows, delta.part_id) >= 0:
            raise DeltaConflictError(
                f"AddPart: part {delta.part_id!r} already exists in {delta.key!r}; "
                f"a re-used id would give two rows one identity."
            )
        if delta.index > len(rows):
            raise DeltaConflictError(
                f"AddPart: index {delta.index} is past the end of {delta.key!r} "
                f"(length {len(rows)})."
            )
        rows.insert(delta.index, {PART_ID_KEY: delta.part_id, **delta.value})
        new[delta.key] = rows
        return new

    if isinstance(delta, RemovePart):
        rows = list(_collection(new, delta.key, "RemovePart"))
        i = _find_part(rows, delta.part_id)
        if i < 0:
            raise DeltaConflictError(
                f"RemovePart: no part {delta.part_id!r} in {delta.key!r}."
            )
        del rows[i]
        new[delta.key] = rows
        return new

    if isinstance(delta, MovePart):
        rows = list(_collection(new, delta.key, "MovePart"))
        i = _find_part(rows, delta.part_id)
        if i < 0:
            raise DeltaConflictError(
                f"MovePart: no part {delta.part_id!r} in {delta.key!r}."
            )
        row = rows.pop(i)
        if delta.index > len(rows):
            raise DeltaConflictError(
                f"MovePart: index {delta.index} is past the end of {delta.key!r} "
                f"(length {len(rows)} once the moved part is taken out)."
            )
        rows.insert(delta.index, row)
        new[delta.key] = rows
        return new

    raise TypeError(f"not a delta: {delta!r}")


# =============================================================================
# The wire form
# =============================================================================
#
# Paths are encoded as JSON Pointers (RFC 6901) so the escaping rule for a
# segment containing "/" or "~" is a citation rather than a house invention.


def _encode_pointer(path: tuple[str, ...]) -> str:
    return "".join("/" + s.replace("~", "~0").replace("/", "~1") for s in path)


def _decode_pointer(pointer: str) -> tuple[str, ...]:
    if not pointer.startswith("/"):
        raise ValueError(f"not a JSON Pointer: {pointer!r} (must start with '/')")
    return tuple(
        s.replace("~1", "/").replace("~0", "~") for s in pointer[1:].split("/")
    )


def encode_patch(deltas: Iterable[Delta]) -> dict[str, Any]:
    """The JSON payload of a patch event carrying ``deltas``, in order."""
    ops: list[dict[str, Any]] = []
    for d in deltas:
        if isinstance(d, SetField):
            ops.append({"op": "set", "path": _encode_pointer(d.path), "value": d.value})
        elif isinstance(d, ClearField):
            ops.append({"op": "clear", "path": _encode_pointer(d.path)})
        elif isinstance(d, AddPart):
            ops.append(
                {
                    "op": "add_part",
                    "key": d.key,
                    "index": d.index,
                    "part": d.part_id,
                    "value": d.value,
                }
            )
        elif isinstance(d, RemovePart):
            ops.append({"op": "remove_part", "key": d.key, "part": d.part_id})
        elif isinstance(d, MovePart):
            ops.append(
                {"op": "move_part", "key": d.key, "part": d.part_id, "index": d.index}
            )
        else:
            raise TypeError(f"not a delta: {d!r}")
    return {OPS_KEY: ops}


def decode_patch(payload: dict[str, Any]) -> list[Delta]:
    """The deltas a patch payload carries. Raises ``ValueError`` on an unknown op.

    Unknown ops are refused rather than skipped: a reader that quietly ignores an
    operation it does not understand folds to a state the producer never wrote,
    and reports success while doing it.
    """
    raw = payload.get(OPS_KEY)
    if not isinstance(raw, list):
        raise ValueError(f"patch payload has no {OPS_KEY!r} list")
    out: list[Delta] = []
    for op in raw:
        kind = op.get("op")
        if kind == "set":
            out.append(SetField(_decode_pointer(op["path"]), op.get("value")))
        elif kind == "clear":
            out.append(ClearField(_decode_pointer(op["path"])))
        elif kind == "add_part":
            out.append(
                AddPart(op["key"], op["index"], op["part"], op.get("value") or {})
            )
        elif kind == "remove_part":
            out.append(RemovePart(op["key"], op["part"]))
        elif kind == "move_part":
            out.append(MovePart(op["key"], op["part"], op["index"]))
        else:
            raise ValueError(
                f"unknown delta op {kind!r}; a reader that skipped it would fold "
                f"to a state the producer never wrote."
            )
    return out


def is_patch(payload: Any) -> bool:
    """Whether ``payload`` is a patch payload (an ``ops`` *list*).

    Shape-based rather than label-based so a caller holding only the decoded
    payload can still tell. Prefer the envelope ``label == PATCH_LABEL`` when you
    have the message, which is what `fold_snapshot` uses.
    """
    return isinstance(payload, dict) and isinstance(payload.get(OPS_KEY), list)


# =============================================================================
# Folding a message window
# =============================================================================


def fold_snapshot(
    messages: Sequence[StreamMessage],
    *,
    base: dict[str, Any] | None = None,
    tombstone_labels: Sequence[str] = ("delete",),
) -> dict[str, Any] | None:
    """Fold snapshots and patches into the current state, or ``None`` if deleted.

    A message whose label is `PATCH_LABEL` is applied to the state so far; any
    other message *replaces* it (an ordinary full snapshot), and a tombstone
    label clears it. So a stream may mix the two freely, and a periodic full
    snapshot is all a compaction strategy needs: it bounds how far back a reader
    must go.

    Args:
        messages: the window, oldest first — typically ``store.read(path)`` for a
            single subject, or one subject's slice of a family stream.
        base: the state a patch at position 0 applies to. Required when the
            window opens mid-stream; ``None`` (the default) means the window must
            start with a snapshot.
        tombstone_labels: labels meaning "no live state for this subject".

    Raises:
        NoBaseSnapshotError: a patch arrived with nothing to apply it to.
        DeltaConflictError: an op did not fit the state; the message's offset is
            named in the error so the divergence can be located in the log.
    """
    state: dict[str, Any] | None = dict(base) if base is not None else None
    tombstones = set(tombstone_labels)

    for msg in messages:
        if msg.label in tombstones:
            state = None
            continue
        payload = json.loads(msg.data)
        if msg.label != PATCH_LABEL:
            state = payload
            continue
        if state is None:
            raise NoBaseSnapshotError(
                f"patch at offset {msg.offset} has no base snapshot: the window "
                f"opens after the subject's last full snapshot, or its last "
                f"event was a tombstone. Pass base= with the state being "
                f"resumed from."
            )
        for delta in decode_patch(payload):
            try:
                state = apply_delta(state, delta)
            except DeltaConflictError as exc:
                raise DeltaConflictError(f"at offset {msg.offset}: {exc}") from exc
    return state


def parts_of(state: dict[str, Any], key: str) -> list[tuple[str, int, dict[str, Any]]]:
    """The collection at ``key`` as ``(part_id, position, row)`` triples.

    The bridge to the projection helpers: pass the rows to `reconcile_tree` with
    ``id_fn`` reading `PART_ID_KEY`, and write the position into the row's
    ``defaults`` as its order field. Identity comes from the log, position from
    the fold — the split ADR 0001 asks for.
    """
    rows = state.get(key) or []
    if not isinstance(rows, list):
        raise DeltaConflictError(f"{key!r} is not a collection.")
    out: list[tuple[str, int, dict[str, Any]]] = []
    for position, row in enumerate(rows):
        if not isinstance(row, dict) or PART_ID_KEY not in row:
            raise DeltaConflictError(
                f"row {position} of {key!r} has no {PART_ID_KEY!r}; it was not "
                f"materialised by an AddPart, so it has no stable identity."
            )
        out.append((row[PART_ID_KEY], position, row))
    return out
