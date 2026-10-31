"""A stream store backed by plain JSONL files on disk (spike — see #229).

**This is a proof of concept, not a shipped backend.** It exists to answer one
question: is `rakaia.protocols.StreamServerStore` a real seam, or a description
of what `DjangoStreamStore` happens to do? The measure is the two shared
conformance suites — `tests/store_contract.py` and `tests/server_store_contract.py`
— passing against a backend with no database under it at all.

Layout, one directory per stream::

    <root>/<quoted path>/
        meta.json            head, closed, ttl, producers, …
        000000000000.jsonl   entry ids 1..N-1
        000000010000.jsonl   entry ids N..2N-1
        .lock                the write lock

One JSON object per line. A segment seals at `segment_size` entries, so sealed
segments are immutable, the filename is the index a read seeks with, and
retention is `rm` of a file.

What the spike does implement, because they are the parts that were uncertain:

* **The log is authoritative, `meta.json` is a cache.** An append and a metadata
  write are two files and cannot be made atomic against each other, so the head
  is recoverable by reading the last complete line of the last segment. A torn
  trailing line — a crash mid-write — is ignored on read rather than parsed.
* **One exclusive `flock` per stream**, held across the whole check-then-write,
  standing in for `DjangoStreamStore._locked_write`'s `select_for_update`.
* **`append_many` is one buffer and one `write()` per segment** under that lock.
  A refused batch writes nothing (#214, #222). Each segment write is atomic, but
  a batch that crosses a segment boundary is two writes, and a crash between
  them leaves the first: recovery keeps that prefix.
* **Deletes and creates take the same lock** as writes, so neither can pull a
  stream out from under an append or reset its head beneath one (#346).

What it deliberately does not do, as shipped: **Windows** (`fcntl` only — the
import is optional so `import rakaia` still works there, and the store refuses
to be constructed rather than run unlocked), **NFS** (`flock` is not reliable
across it), **readers do not take the lock**, and **appends are not broadcast**
to live subscribers the way `DjangoStreamStore` broadcasts over channels, since
this store cannot reach Django. `django_rakaia.checks` reports that last one as
``rakaia.W002`` rather than leaving it to be discovered. One more, added when
it was found rather than left implied: a **symlink inside the root** still leads
a write out of the store, because `_contained` checks containment lexically and
a lexical check cannot see one. See its docstring for why that trade was taken.

Two things this list used to name are now implemented, and the note is kept
because their absence was a documented limitation: **fsync durability** is on by
default (a batch pays one sync per segment, and a new segment also syncs the
directory entry naming it), and **reads skip segments** by filename index rather
than scanning the whole log.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import shutil
import threading
import time
import uuid
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote, unquote

try:
    import fcntl
except ImportError:  # pragma: no cover - Windows has no fcntl
    # Not a soft failure with a degraded lock: without an exclusive lock two
    # writers hand out the same offset, which loses events silently. The store
    # refuses to be constructed instead, and `import rakaia` still works —
    # this module is imported by the package `__init__`, so raising here would
    # take the whole library down on a platform that merely cannot use one
    # backend.
    fcntl = None  # type: ignore[assignment]

from . import stream_rules
from .append_decision import (
    AppendPlan,
    StreamFacts,
    plan_appends,
    plan_initial_write,
)
from .offsets import PLAIN
from .producer import is_producer_state_expired
from .read_decision import check_page_size, page_of
from .types import (
    AppendResult,
    ClosedBy,
    CloseResult,
    ProducerAccepted,
    ProducerState,
    ProducerValidationResult,
    Stream,
    StreamConfigConflict,
    StreamMessage,
    StreamNotFound,
)

_DEFAULT_SEGMENT_SIZE = 10_000


class SegmentSizeMismatch(ValueError):
    """The store was opened with a `segment_size` other than the stream's own.

    Segment filenames are computed from the size, so a store opened with another
    one would put new entries into sealed segments and a resumed read would skip
    some. Open the stream with the size it was written with.
    """


def _discard_torn_tail(fh: Any) -> None:
    """Truncate a partial trailing line before appending after it.

    A crash mid-append leaves bytes with no newline. Reads already skip them —
    but an append that simply continued would glue its own record onto the
    fragment and lose *both*. Recovery has to happen on the write side too, and
    this is the cheapest place: the file is already open and locked. Shared with
    `JsonlOutcomeStore`, whose files have the same shape and the same failure.
    """
    fh.seek(0, os.SEEK_END)
    size = fh.tell()
    if size == 0:
        return
    fh.seek(size - 1)
    if fh.read(1) == b"\n":
        return
    fh.seek(0)
    cut = fh.read().rfind(b"\n") + 1
    fh.truncate(cut)


def _contained(root: Path, name: str, suffix: str = "") -> Path:
    """The path `name` maps to, *proven* to sit directly under `root`.

    A caller-supplied name — a stream path, a consumer id — becomes exactly one
    child of `root`. `quote(safe="")` does the mapping, because a stream path
    contains slashes and a stream is one directory rather than a tree: `/a/b`
    and `/a%2Fb` must not collide, and percent-encoding is the one mapping
    `list_paths` can reverse.

    What makes it safe is not the encoding, though. `quote` leaves a dot alone,
    so a stream called `..` used to resolve *above* the root and write outside
    the store entirely. The tempting repair is to extend the escaping until
    every dangerous name is covered, and that is the losing move: the bad set is
    open-ended, and the next encoding nobody thought of walks straight through.
    So the rule here is the structural one instead — build the path,
    canonicalise it, and check it really is a child of the root. Escaping the
    dots is only the repair attempted when the check says no; the check, not a
    list of known-bad names, is what decides. A name that still escapes after
    the repair is refused rather than written.

    The containment is *lexical*, and that is a real limit rather than a
    detail. The encoded name can never contain a separator, so `normpath`
    collapsing `.` and `..` covers every spelling a caller can put in a name —
    but it cannot see a symlink, and a symlink planted inside the root still
    leads a write out of the store. `Path.resolve()` would catch it — the name
    would be refused rather than misplaced — at the cost of a filesystem walk on
    a path taken on every single append. The trade
    was taken because the two sides are not comparable threats: a name is
    supplied by whoever calls the store, while the contents of the root are the
    deployer's, and anyone who can create a directory entry inside the root can
    write outside it whatever this function does. Named rather than glossed
    over, and pinned by a strict `xfail` in `test_jsonl_containment.py` so the
    limit is found deliberately instead of discovered.

    Found by a containment test rather than by review, which is the part worth
    remembering: an unencoded name still *round-trips*, so reading back what was
    written passes while the file sits somewhere it should never have been.
    """
    base = Path(os.path.normpath(root))
    encoded = quote(name, safe="")
    candidate = Path(os.path.normpath(base / (encoded + suffix)))
    if candidate.parent == base:
        return candidate
    # Outside, so re-encode and re-check. `%00` covers the empty name, which
    # would otherwise collapse a level away rather than escape one.
    encoded = encoded.replace(".", "%2E") or "%00"
    candidate = Path(os.path.normpath(base / (encoded + suffix)))
    if candidate.parent == base:
        return candidate
    raise ValueError(f"name {name!r} does not map to a path under {root}")


#: Where retired offset high-marks live, as a sibling of the stream directories.
#:
#: The leading ``%`` is load-bearing. Stream directories are named
#: ``quote(path, safe="")``, and `quote` escapes ``%`` itself — so no stream path
#: can ever produce this name, while a plainer choice could: ``.retired`` is what
#: this was, and `quote` leaves a leading dot alone, so a stream *called*
#: ``.retired`` would have been handed the watermark directory to live in.
_RETIRED_DIR = "%retired"
_TRASH_GRACE_SECONDS = 60


@dataclass
class _Meta:
    """The cached half of a stream's state — everything not in the log.

    `head` and `next_id` are *also* derivable from the log, and are held here
    only to save a scan. Everything else (closed_by, producer states, the TTL
    window) is not, which is why this file is written with
    write-temp-then-`os.replace` rather than appended to.
    """

    content_type: str | None = None
    ttl_seconds: int | None = None
    expires_at: str | None = None
    created_at: float = 0.0
    last_activity_at: float = 0.0
    last_seq: str | None = None
    closed: bool = False
    closed_by: ClosedBy | None = None
    producers: dict[str, ProducerState] = field(default_factory=dict)
    head: int = 0
    """The highest entry id issued. Also the numeric part of the head offset."""
    segment_size: int | None = None
    """Entries per segment, fixed when the stream is created. Segment filenames
    are computed from it; `JsonlStreamStore._load_meta` refuses a store opened
    with another. ``None`` for a stream written before it was recorded."""
    generation: str | None = None
    """Stamp made when the stream was created; ``None`` for a stream written
    before stamps existed. See `get_generation`."""
    last_event_ts: float | None = None
    """The last `event_ts` this store stamped itself, so its stamps never go
    backwards within the stream (#284). Producer-supplied times are not recorded.
    Lost with the rest of the cache if `meta.json` is: it is not derivable from
    the log, which does not say which times were stamped and which were sent."""

    def to_json(self) -> dict[str, Any]:
        # `last_activity_at` is deliberately absent: it lives in its own file.
        # See `JsonlStreamStore._save_activity`.
        return {
            "content_type": self.content_type,
            "ttl_seconds": self.ttl_seconds,
            "expires_at": self.expires_at,
            "created_at": self.created_at,
            "last_seq": self.last_seq,
            "closed": self.closed,
            "closed_by": (
                None
                if self.closed_by is None
                else {
                    "producer_id": self.closed_by.producer_id,
                    "epoch": self.closed_by.epoch,
                    "seq": self.closed_by.seq,
                }
            ),
            "producers": {
                pid: {
                    "epoch": s.epoch,
                    "last_seq": s.last_seq,
                    "last_updated": s.last_updated,
                }
                for pid, s in self.producers.items()
            },
            "head": self.head,
            "segment_size": self.segment_size,
            "generation": self.generation,
            "last_event_ts": self.last_event_ts,
        }

    @classmethod
    def from_json(cls, raw: dict[str, Any]) -> _Meta:
        cb = raw.get("closed_by")
        return cls(
            content_type=raw.get("content_type"),
            ttl_seconds=raw.get("ttl_seconds"),
            expires_at=raw.get("expires_at"),
            created_at=raw.get("created_at", 0.0),
            last_seq=raw.get("last_seq"),
            closed=raw.get("closed", False),
            closed_by=(
                None
                if cb is None
                else ClosedBy(
                    producer_id=cb["producer_id"], epoch=cb["epoch"], seq=cb["seq"]
                )
            ),
            producers={
                pid: ProducerState(
                    epoch=s["epoch"],
                    last_seq=s["last_seq"],
                    last_updated=s["last_updated"],
                )
                for pid, s in raw.get("producers", {}).items()
            },
            head=raw.get("head", 0),
            segment_size=raw.get("segment_size"),
            generation=raw.get("generation"),
            last_event_ts=raw.get("last_event_ts"),
        )


class JsonlStreamStore:
    """A `rakaia.StreamServerStore` over a directory of JSONL files."""

    def __init__(
        self,
        root: str | Path,
        *,
        segment_size: int = _DEFAULT_SEGMENT_SIZE,
        fsync: bool = True,
        permanent: bool = False,
    ):
        """`fsync` on means an append that has returned has reached the disk.

        On by default because that is what the store it is being measured
        against does: a database commit is durable, and a backend that returned
        from `append` with the record only in the page cache would be claiming
        something weaker while looking the same. A power cut then loses writes
        the caller was told had landed.

        Turn it off for a root on tmpfs, where there is no disk to reach and the
        syscall is pure cost, and for test fixtures. Process death is survived
        either way — the page cache outlives the process — so this is a choice
        about power loss and kernel panics, not about crashes.

        `permanent` keeps every stream for good; see `rakaia.stream_rules`.
        """
        if fcntl is None:  # pragma: no cover - Windows
            raise RuntimeError(
                "JsonlStreamStore needs fcntl.flock, which this platform does "
                "not provide (Windows wants msvcrt.locking, unimplemented). "
                "Without an exclusive lock two writers issue the same offset "
                "and events are lost silently, so the store refuses to start "
                "rather than run unlocked."
            )
        self.root = Path(root)
        self.segment_size = segment_size
        self.fsync = fsync
        self.permanent = permanent
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / _RETIRED_DIR).mkdir(exist_ok=True)

    # =========================================================================
    # Paths on disk
    # =========================================================================

    def _dir(self, path: str) -> Path:
        """The directory for `path`, which `_contained` guarantees is under the root."""
        return _contained(self.root, path)

    def _segment(self, path: str, entry_id: int) -> Path:
        start = (entry_id // self.segment_size) * self.segment_size
        return self._dir(path) / f"{start:012d}.jsonl"

    def _segments(self, path: str) -> list[Path]:
        d = self._dir(path)
        if not d.is_dir():
            return []
        return sorted(d.glob("*.jsonl"))

    def _retired_path(self, path: str) -> Path:
        return _contained(self.root / _RETIRED_DIR, path)

    def _retired(self, path: str) -> int:
        f = self._retired_path(path)
        try:
            return int(f.read_text())
        except (OSError, ValueError):
            return 0

    def _retired_event_ts_path(self, path: str) -> Path:
        # `%` cannot follow a name `quote` produced unless two hex digits do, so
        # no stream's high-mark file can be called this.
        return _contained(self.root / _RETIRED_DIR, path, "%event_ts")

    def _retired_event_ts(self, path: str) -> float | None:
        """The last `event_ts` this store stamped on a deleted `path`, if any.

        Kept beside the retired high mark so a recreated path carries on from
        it, as the durable store's watermark and the in-memory store do (#284).
        """
        try:
            return float(self._retired_event_ts_path(path).read_text())
        except (OSError, ValueError):
            return None

    # =========================================================================
    # Metadata and the lock
    # =========================================================================

    def _load_meta(self, path: str, *, check_segment_size: bool = True) -> _Meta | None:
        f = self._dir(path) / "meta.json"
        try:
            raw = json.loads(f.read_text())
        except FileNotFoundError:
            # No metadata and no log is an absent stream. No metadata *with* a
            # log is a lost cache, and the stream is still there — recovering it
            # is the whole reason the head is derivable from the last line.
            if not self._segments(path):
                return None
            raw = {}
        except (OSError, ValueError):
            # A torn meta.json costs a scan, not the stream: the log is
            # authoritative, so rebuild what is derivable and accept the loss of
            # what is not. This is the property the layout is chosen for.
            raw = {}
        meta = _Meta.from_json(raw)
        # The log is written before `meta.json`, so a process that died between
        # the two leaves a log ahead of its cache. The head is whichever is
        # higher, or the next append reissues an offset already in the log (#346).
        # A fresh `meta.json` costs one read of the last segment's final line.
        meta.head = max(meta.head, self._scan_head(path))
        if meta.segment_size is None:
            # A delete does not care about the size, so it must not be refused
            # for one it cannot work out.
            meta.segment_size = self._segment_size_on_disk(
                path, meta.head, refuse=check_segment_size
            )
        if check_segment_size and meta.segment_size != self.segment_size:
            raise SegmentSizeMismatch(
                f"Stream {path} was written with segment_size="
                f"{meta.segment_size} but this store was opened with "
                f"segment_size={self.segment_size}; open it with "
                f"{meta.segment_size}."
            )
        # Only a stream with a TTL has a sliding window, so only that stream
        # pays the extra read. Loading it unconditionally cost an `open()` on
        # every append to every stream, for a field nothing would consult.
        meta.last_activity_at = (
            self._load_activity(path, meta.created_at)
            if meta.ttl_seconds is not None
            else meta.created_at
        )
        return meta

    def _segment_size_on_disk(
        self, path: str, head: int, *, refuse: bool = True
    ) -> int:
        """The size a stream's files show, for one whose meta never recorded it.

        The second segment starts one size after the first, so the difference of
        their names is the size; the first need not start at 0, because a
        recreated stream resumes above its retired id. A stream still in its
        first segment shows no size, so this store's own is adopted only when
        every entry already sits where that size would put it: the highest id
        (`head`) falls in the one segment on disk, `head // size * size` being
        that segment's start. Otherwise the stream was written with a larger
        size, the old one cannot be known, and the open is refused.
        """
        segments = self._segments(path)
        if len(segments) > 1:
            return int(segments[1].stem) - int(segments[0].stem)
        if (
            refuse
            and segments
            and head // self.segment_size * self.segment_size != int(segments[0].stem)
        ):
            raise SegmentSizeMismatch(
                f"Stream {path} has no recorded segment_size and already holds "
                f"entries up to id {head} in one segment, which a store opened "
                f"with segment_size={self.segment_size} would not have written "
                f"there: it was written with a larger size. Open it with the "
                f"size it was written with."
            )
        return self.segment_size

    def _load_activity(self, path: str, default: float) -> float:
        try:
            return float((self._dir(path) / "activity").read_text())
        except (OSError, ValueError):
            return default

    def _save_activity(self, path: str, when: float) -> None:
        """Write the TTL sliding-window anchor, and nothing else.

        This is the one field a *reader* writes, and it is in its own file for
        exactly that reason. `DjangoStreamStore._touch` extends the window with
        ``save(update_fields=["last_activity_at"])`` — a single-column update
        that cannot disturb a concurrent writer's columns. Metadata here is a
        file replaced whole, so keeping the window inside it would have a reader
        write back a stale head over every append that landed while it was
        reading, and the next append would reissue offsets that already exist.
        A separate file is what buys back the column-level isolation; it also
        means the touch needs no lock, since last-write-wins on a timestamp is
        the correct answer.
        """
        self._replace(self._dir(path) / "activity", str(when))

    def _save_meta(self, path: str, meta: _Meta) -> None:
        self._replace(self._dir(path) / "meta.json", json.dumps(meta.to_json()))

    def _replace(self, target: Path, text: str) -> None:
        """Write `text` to `target` atomically, via a temp file nobody shares.

        The temp name carries the pid and thread id. It used to be a fixed
        ``.tmp``, which two writers raced for: one replaced the file the other
        was still writing, and the loser's `replace` failed with `FileNotFound`
        on a path it had itself created. A shared scratch name is a lock by
        accident, and a broken one.
        """
        tmp = target.with_name(
            f"{target.name}.{os.getpid()}.{threading.get_ident()}.tmp"
        )
        if self.fsync:
            with tmp.open("w") as fh:
                fh.write(text)
                fh.flush()
                os.fsync(fh.fileno())
        else:
            tmp.write_text(text)
        tmp.replace(target)
        if self.fsync:
            self._fsync_dir(target.parent)

    @staticmethod
    def _fsync_dir(directory: Path) -> None:
        """Commit a directory entry — a rename is not durable without it."""
        fd = os.open(directory, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)

    def _scan_head(self, path: str) -> int:
        """The highest entry id in the log, by reading the end of the last segment.

        Reads backwards in blocks until it has one complete line, so the cost is
        a line, not the segment. A trailing line with no newline is a crash
        mid-append and is skipped, as `_read_segment` skips it.
        """
        segments = self._segments(path)
        if not segments:
            return 0
        try:
            with segments[-1].open("rb") as fh:
                end = fh.seek(0, os.SEEK_END)
                size = 4096
                while True:
                    start = max(0, end - size)
                    fh.seek(start)
                    chunk = fh.read(end - start)
                    # Complete lines only: drop the torn tail, and drop the first
                    # fragment unless the chunk reaches the start of the file.
                    lines = chunk.split(b"\n")[:-1]
                    if start > 0:
                        lines = lines[1:]
                    for line in reversed(lines):
                        try:
                            return int(json.loads(line)["id"])
                        except (ValueError, KeyError):
                            continue
                    if start == 0:
                        return 0
                    size *= 2
        except FileNotFoundError:
            return 0

    @contextmanager
    def _locked(
        self, path: str, *, check_expiry: bool = True
    ) -> Iterator[tuple[_Meta, list[dict[str, Any]]]]:
        """Open the stream for writing: metadata loaded, lock held, buffer ready.

        The single door every write goes through — the file-lock equivalent of
        `DjangoStreamStore._locked_write`. Yields the metadata and a line buffer;
        on a clean exit the buffer is appended to the log in one `write()` and
        the metadata is replaced. On an exception nothing is written at all,
        which is what makes a refused batch leave no prefix.
        """
        with self._stream_lock(path):
            meta = self._load_meta(path)
            if meta is None:
                raise StreamNotFound(f"Stream not found: {path}")
            if check_expiry and self._is_expired(meta):
                # Expiry is decided under the lock, exactly as the durable
                # store decides it inside its transaction, and the reap
                # happens on the far side — nothing has been written yet.
                raise _Expired(path)
            buffer: list[dict[str, Any]] = []
            yield meta, buffer
            self._flush(path, buffer)
            self._save_meta(path, meta)

    @contextmanager
    def _stream_lock(self, path: str, *, create: bool = False) -> Iterator[None]:
        """Hold the stream's exclusive lock: the one door for writes, creates and deletes.

        The lock file lives inside the stream's directory, and a delete removes
        that directory while holding it. A waiter then wakes holding a lock on a
        file that no longer exists, so after acquiring it compares the open file
        with the one the directory now names and starts again if they differ. A
        stream that is gone raises `StreamNotFound`, unless `create` is set, in
        which case the directory is made afresh. Not re-entrant: a second
        acquisition from the same process waits for the first.
        """
        # Narrowing, not defence: `__init__` refuses to build a store on a
        # platform without `fcntl`, so by here it is always a module. Stated as
        # an assert rather than an ignore comment, the way `store.py` states the
        # same kind of guarantee about a producer verdict.
        assert fcntl is not None
        d = self._dir(path)
        lock_path = d / ".lock"
        while True:
            if create:
                d.mkdir(parents=True, exist_ok=True)
            try:
                lockfile = lock_path.open("a+")
            except FileNotFoundError:
                if create:
                    continue
                raise StreamNotFound(f"Stream not found: {path}") from None
            try:
                fcntl.flock(lockfile, fcntl.LOCK_EX)
                try:
                    same_file = (
                        os.fstat(lockfile.fileno()).st_ino == lock_path.stat().st_ino
                    )
                except FileNotFoundError:
                    same_file = False
                if not same_file:
                    continue  # deleted (and maybe recreated) while we waited
                yield
                return
            finally:
                lockfile.close()  # closing drops the flock

    def _flush(self, path: str, buffer: list[dict[str, Any]]) -> None:
        """Write the whole buffer, one `write()` per segment it spans."""
        if not buffer:
            return
        by_segment: dict[Path, list[str]] = {}
        for record in buffer:
            seg = self._segment(path, int(record["id"]))
            by_segment.setdefault(seg, []).append(json.dumps(record))
        for seg, lines in by_segment.items():
            existed = seg.exists()
            with seg.open("a+b") as fh:
                _discard_torn_tail(fh)
                fh.write("".join(line + "\n" for line in lines).encode("utf-8"))
                if self.fsync:
                    fh.flush()
                    os.fsync(fh.fileno())
            if self.fsync and not existed:
                # A new segment's *name* lives in the directory, and fsyncing
                # the file does not commit the directory entry that points at
                # it. Without this a roll-over can survive as data nothing can
                # find.
                self._fsync_dir(seg.parent)

    # =========================================================================
    # Records
    # =========================================================================

    @staticmethod
    def _encode(data: bytes) -> dict[str, Any]:
        """A payload as JSON. Text stays readable; anything else is base64.

        Readability is most of the point of this backend, so a UTF-8 payload is
        stored as itself rather than as base64 of itself.
        """
        try:
            return {"data": data.decode("utf-8"), "b64": False}
        except UnicodeDecodeError:
            return {"data": base64.b64encode(data).decode("ascii"), "b64": True}

    @staticmethod
    def _decode(record: dict[str, Any]) -> bytes:
        if record.get("b64"):
            return base64.b64decode(record["data"])
        return str(record["data"]).encode("utf-8")

    @staticmethod
    def _read_segment(segment: Path) -> Iterator[dict[str, Any]]:
        """Every complete record in `segment`.

        A trailing line with no newline is a crash mid-append and is skipped —
        the log's own torn-write recovery, and the reason the head can be
        rebuilt from it at all.
        """
        try:
            text = segment.read_text()
        except FileNotFoundError:
            return
        for line in text.split("\n"):
            if not line:
                continue
            try:
                yield json.loads(line)
            except ValueError:
                # Only ever the last line, and only after a crash.
                continue

    def _relevant_segments(self, path: str, after_id: int) -> list[Path]:
        """The segments that can hold an id above `after_id`.

        This is the file-backed answer to `entries.filter(offset__gt=...)`: a
        resume read on a long stream must not pay for the segments it has
        already consumed. A segment is skippable when the *next* one starts at
        or below `after_id`, because every id it holds is then below that too.

        Expressed against the next segment's start rather than arithmetic on
        `self.segment_size`. The two agree, because a store opened with another
        size than the stream was written with is refused, but the filenames are
        what the data says and need no assumption.
        """
        segments = self._segments(path)
        if after_id <= 0:
            return segments
        first = 0
        for i in range(len(segments) - 1):
            if int(segments[i + 1].stem) > after_id:
                break
            first = i + 1
        return segments[first:]

    def _messages(self, path: str, after_id: int = 0) -> list[StreamMessage]:
        out: list[StreamMessage] = []
        for segment in self._relevant_segments(path, after_id):
            for record in self._read_segment(segment):
                if int(record["id"]) <= after_id:
                    continue
                out.append(
                    StreamMessage(
                        data=self._decode(record),
                        offset=record["offset"],
                        timestamp=record["ts"],
                        event_ts=record.get("event_ts"),
                        label=record.get("label", ""),
                        metadata=record.get("metadata"),
                        tags=tuple(record.get("tags", ())),
                        correlation_id=record.get("correlation_id"),
                    )
                )
        return out

    # =========================================================================
    # Expiry
    # =========================================================================

    def _is_expired(self, meta: _Meta) -> bool:
        return stream_rules.is_expired(
            meta.ttl_seconds,
            meta.expires_at,
            meta.last_activity_at,
            now=time.time(),
            permanent=self.permanent,
        )

    def _live_meta(self, path: str) -> _Meta | None:
        """The stream's metadata, reaping it if it has expired."""
        meta = self._load_meta(path)
        if meta is None:
            return None
        if self._is_expired(meta):
            self.delete(path)
            return None
        return meta

    def _require(self, path: str) -> _Meta:
        meta = self._live_meta(path)
        if meta is None:
            raise StreamNotFound(f"Stream not found: {path}")
        return meta

    def _touch(self, path: str, meta: _Meta) -> None:
        """Extend the sliding TTL window. No-op for a stream without a TTL."""
        if meta.ttl_seconds is None:
            return
        meta.last_activity_at = time.time()
        self._save_activity(path, meta.last_activity_at)

    def touch(self, path: str) -> None:
        meta = self._live_meta(path)
        if meta is not None:
            self._touch(path, meta)

    # =========================================================================
    # Lifecycle
    # =========================================================================

    def create(
        self,
        path: str,
        *,
        content_type: str | None = None,
        ttl_seconds: int | None = None,
        expires_at: str | None = None,
        initial_data: bytes | None = None,
        closed: bool = False,
    ) -> Stream:
        stream_rules.check_expiry_allowed(
            path, ttl_seconds, expires_at, permanent=self.permanent
        )
        # The whole create runs under the stream lock: two creators, or a create
        # racing a delete, must not each decide the stream is absent.
        with self._stream_lock(path, create=True):
            existing = self._load_meta(path)
            if existing is not None and self._is_expired(existing):
                self._delete_files(path, keep_directory=True)
                existing = None
            if existing is not None:
                if stream_rules.same_config(
                    stream_rules.StreamConfig(
                        existing.content_type,
                        existing.ttl_seconds,
                        existing.expires_at,
                        existing.closed,
                    ),
                    stream_rules.StreamConfig(
                        content_type, ttl_seconds, expires_at, closed
                    ),
                    permanent=self.permanent,
                ):
                    return self._as_stream(path, existing)
                raise StreamConfigConflict(
                    f"Stream already exists with different configuration: {path}"
                )

            now = time.time()
            meta = _Meta(
                content_type=content_type,
                ttl_seconds=ttl_seconds,
                expires_at=expires_at,
                created_at=now,
                generation=uuid.uuid4().hex,
                last_activity_at=now,
                closed=closed,
                # A recreated path resumes above the id it retired, so offsets
                # stay globally monotonic across delete-and-recreate (#34).
                head=self._retired(path),
                segment_size=self.segment_size,
                last_event_ts=self._retired_event_ts(path),
            )
            self._save_meta(path, meta)
            self._save_activity(path, now)

            if initial_data:
                # No expiry check: a stream created with `ttl_seconds=0` is
                # already expired the instant it exists, and `create` still has
                # to return it — the reap belongs to the next *read*.
                try:
                    buffer: list[dict[str, Any]] = []
                    self._write_plan(
                        meta, buffer, plan_initial_write(content_type, initial_data)
                    )
                    self._flush(path, buffer)
                    self._save_meta(path, meta)
                except Exception:
                    # A create whose body is refused must leave nothing behind,
                    # not an empty stream where the caller's failed one would
                    # be: the directory is already on disk, so the rollback is
                    # explicit. It also does not retire a high mark, because the
                    # stream it removes never existed as far as the caller knows.
                    self._delete_files(path, retire=False)
                    raise
            return self._as_stream(path, meta)

    def _as_stream(self, path: str, meta: _Meta) -> Stream:
        ttl_seconds, expires_at = stream_rules.reported_expiry(
            meta.ttl_seconds, meta.expires_at, permanent=self.permanent
        )
        return Stream(
            path=path,
            content_type=meta.content_type,
            current_offset=PLAIN.render(meta.head),
            last_seq=meta.last_seq,
            ttl_seconds=ttl_seconds,
            expires_at=expires_at,
            created_at=meta.created_at,
            generation=meta.generation or "",
            last_activity_at=meta.last_activity_at,
            producers=dict(meta.producers),
            closed=meta.closed,
            closed_by=meta.closed_by,
        )

    def get(self, path: str) -> Stream | None:
        meta = self._live_meta(path)
        return None if meta is None else self._as_stream(path, meta)

    def has(self, path: str) -> bool:
        return self._live_meta(path) is not None

    def delete(self, path: str, *, force: bool = False) -> bool:
        """Delete the stream, keeping its retired high mark and last stamp so a
        recreate carries on above them. A permanent store refuses with
        `DeleteNotAllowed` unless `force` is set."""
        stream_rules.check_delete(path, permanent=self.permanent, force=force)
        try:
            with self._stream_lock(path):
                self._delete_files(path)
        except StreamNotFound:
            return False
        return True

    def _delete_files(
        self, path: str, *, keep_directory: bool = False, retire: bool = True
    ) -> None:
        """Retire the high mark and remove the stream. The caller holds the lock.

        `keep_directory` leaves the directory and its lock file, for a create
        that reaps an expired stream and carries on inside the same lock.
        """
        d = self._dir(path)
        # The size does not matter to a delete, so a store opened with the wrong
        # one can still remove what it cannot read.
        meta = self._load_meta(path, check_segment_size=False)
        if retire:
            high = max(self._retired(path), meta.head if meta else 0)
            self._replace(self._retired_path(path), str(high))
            stamps = [
                ts
                for ts in (
                    self._retired_event_ts(path),
                    meta.last_event_ts if meta else None,
                )
                if ts is not None
            ]
            if stamps:
                self._replace(self._retired_event_ts_path(path), repr(max(stamps)))
        if keep_directory:
            for child in d.iterdir():
                if child.name != ".lock":
                    child.unlink()
            return
        # Renamed away in one step, then emptied: a writer queued on the lock
        # sees the directory gone at once. Unlinking the lock file first and the
        # directory after left a gap in which a waiter's `open` recreated it.
        gone = self.root / _RETIRED_DIR / f"%trash-{uuid.uuid4().hex}"
        d.rename(gone)
        # A rename keeps the directory's old mtime, so without this a stream
        # last written over a minute ago would look abandoned to another
        # delete's sweep while this one is still emptying it.
        os.utime(gone)
        shutil.rmtree(gone)
        self._sweep_trash()

    def _sweep_trash(self) -> None:
        """Remove what a delete that died between its rename and its removal left.

        Only directories untouched for a minute, so a delete still emptying its
        own is never raced by another one's sweep.
        """
        cutoff = time.time() - _TRASH_GRACE_SECONDS
        for stale in (self.root / _RETIRED_DIR).glob("%trash-*"):
            try:
                if stale.stat().st_mtime < cutoff:
                    shutil.rmtree(stale, ignore_errors=True)
            except FileNotFoundError:
                continue

    def check_protocol_delete(self, path: str) -> None:
        """Refuse a client's DELETE on a permanent store."""
        stream_rules.check_protocol_delete(path, permanent=self.permanent)

    def clear(self) -> None:
        for child in self.root.iterdir():
            if child.is_dir() and child.name != _RETIRED_DIR:
                for f in child.iterdir():
                    f.unlink()
                child.rmdir()

    def list_paths(self) -> list[str]:
        return [
            unquote(d.name)
            for d in self.root.iterdir()
            if d.is_dir() and d.name != _RETIRED_DIR
        ]

    # =========================================================================
    # Append
    # =========================================================================

    def _write_plan(
        self, meta: _Meta, buffer: list[dict[str, Any]], plan: AppendPlan
    ) -> list[StreamMessage | None]:
        """Buffer the plan's writes, returning per item the last message it
        became, or None for an item that writes nothing.

        Runs under the stream's file lock, which is what makes reading
        `meta.last_event_ts` and advancing it safe.
        """
        writes, stamp = plan.stamped(meta.last_event_ts, time.time())
        if stamp is not None:
            meta.last_event_ts = stamp
        append_time = time.time()
        results: list[StreamMessage | None] = []
        for write in writes:
            if write is None:
                results.append(None)
                continue
            message: StreamMessage | None = None
            for payload in write.payloads:
                meta.head += 1
                offset = PLAIN.render(meta.head)
                record = {
                    "id": meta.head,
                    "offset": offset,
                    "ts": append_time,
                    "event_ts": write.event_ts,
                    "label": write.label,
                    "metadata": write.metadata,
                    **self._encode(payload),
                }
                # Written only when set, so a record without them is
                # byte-for-byte what 0.7 wrote and reads back the same.
                if write.tags:
                    record["tags"] = list(write.tags)
                if write.correlation_id is not None:
                    record["correlation_id"] = write.correlation_id
                buffer.append(record)
                message = StreamMessage(
                    data=payload,
                    offset=offset,
                    timestamp=append_time,
                    event_ts=write.event_ts,
                    label=write.label,
                    metadata=write.metadata,
                    tags=write.tags,
                    correlation_id=write.correlation_id,
                )
            results.append(message)
        return results

    def _producer_state(self, meta: _Meta, producer_id: str | None):
        if producer_id is None:
            return None
        now = time.time()
        for pid in [
            pid
            for pid, state in meta.producers.items()
            if is_producer_state_expired(state, now)
        ]:
            del meta.producers[pid]
        return meta.producers.get(producer_id)

    @staticmethod
    def _commit_producer(meta: _Meta, result: ProducerValidationResult | None) -> None:
        if isinstance(result, ProducerAccepted) and result.proposed_state is not None:
            meta.producers[result.producer_id] = result.proposed_state

    def append(self, path: str, data: bytes, options: Any = None) -> AppendResult:
        return self.append_many(path, [(data, options)])[0]

    @staticmethod
    def _facts(meta: _Meta) -> StreamFacts:
        return StreamFacts(
            closed=meta.closed,
            closed_by=meta.closed_by,
            content_type=meta.content_type,
            last_seq=meta.last_seq,
        )

    def append_many(
        self, path: str, events: Iterable[tuple[bytes, Any]]
    ) -> list[AppendResult]:
        """Append an ordered batch under one lock and one write per segment.

        All-or-nothing, and by construction rather than by care: the shared
        `plan_appends` refuses the whole batch before a single record reaches
        the buffer, and the buffer is not flushed until the block exits cleanly.
        A conflict on item three leaves items one and two unwritten because they
        were never written in the first place (#214).
        """
        items = list(events)
        if not items:
            return []
        try:
            with self._locked(path) as (meta, buffer):
                producer_ids = {
                    pid
                    for _d, o in items
                    if (pid := getattr(o, "producer_id", None)) is not None
                }
                plan = plan_appends(
                    self._facts(meta),
                    [o for _d, o in items],
                    payloads=[d for d, _o in items],
                    producer_states={
                        pid: self._producer_state(meta, pid) for pid in producer_ids
                    },
                    now=time.time(),
                )
                messages = self._write_plan(meta, buffer, plan)
                if plan.writes_anything:
                    self._touch(path, meta)
                for accepted in plan.producer_commits.values():
                    self._commit_producer(meta, accepted)
                meta.last_seq = plan.last_seq
                if plan.close:
                    meta.closed = True
                    meta.closed_by = plan.closed_by
                return [
                    AppendResult(
                        message=message,
                        stream_closed=write.close if write else verdict.stream_closed,
                        producer_result=verdict.producer_result,
                    )
                    for verdict, write, message in zip(
                        plan.verdicts, plan.writes, messages, strict=True
                    )
                ]
        except _Expired:
            self.delete(path)
            raise StreamNotFound(f"Stream not found: {path}") from None

    # =========================================================================
    # Close
    # =========================================================================

    def close_stream(
        self,
        path: str,
        *,
        producer_id: str | None = None,
        producer_epoch: int | None = None,
        producer_seq: int | None = None,
    ) -> CloseResult | None:
        producer = (
            (producer_id, producer_epoch, producer_seq)
            if producer_id is not None
            and producer_epoch is not None
            and producer_seq is not None
            else None
        )
        if self._live_meta(path) is None:
            return None
        try:
            with self._locked(path) as (meta, _buffer):
                verdict = stream_rules.decide_close(
                    closed=meta.closed,
                    closed_by=meta.closed_by,
                    producer=producer,
                    producer_state=(
                        self._producer_state(meta, producer[0]) if producer else None
                    ),
                    now=time.time(),
                )
                if verdict.close:
                    self._commit_producer(meta, verdict.producer_result)
                    meta.closed = True
                    if verdict.closed_by is not None:
                        meta.closed_by = verdict.closed_by
                if verdict.touch:
                    self._touch(path, meta)
                return CloseResult(
                    final_offset=PLAIN.render(meta.head),
                    already_closed=verdict.already_closed,
                    producer_result=verdict.producer_result,
                )
        except _Expired:
            self.delete(path)
            return None

    # =========================================================================
    # Read
    # =========================================================================

    def read(
        self, path: str, offset: str | None = None, *, limit: int | None = None
    ) -> tuple[list[StreamMessage], bool]:
        meta = self._require(path)
        check_page_size(limit)
        self._touch(path, meta)
        # The page is cut after the segment scan, so this bounds the response,
        # not the read: the log is scanned from `offset` as before.
        return page_of(self._read_since(path, offset), limit)

    def peek(self, path: str, offset: str | None) -> list[StreamMessage]:
        """`read` without extending the TTL window; see `StreamServerStore.peek`."""
        self._require(path)
        return self._read_since(path, offset)

    def _read_since(self, path: str, offset: str | None) -> list[StreamMessage]:
        if not offset or offset == "-1":
            return self._messages(path)
        # `PLAIN.key` is the strict parse: it raises `ForeignOffset` for the
        # in-memory store's compound token rather than letting `int()` read the
        # underscore as a digit separator and return a plausible wrong window.
        return self._messages(path, after_id=PLAIN.key(offset)[0])

    def get_current_offset(self, path: str) -> str | None:
        meta = self._live_meta(path)
        return None if meta is None else PLAIN.render(meta.head)

    def get_generation(self, path: str) -> str | None:
        """The stream's creation stamp; ``None`` if absent or expired. Changes
        when the stream is recreated.

        A stream written before stamps existed is given one the first time it is
        asked for, saved under the stream's lock, so a recreate after that is
        told apart from the original.
        """
        meta = self._live_meta(path)
        if meta is None or meta.generation is not None:
            return None if meta is None else meta.generation
        try:
            with self._locked(path) as (locked, _):
                if locked.generation is None:
                    locked.generation = uuid.uuid4().hex
                return locked.generation
        except (_Expired, StreamNotFound):
            return None

    def adopt_generation(self, path: str, generation: str) -> None:
        """Give the stream another log's stamp, because it is a copy of that log.

        For `rakaia.migrate` alone, and only when the copy kept every offset: a
        consumer's saved position then means the same event here, and the stamp
        it saved beside it must not call this a different log. Anywhere else a
        stream's stamp is what says it is *not* the stream another one was.

        Raises:
            StreamNotFound: if `path` is absent or expired.
        """
        try:
            with self._locked(path) as (meta, _buffer):
                meta.generation = generation
        except _Expired:
            self.delete(path)
            raise StreamNotFound(f"Stream not found: {path}") from None

    async def run_sync(self, fn: Any, *args: Any, **kwargs: Any) -> Any:
        """Run a synchronous store call for an async server, in a thread.

        The reason is not the durable store's reason. Django refuses ORM access
        from an async context outright; `open()` does not, and this store's
        first version called straight through on the grounds that file I/O is
        allowed on the event loop.

        Allowed, but not instant, and one call here is not I/O at all — it is
        `flock`, which blocks for as long as *another process* holds the lock.
        On the event loop that is not one slow request, it is every connection
        the server is currently serving, stalled behind a writer it does not
        know about. A batch commit or a fsync on a busy disk does the same thing
        for shorter. The thread hop costs a few tens of microseconds and buys
        back the property an async server exists for.
        """
        return await asyncio.to_thread(fn, *args, **kwargs)


class _Expired(Exception):
    """A stream was found expired under the lock — a signal, not an outcome.

    Same trick as `django_rakaia.django_store._StreamExpired`: the reap has to
    happen outside the write path (which is abandoning everything it did), so
    the discovery leaves by exception and the deletion happens on the far side.
    """

    def __init__(self, path: str) -> None:
        super().__init__(path)
        self.path = path
