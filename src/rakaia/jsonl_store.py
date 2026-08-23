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
* **`append_many` is one buffer and one `write()`** under that lock, so a batch
  cannot leave a prefix behind (#214, #222).

What it deliberately does not do: Windows (`fcntl` only), fsync durability,
segment-skipping reads (it scans), or any attempt to make readers lock-free
correctly. Those are the questions for the real implementation, not for the
question this spike is asking.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import threading
import time
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
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

from .append_decision import StreamFacts, decide_append, decide_append_batch
from .context import merge_provenance
from .json_mode import (
    format_json_response,
    is_json_content_type,
    normalize_content_type,
    process_json_append,
)
from .offsets import PLAIN
from .producer import is_producer_state_expired
from .types import (
    AppendOptions,
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

_POLL_INTERVAL_SECONDS = 0.05

_DEFAULT_SEGMENT_SIZE = 10_000

#: Where retired offset high-marks live, as a sibling of the stream directories.
#:
#: The leading ``%`` is load-bearing. Stream directories are named
#: ``quote(path, safe="")``, and `quote` escapes ``%`` itself — so no stream path
#: can ever produce this name, while a plainer choice could: ``.retired`` is what
#: this was, and `quote` leaves a leading dot alone, so a stream *called*
#: ``.retired`` would have been handed the watermark directory to live in.
_RETIRED_DIR = "%retired"


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
        )


class JsonlStreamStore:
    """A `rakaia.StreamServerStore` over a directory of JSONL files."""

    def __init__(
        self,
        root: str | Path,
        *,
        segment_size: int = _DEFAULT_SEGMENT_SIZE,
        fsync: bool = True,
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
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / _RETIRED_DIR).mkdir(exist_ok=True)
        self._producer_locks: dict[str, asyncio.Lock] = {}

    # =========================================================================
    # Paths on disk
    # =========================================================================

    def _dir(self, path: str) -> Path:
        """The directory for `path`.

        `quote(safe="")` because a stream path contains slashes and a stream is
        one directory, not a tree: `/a/b` and `/a%2Fb` must not collide, and
        percent-encoding is the one mapping that is reversible for `list_paths`.
        """
        return self.root / quote(path, safe="")

    def _segment(self, path: str, entry_id: int) -> Path:
        start = (entry_id // self.segment_size) * self.segment_size
        return self._dir(path) / f"{start:012d}.jsonl"

    def _segments(self, path: str) -> list[Path]:
        d = self._dir(path)
        if not d.is_dir():
            return []
        return sorted(d.glob("*.jsonl"))

    def _retired_path(self, path: str) -> Path:
        return self.root / _RETIRED_DIR / quote(path, safe="")

    def _retired(self, path: str) -> int:
        f = self._retired_path(path)
        try:
            return int(f.read_text())
        except (OSError, ValueError):
            return 0

    # =========================================================================
    # Metadata and the lock
    # =========================================================================

    def _load_meta(self, path: str) -> _Meta | None:
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
        if not raw:
            meta.head = self._scan_head(path)
        # Only a stream with a TTL has a sliding window, so only that stream
        # pays the extra read. Loading it unconditionally cost an `open()` on
        # every append to every stream, for a field nothing would consult.
        meta.last_activity_at = (
            self._load_activity(path, meta.created_at)
            if meta.ttl_seconds is not None
            else meta.created_at
        )
        return meta

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
        """The highest entry id in the log, by reading the last segment.

        The recovery path: what the head *is* when `meta.json` cannot say.
        """
        segments = self._segments(path)
        if not segments:
            return 0
        for record in reversed(list(self._read_segment(segments[-1]))):
            return int(record["id"])
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
        d = self._dir(path)
        if not d.is_dir():
            raise StreamNotFound(f"Stream not found: {path}")
        # Narrowing, not defence: `__init__` refuses to build a store on a
        # platform without `fcntl`, so by here it is always a module. Stated as
        # an assert rather than an ignore comment, the way `store.py` states the
        # same kind of guarantee about a producer verdict.
        assert fcntl is not None
        with (d / ".lock").open("a+") as lockfile:
            fcntl.flock(lockfile, fcntl.LOCK_EX)
            try:
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
            finally:
                fcntl.flock(lockfile, fcntl.LOCK_UN)

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
                self._discard_torn_tail(fh)
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

    @staticmethod
    def _discard_torn_tail(fh: Any) -> None:
        """Truncate a partial trailing line before appending after it.

        A crash mid-append leaves bytes with no newline. Reads already skip them
        — but an append that simply continued would glue its own record onto the
        fragment and lose *both*. Recovery has to happen on the write side too,
        and this is the cheapest place: the file is already open and locked.
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

        Deliberately expressed against the next segment's start rather than
        arithmetic on `self.segment_size`. The filenames record the boundaries
        as they were when the records were written, and the store can be
        reopened with a different `segment_size` — sizing is a constructor
        argument, not a property of the data. Arithmetic against the current
        size would skip a segment that a differently-sized past had filled.
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
                    )
                )
        return out

    # =========================================================================
    # Expiry
    # =========================================================================

    @staticmethod
    def _is_expired(meta: _Meta) -> bool:
        now = time.time()
        if (
            meta.ttl_seconds is not None
            and now - meta.last_activity_at > meta.ttl_seconds
        ):
            return True
        if meta.expires_at is not None:
            try:
                expires = datetime.fromisoformat(meta.expires_at.replace("Z", "+00:00"))
                if expires.tzinfo is None:
                    expires = expires.replace(tzinfo=timezone.utc)
                if now > expires.timestamp():
                    return True
            except ValueError:
                return False
        return False

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
        existing = self._live_meta(path)
        if existing is not None:
            if (
                normalize_content_type(content_type)
                == normalize_content_type(existing.content_type)
                and ttl_seconds == existing.ttl_seconds
                and expires_at == existing.expires_at
                and closed == existing.closed
            ):
                return self._as_stream(path, existing)
            raise StreamConfigConflict(
                f"Stream already exists with different configuration: {path}"
            )

        now = time.time()
        d = self._dir(path)
        d.mkdir(parents=True, exist_ok=True)
        meta = _Meta(
            content_type=content_type,
            ttl_seconds=ttl_seconds,
            expires_at=expires_at,
            created_at=now,
            last_activity_at=now,
            closed=closed,
            # A recreated path resumes above the id it retired, so offsets stay
            # globally monotonic across delete-and-recreate (#34).
            head=self._retired(path),
        )
        self._save_meta(path, meta)
        self._save_activity(path, now)

        if initial_data:
            # `check_expiry=False`: a stream created with `ttl_seconds=0` is
            # already expired the instant it exists, and `create` still has to
            # return it — the reap belongs to the next *read*, not to the
            # creation. Reload afterwards for the same reason `_require` cannot
            # be used here.
            try:
                with self._locked(path, check_expiry=False) as (m, buffer):
                    self._write(m, buffer, initial_data, is_initial_create=True)
            except Exception:
                # A create whose body is refused must leave nothing behind, not
                # an empty stream where the caller's failed one would be. The
                # other two stores get this from unwinding before they register
                # the stream at all; here the directory is already on disk, so
                # the rollback has to be explicit. Without it a `create` that
                # raised still reaped the expired stream it was replacing and
                # then left its own replacement — visible to `ls`, absent from
                # the API, and holding the path against a later create with a
                # different content type.
                self.delete(path)
                raise
            meta = self._load_meta(path) or meta
        return self._as_stream(path, meta)

    def _as_stream(self, path: str, meta: _Meta) -> Stream:
        return Stream(
            path=path,
            content_type=meta.content_type,
            current_offset=PLAIN.render(meta.head),
            last_seq=meta.last_seq,
            ttl_seconds=meta.ttl_seconds,
            expires_at=meta.expires_at,
            created_at=meta.created_at,
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

    def delete(self, path: str) -> bool:
        d = self._dir(path)
        if not d.is_dir():
            return False
        meta = self._load_meta(path)
        high = max(self._retired(path), meta.head if meta else 0)
        self._retired_path(path).write_text(str(high))
        for child in d.iterdir():
            child.unlink()
        d.rmdir()
        return True

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

    def _write(
        self,
        meta: _Meta,
        buffer: list[dict[str, Any]],
        data: bytes,
        *,
        is_initial_create: bool = False,
        label: str = "",
        metadata: dict | None = None,
        event_ts: float | None = None,
    ) -> StreamMessage | None:
        """Turn one append into one or more buffered records.

        JSON mode flattens a posted array one level, exactly as the other two
        stores do — each element becomes its own entry with its own offset.
        """
        if is_json_content_type(meta.content_type):
            payloads = process_json_append(data, is_initial_create)
            if not payloads:
                return None
        else:
            payloads = [data]

        append_time = time.time()
        message: StreamMessage | None = None
        for payload in payloads:
            meta.head += 1
            offset = PLAIN.render(meta.head)
            record = {
                "id": meta.head,
                "offset": offset,
                "ts": append_time,
                "event_ts": event_ts if event_ts is not None else append_time,
                "label": label,
                "metadata": metadata,
                **self._encode(payload),
            }
            buffer.append(record)
            message = StreamMessage(
                data=payload,
                offset=offset,
                timestamp=append_time,
                event_ts=record["event_ts"],
                label=label,
                metadata=metadata,
            )
        return message

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
        opts = options or AppendOptions()
        try:
            with self._locked(path) as (meta, buffer):
                verdict = decide_append(
                    self._facts(meta),
                    opts,
                    producer_state=self._producer_state(meta, opts.producer_id),
                    now=time.time(),
                )
                if not verdict.write:
                    return AppendResult(
                        message=None,
                        stream_closed=verdict.stream_closed,
                        producer_result=verdict.producer_result,
                    )
                message = self._write(
                    meta,
                    buffer,
                    data,
                    label=opts.label,
                    metadata=merge_provenance(opts.metadata),
                    event_ts=opts.event_ts,
                )
                self._touch(path, meta)
                self._commit_producer(meta, verdict.producer_result)
                if opts.seq is not None:
                    meta.last_seq = opts.seq
                if opts.close:
                    self._mark_closed(meta, opts)
                return AppendResult(
                    message=message,
                    producer_result=verdict.producer_result,
                    stream_closed=opts.close,
                )
        except _Expired:
            self.delete(path)
            raise StreamNotFound(f"Stream not found: {path}") from None

    @staticmethod
    def _facts(meta: _Meta) -> StreamFacts:
        return StreamFacts(
            closed=meta.closed,
            closed_by=meta.closed_by,
            content_type=meta.content_type,
            last_seq=meta.last_seq,
        )

    @staticmethod
    def _mark_closed(meta: _Meta, opts: Any) -> None:
        meta.closed = True
        producer_id = getattr(opts, "producer_id", None)
        if producer_id is not None:
            meta.closed_by = ClosedBy(
                producer_id=producer_id,
                epoch=getattr(opts, "producer_epoch", None) or 0,
                seq=getattr(opts, "producer_seq", None) or 0,
            )

    def append_many(
        self, path: str, events: Iterable[tuple[bytes, Any]]
    ) -> list[AppendResult]:
        """Append an ordered batch under one lock and one write per segment.

        All-or-nothing, and by construction rather than by care: the shared
        `decide_append_batch` refuses the whole batch before a single record
        reaches the buffer, and the buffer is not flushed until the block exits
        cleanly. A conflict on item three leaves items one and two unwritten
        because they were never written in the first place (#214).
        """
        items = list(events)
        if not items:
            return []
        try:
            with self._locked(path) as (meta, buffer):
                options = [o for _d, o in items]
                payloads = [d for d, _o in items]
                producer_ids = {
                    pid
                    for o in options
                    if (pid := getattr(o, "producer_id", None)) is not None
                }
                batch = decide_append_batch(
                    self._facts(meta),
                    options,
                    payloads=payloads,
                    producer_states={
                        pid: self._producer_state(meta, pid) for pid in producer_ids
                    },
                    now=time.time(),
                )
                results: list[AppendResult] = []
                for (data, opts), verdict in zip(items, batch.verdicts, strict=True):
                    if not verdict.write:
                        results.append(
                            AppendResult(
                                message=None,
                                stream_closed=verdict.stream_closed,
                                producer_result=verdict.producer_result,
                            )
                        )
                        continue
                    o = opts or AppendOptions()
                    message = self._write(
                        meta,
                        buffer,
                        data,
                        label=o.label,
                        metadata=merge_provenance(o.metadata),
                        event_ts=o.event_ts,
                    )
                    results.append(
                        AppendResult(
                            message=message,
                            producer_result=verdict.producer_result,
                            stream_closed=bool(o.close),
                        )
                    )
                if batch.writes_anything:
                    self._touch(path, meta)
                for accepted in batch.producer_commits.values():
                    self._commit_producer(meta, accepted)
                if batch.last_seq is not None:
                    meta.last_seq = batch.last_seq
                if batch.closing_opts is not None:
                    self._mark_closed(meta, batch.closing_opts)
                return results
        except _Expired:
            self.delete(path)
            raise StreamNotFound(f"Stream not found: {path}") from None

    def _get_producer_lock(self, path: str, producer_id: str) -> asyncio.Lock:
        key = f"{path}:{producer_id}"
        if key not in self._producer_locks:
            self._producer_locks[key] = asyncio.Lock()
        return self._producer_locks[key]

    async def append_with_producer(
        self, path: str, data: bytes, options: Any = None
    ) -> AppendResult:
        opts = options or AppendOptions()
        if not opts.producer_id:
            return self.append(path, data, opts)
        async with self._get_producer_lock(path, opts.producer_id):
            return self.append(path, data, opts)

    # =========================================================================
    # Close
    # =========================================================================

    def close_stream(self, path: str) -> CloseResult | None:
        if self._live_meta(path) is None:
            return None
        try:
            with self._locked(path) as (meta, _buffer):
                already = meta.closed
                meta.closed = True
                self._touch(path, meta)
                return CloseResult(
                    final_offset=PLAIN.render(meta.head), already_closed=already
                )
        except _Expired:
            self.delete(path)
            return None

    async def close_stream_with_producer(
        self, path: str, producer_id: str, producer_epoch: int, producer_seq: int
    ) -> CloseResult | None:
        async with self._get_producer_lock(path, producer_id):
            if self._live_meta(path) is None:
                return None
            try:
                with self._locked(path) as (meta, _buffer):
                    verdict = decide_append(
                        StreamFacts(closed=meta.closed, closed_by=meta.closed_by),
                        AppendOptions(
                            producer_id=producer_id,
                            producer_epoch=producer_epoch,
                            producer_seq=producer_seq,
                        ),
                        producer_state=self._producer_state(meta, producer_id),
                        now=time.time(),
                    )
                    if not verdict.write:
                        return CloseResult(
                            final_offset=PLAIN.render(meta.head),
                            already_closed=verdict.stream_closed,
                            producer_result=verdict.producer_result,
                        )
                    self._commit_producer(meta, verdict.producer_result)
                    meta.closed = True
                    meta.closed_by = ClosedBy(
                        producer_id=producer_id, epoch=producer_epoch, seq=producer_seq
                    )
                    self._touch(path, meta)
                    return CloseResult(
                        final_offset=PLAIN.render(meta.head),
                        already_closed=False,
                        producer_result=verdict.producer_result,
                    )
            except _Expired:
                self.delete(path)
                return None

    # =========================================================================
    # Read
    # =========================================================================

    def read(
        self, path: str, offset: str | None = None
    ) -> tuple[list[StreamMessage], bool]:
        meta = self._require(path)
        self._touch(path, meta)
        return self._read_since(path, offset), True

    def _read_since(self, path: str, offset: str | None) -> list[StreamMessage]:
        """The messages after `offset`, without extending the TTL window.

        Split out from `read` for the same reason as
        `DjangoStreamStore._read_since`: long-poll checks for new messages on
        every tick and must not write the activity file on every tick with it.
        """
        if not offset or offset == "-1":
            return self._messages(path)
        # `PLAIN.key` is the strict parse: it raises `ForeignOffset` for the
        # in-memory store's compound token rather than letting `int()` read the
        # underscore as a digit separator and return a plausible wrong window.
        return self._messages(path, after_id=PLAIN.key(offset)[0])

    def get_current_offset(self, path: str) -> str | None:
        meta = self._live_meta(path)
        return None if meta is None else PLAIN.render(meta.head)

    def format_response(self, path: str, messages: list[StreamMessage]) -> bytes:
        meta = self._require(path)
        if is_json_content_type(meta.content_type):
            return format_json_response([m.data for m in messages])
        return b"".join(m.data for m in messages)

    async def wait_for_messages(
        self, path: str, offset: str, timeout_seconds: float
    ) -> tuple[list[StreamMessage], bool, bool]:
        """Long-poll by polling the log.

        No `asyncio.Event`: the whole point of a file-backed store is that the
        appends come from another process, which an in-process event would never
        see. Same shape as `DjangoStreamStore.wait_for_messages`, including the
        two asymmetries — an absent stream on the *first* pass is a
        `StreamNotFound`, but a stream that expires *mid-wait* is an ordinary
        timeout rather than a 404.
        """
        deadline = time.monotonic() + timeout_seconds
        entered = False
        while True:
            # Every tick's file access goes through a thread, for the same
            # reason `run_sync` does: a poll that reads a large segment on the
            # event loop stalls every other connection while it does.
            meta = await asyncio.to_thread(self._live_meta, path)
            if meta is None:
                if not entered:
                    raise StreamNotFound(f"Stream not found: {path}")
                return [], True, False
            entered = True

            messages = await asyncio.to_thread(self._read_since, path, offset)
            if messages:
                # Once, on the way out — not on every tick. Polling through
                # `read` would rewrite the activity file twenty times a second
                # for every waiter on a stream with a TTL.
                await asyncio.to_thread(self._touch, path, meta)
                return messages, False, False
            if meta.closed:
                return [], False, True

            remaining = deadline - time.monotonic()
            if remaining <= 0:
                return [], True, False
            await asyncio.sleep(min(_POLL_INTERVAL_SECONDS, remaining))

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
