"""An `OutcomeStore` backed by plain JSONL files on disk.

The second implementation, and the reason the seam exists. `InMemoryOutcomeStore`
proves nothing on its own — the same commit wrote it and the protocol, so of
course it fits. This one has no database under it and shares no code with the
first, so `tests/outcome_store_contract.py` passing against both is the evidence
that an outcome is as store-agnostic as ADR 0007 Decision 1 claims.

Outcomes are **append-only by definition** (Decision 6a), which makes a log file
the natural shape rather than a compromise: `record` is one line appended, and
nothing ever rewrites a line. Where the stream store needs segments, sealing and
a head cache, this needs none of them — there is no head to track, and the
population is exceptions only.

Layout, one file per consumer and stream::

    <root>/<quoted consumer>/<quoted stream path>.jsonl

`quote(safe="")` on both components for the reason `JsonlStreamStore._dir` gives:
a stream path contains slashes and must map to one name, not a tree, and
percent-encoding is the mapping that is reversible.

Sharing the stream store's limits, and for the same reasons: `fcntl` only, so no
Windows; `flock` is unreliable on NFS; readers do not take the lock. A torn
trailing line — a crash mid-append — is skipped on read rather than parsed,
because a partial outcome is not worth failing a whole report over.
"""

from __future__ import annotations

import json
import os
from dataclasses import asdict, fields
from pathlib import Path
from urllib.parse import quote

from .outcomes import Outcome, _order

_FIELDS = {f.name for f in fields(Outcome)}

try:  # pragma: no cover - platform dependent
    import fcntl
except ImportError:  # pragma: no cover - Windows
    fcntl = None  # type: ignore[assignment]


def _safe_name(name: str) -> str:
    """One path segment for `name`, guaranteed to stay under its parent.

    `quote(safe="")` maps a slash to `%2F`, which is what keeps a stream path a
    name rather than a tree, and it is reversible. What it does **not** touch is a
    dot: `quote("..")` is `".."`, so a consumer or stream called `..` resolves
    above the root and writes outside the store entirely. Only an all-dot name can
    do that — anything else already carries an encoded character — so those are the
    ones encoded here, and an empty name, which would otherwise collapse a
    directory level.

    Found by a containment test rather than by review, and that is the point worth
    keeping: an unencoded name still *round-trips*, so reading back what was
    written passes while the file sits somewhere it should never have been.
    """
    quoted = quote(name, safe="")
    if not quoted or set(quoted) <= {"."}:
        return quoted.replace(".", "%2E") or "%00"
    return quoted


class JsonlOutcomeStore:
    """Outcomes kept as JSONL, one file per `(consumer, stream_path)`."""

    def __init__(self, root: str | Path, *, fsync: bool = True):
        """`fsync` on means a recorded outcome has reached the disk when `record` returns.

        On by default for the reason `JsonlStreamStore` gives: the backend it is
        measured against is a database, whose commit is durable, and returning
        with the record only in the page cache would claim something weaker while
        looking the same. Turn it off for tmpfs and fixtures.
        """
        if fcntl is None:  # pragma: no cover - Windows
            raise RuntimeError(
                "JsonlOutcomeStore needs fcntl.flock, which this platform does not "
                "provide. Two writers appending unlocked can interleave a partial "
                "line, so the store refuses to start rather than run unlocked."
            )
        self.root = Path(root)
        self.fsync = fsync
        self.root.mkdir(parents=True, exist_ok=True)

    def _file(self, consumer: str, stream_path: str) -> Path:
        return self.root / _safe_name(consumer) / f"{_safe_name(stream_path)}.jsonl"

    def record(self, outcome: Outcome) -> None:
        target = self._file(outcome.consumer, outcome.stream_path)
        target.parent.mkdir(parents=True, exist_ok=True)
        line = json.dumps(asdict(outcome), sort_keys=True) + "\n"
        # Narrowing, not defence: `__init__` refuses to build a store on a platform
        # without `fcntl`, so by here it is always a module. Stated as an assert
        # rather than an ignore comment, the way `jsonl_store.py` states the same
        # guarantee.
        assert fcntl is not None
        # Append under an exclusive lock so two writers cannot interleave halves
        # of a line. `a` mode means the write is positioned at the end at write
        # time, not at open time, so the lock is what orders them rather than a
        # seek this would otherwise have to do itself.
        with target.open("a", encoding="utf-8") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            try:
                fh.write(line)
                fh.flush()
                if self.fsync:
                    os.fsync(fh.fileno())
            finally:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    def latest(self, consumer: str, stream_path: str) -> list[Outcome]:
        best: dict[str, Outcome] = {}
        for record in self._read(consumer, stream_path):
            held = best.get(record.subject)
            if held is None or record.attempt >= held.attempt:
                best[record.subject] = record
        return sorted(best.values(), key=_order)

    def _read(self, consumer: str, stream_path: str) -> list[Outcome]:
        target = self._file(consumer, stream_path)
        if not target.is_file():
            return []
        out: list[Outcome] = []
        for line in target.read_text(encoding="utf-8").splitlines():
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except ValueError:
                # A torn trailing line from a crash mid-append. Skipping it
                # loses one outcome; failing the read would lose the report.
                continue
            payload["reasons"] = tuple(payload.get("reasons", ()))
            try:
                out.append(
                    Outcome(**{k: v for k, v in payload.items() if k in _FIELDS})
                )
            except (TypeError, ValueError):
                # A line this version cannot build: a field added by a later
                # version, or one removed. Unknown keys are dropped rather than
                # passed through, because `Outcome(**payload)` raises on an extra
                # one — and a single such line would otherwise take the whole
                # report down, which is exactly what the torn-line handling above
                # exists to prevent.
                continue
        return out
