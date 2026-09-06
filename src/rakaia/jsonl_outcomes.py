"""An `OutcomeStore` backed by plain JSONL files on disk.

The second implementation, and the reason the seam exists — but be careful what
the pair proves. It shows the *storing* is store-agnostic: keeping records in
files needs no method the protocol lacks, and a third backend found a difference
neither of these two had on its first run against the shared contract.

It does **not** show the codec is right. Decision 6b makes both stores share
`encode`, `decode` and `_order`, so a defect in any of the three is applied
identically by both, agreed on by both, and invisible to the contract suite and
to the cross-store comparison alike. That was the trade: one shared rendering
buys structural agreement and spends the independence that would have made
agreement evidence. What covers the codec is `test_outcome_validation.py`, which
tests it directly rather than through a store. An earlier version of this
paragraph said the two stores "share no code", which was never true.

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
trailing line — a crash mid-append — is cut off before the next append, so that
record is not glued onto it and lost as well. One that is read costs that line
and not the report, and is reported as a line this version cannot build: the
reader cannot tell a crash fragment from a record written by another version,
and does not try to.

What the lock orders is not the write itself — with `O_APPEND` a single short
`write()` lands whole on a local filesystem — but the torn-tail check against a
concurrent append: find the fragment, truncate, write. The test that pins it
holds a writer inside that window and checks a second one waits, because a
second writer that gets through is the one that can lose a line.
"""

from __future__ import annotations

import os
from pathlib import Path
from urllib.parse import quote

from .jsonl_store import _discard_torn_tail
from .outcomes import Outcome, _order, decode, encode

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
        line = (encode(outcome) + "\n").encode("utf-8")
        # Narrowing, not defence: `__init__` refuses to build a store on a platform
        # without `fcntl`, so by here it is always a module. Stated as an assert
        # rather than an ignore comment, the way `jsonl_store.py` states the same
        # guarantee.
        assert fcntl is not None
        # Append under an exclusive lock so two writers cannot interleave halves
        # of a line. `a+` means the write is positioned at the end at write time,
        # not at open time, and the file is readable, which the torn-tail check
        # needs: it has to see the last byte before deciding whether to cut.
        with target.open("a+b") as fh:
            fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
            try:
                _discard_torn_tail(fh)
                fh.write(line)
                fh.flush()
                if self.fsync:
                    os.fsync(fh.fileno())
            finally:
                fcntl.flock(fh.fileno(), fcntl.LOCK_UN)

    def latest(self, consumer: str, stream_path: str) -> list[Outcome]:
        best: dict[str, Outcome] = {}
        for record in self._read(consumer, stream_path):
            # The file name says which scope this is, but the line says so too,
            # and the line wins: on a case-folding filesystem two consumers can
            # share a file, and a hand-edited file can hold anything.
            if record.consumer != consumer or record.stream_path != stream_path:
                continue
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
            # `decode` takes the stored text directly — the same text `encode`
            # produced and the same text the in-memory store keeps. A torn line
            # from a crash mid-append, and a line this version cannot rebuild,
            # are the same case to it: that line is lost, never the report.
            outcome = decode(line)
            if outcome is not None:
                out.append(outcome)
        return out
