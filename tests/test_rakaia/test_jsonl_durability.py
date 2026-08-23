"""What survives what (#229).

`DjangoStreamStore` inherits durability from its database: an append that has
returned has been committed, and a commit is on disk. A file-backed store gets
none of that for free, and "the bytes are in the page cache" looks identical to
"the bytes are on the disk" right up until the moment it doesn't.

So the two claims are separated here and tested separately. Process death is
survived with or without `fsync`, because the page cache outlives the process.
Power loss is survived only with it.
"""

from __future__ import annotations

import json
import os
import subprocess
import sys

import pytest

from rakaia.jsonl_store import JsonlStreamStore
from rakaia.types import AppendOptions

PATH = "s"


@pytest.fixture
def root(tmp_path):
    return tmp_path / "streams"


@pytest.fixture
def fsyncs(monkeypatch):
    """Every `os.fsync` the store issues."""
    seen: list[int] = []
    original = os.fsync

    def counting(fd):
        seen.append(fd)
        return original(fd)

    monkeypatch.setattr("rakaia.jsonl_store.os.fsync", counting)
    return seen


_CHILD = """
import json, os, sys
from rakaia.jsonl_store import JsonlStreamStore

root, path, count = sys.argv[1], sys.argv[2], int(sys.argv[3])
store = JsonlStreamStore(root)
store.create(path)
for i in range(count):
    store.append(path, json.dumps({"n": i}).encode())

# No flush, no close, no atexit, no unwinding: the process dies here, between
# an append returning and anything tidy happening.
os.kill(os.getpid(), 9)
"""


def test_a_killed_writer_leaves_every_append_it_returned_from(root, tmp_path):
    """SIGKILL after the last append. Everything it was told had landed, landed.

    This is the claim a caller actually relies on, and the one an in-memory
    store cannot make at all. Note what is *not* being tested: buffering. If
    the store held records in a Python-side buffer and flushed on close, this
    test would fail — which is the point of killing the process rather than
    returning from it.
    """
    child = tmp_path / "child.py"
    child.write_text(_CHILD)

    proc = subprocess.run(
        [sys.executable, str(child), str(root), PATH, "50"],
        capture_output=True,
        check=False,
    )
    assert proc.returncode == -9, f"child did not die as expected: {proc.stderr!r}"

    survivor = JsonlStreamStore(root)
    messages, _ = survivor.read(PATH)
    assert [m.data for m in messages] == [
        json.dumps({"n": i}).encode() for i in range(50)
    ]
    assert survivor.get_current_offset(PATH) == messages[-1].offset


def test_a_killed_writer_leaves_a_log_that_can_be_appended_to(root, tmp_path):
    """Recovery is not read-only: the survivor must be writable, with no gap
    and no reissued offset where the dead process stopped."""
    child = tmp_path / "child.py"
    child.write_text(_CHILD)
    subprocess.run(
        [sys.executable, str(child), str(root), PATH, "20"],
        capture_output=True,
        check=False,
    )

    survivor = JsonlStreamStore(root)
    before = [m.offset for m in survivor.read(PATH)[0]]
    result = survivor.append(PATH, b'{"after": true}')

    assert result.message is not None
    assert result.message.offset not in before
    assert result.message.offset > before[-1]
    assert len(survivor.read(PATH)[0]) == 21


def test_an_append_reaches_the_disk_before_it_returns(root, fsyncs):
    """With `fsync` on, an append syncs the segment it wrote."""
    store = JsonlStreamStore(root)
    store.create(PATH)
    fsyncs.clear()

    store.append(PATH, b'{"n": 1}')

    assert fsyncs, "an append returned without syncing anything"


def test_fsync_can_be_turned_off_for_a_root_with_no_disk_behind_it(root, fsyncs):
    """The tmpfs case: no syscall at all, because there is nothing to sync to."""
    store = JsonlStreamStore(root, fsync=False)
    store.create(PATH)
    fsyncs.clear()

    store.append(PATH, b'{"n": 1}')

    assert fsyncs == []


def test_a_new_segment_syncs_its_directory_entry(root, fsyncs):
    """A roll-over creates a file, and fsyncing a file does not commit the name
    that points at it. Without the directory sync a segment can survive a power
    cut as data nothing can find."""
    store = JsonlStreamStore(root, segment_size=4)
    store.create(PATH)
    for i in range(3):
        store.append(PATH, json.dumps({"n": i}).encode())

    fsyncs.clear()
    store.append(PATH, b'{"n": 3}')  # rolls into a new segment

    assert len(fsyncs) >= 2, (
        "a roll-over synced the file but not the directory entry naming it"
    )


def test_a_batch_pays_one_sync_per_segment_not_one_per_item(root, fsyncs):
    """Durability must not make `append_many` linear in the batch size.

    A batch is one write per segment, so it is one sync per segment — the
    property that makes fsync-by-default affordable for bulk writes.
    """
    store = JsonlStreamStore(root, segment_size=1000)
    store.create(PATH)
    fsyncs.clear()

    store.append_many(
        PATH, [(json.dumps({"n": i}).encode(), AppendOptions()) for i in range(200)]
    )
    batched = len(fsyncs)

    fsyncs.clear()
    for i in range(200):
        store.append(PATH, json.dumps({"n": i}).encode())
    one_by_one = len(fsyncs)

    assert batched < one_by_one / 10, (
        f"a batch of 200 cost {batched} syncs against {one_by_one} for the loop"
    )
