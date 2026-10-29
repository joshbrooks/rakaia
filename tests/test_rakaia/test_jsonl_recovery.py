"""Reopening a file store with another segment size, and deleting or creating a
stream while someone writes to it (#346 items 2 and 3).

Every concurrency test forces the interleave by holding the stream lock itself,
so none of them depends on timing luck.
"""

from __future__ import annotations

import fcntl
import threading
import time

import pytest

from rakaia.jsonl_store import JsonlStreamStore, SegmentSizeMismatch, _Meta
from rakaia.offsets import PLAIN
from rakaia.types import StreamNotFound

PATH = "s"


@pytest.fixture
def root(tmp_path):
    return tmp_path / "streams"


def _offsets(store: JsonlStreamStore) -> list[str]:
    return [m.offset for m in store.read(PATH)[0]]


def test_reopening_with_a_different_segment_size_is_refused(root):
    store = JsonlStreamStore(root, segment_size=4)
    store.create(PATH)
    for i in range(6):
        store.append(PATH, str(i).encode())

    wider = JsonlStreamStore(root, segment_size=8)
    with pytest.raises(SegmentSizeMismatch, match=r"4.*8|8.*4"):
        wider.read(PATH)
    with pytest.raises(SegmentSizeMismatch):
        wider.append(PATH, b"x")

    same = JsonlStreamStore(root, segment_size=4)
    first_six = _offsets(same)
    assert len(first_six) == 6
    seventh = same.append(PATH, b"6")
    resumed, _ = same.read(PATH, first_six[-1])
    assert [m.offset for m in resumed] == [seventh.message.offset]


def test_a_stream_written_without_a_recorded_size_adopts_the_size_its_files_show(
    root,
):
    store = JsonlStreamStore(root, segment_size=4)
    store.create(PATH)
    for i in range(6):
        store.append(PATH, str(i).encode())
    (root / PATH / "meta.json").unlink()  # the size went with the cache

    with pytest.raises(SegmentSizeMismatch):
        JsonlStreamStore(root, segment_size=8).read(PATH)
    assert len(_offsets(JsonlStreamStore(root, segment_size=4))) == 6


def test_delete_waits_for_a_writer_that_holds_the_lock(root):
    store = JsonlStreamStore(root, segment_size=4)
    store.create(PATH)
    holding = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []

    def writer() -> None:
        try:
            with store._locked(PATH) as (_meta, _buffer):
                holding.set()
                assert release.wait(timeout=10)
        except BaseException as exc:
            errors.append(exc)

    deleted: list[bool] = []
    t_writer = threading.Thread(target=writer)
    t_writer.start()
    assert holding.wait(timeout=10)
    t_delete = threading.Thread(target=lambda: deleted.append(store.delete(PATH)))
    t_delete.start()
    time.sleep(0.3)
    assert t_delete.is_alive(), "delete did not wait for the writer's lock"
    release.set()
    t_writer.join(timeout=10)
    t_delete.join(timeout=10)

    assert errors == []
    assert deleted == [True]


def test_a_writer_queued_behind_a_delete_gets_stream_not_found(root):
    store = JsonlStreamStore(root, segment_size=4)
    store.create(PATH)
    outcome: list[BaseException | None] = []

    def writer() -> None:
        try:
            store.append(PATH, b"late")
            outcome.append(None)
        except BaseException as exc:
            outcome.append(exc)

    with store._stream_lock(PATH):
        t = threading.Thread(target=writer)
        t.start()
        time.sleep(0.3)
        store._delete_files(PATH)
    t.join(timeout=10)

    assert len(outcome) == 1
    assert isinstance(outcome[0], StreamNotFound), outcome


def test_a_writer_queued_behind_a_delete_does_not_run_on_the_old_lock(root):
    """After delete and recreate the lock file is a new one, and the waiter must
    take that one, not the unlinked file it was queued on."""
    store = JsonlStreamStore(root, segment_size=4)
    store.create(PATH)
    finished = threading.Event()

    def writer() -> None:
        store.append(PATH, b"late")
        finished.set()

    with store._stream_lock(PATH):
        t = threading.Thread(target=writer)
        t.start()
        time.sleep(0.3)
        store._delete_files(PATH)
        (root / PATH).mkdir()
        store._save_meta(PATH, _Meta(segment_size=4))
        new_lock = (root / PATH / ".lock").open("a+")
        fcntl.flock(new_lock, fcntl.LOCK_EX)
    try:
        assert not finished.wait(timeout=0.5), "the waiter ran under the dead lock"
    finally:
        new_lock.close()
    t.join(timeout=10)
    assert finished.is_set()


def test_delete_writes_the_retired_high_mark_atomically(root, monkeypatch):
    store = JsonlStreamStore(root, segment_size=4)
    store.create(PATH)
    store.append(PATH, b"one")
    replaced: list[str] = []
    original = JsonlStreamStore._replace

    def spy(self, target, text):
        replaced.append(target)
        return original(self, target, text)

    monkeypatch.setattr(JsonlStreamStore, "_replace", spy)
    store.delete(PATH)

    assert store._retired_path(PATH) in replaced
    assert not list((root / "%retired").glob("*.tmp"))
    assert store.create(PATH).current_offset == PLAIN.render(1)


def test_create_waits_for_the_stream_lock(root):
    """A create racing a writer must not decide the stream is absent and reset it."""
    store = JsonlStreamStore(root, segment_size=4)
    store.create(PATH)
    finished = threading.Event()

    def create() -> None:
        JsonlStreamStore(root, segment_size=4).create(PATH)
        finished.set()

    with store._stream_lock(PATH):
        t = threading.Thread(target=create)
        t.start()
        assert not finished.wait(timeout=0.3), "create did not wait for the lock"
    t.join(timeout=10)
    assert finished.is_set()
