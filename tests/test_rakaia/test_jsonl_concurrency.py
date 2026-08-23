"""What the JSONL store's lock is actually holding (#229).

The spike's first pass had an `flock` that no test needed: deleting it left all
117 tests passing, which is the exact failure mode `CLAUDE.md` warns about for
the durable store's `select_for_update` — two earlier attempts there passed with
and without the lock.

So this file is the JSONL counterpart of `test_concurrent_appends.py` and
`test_locking.py`, and it is held to the same standard: **every test here names
the mechanism it holds, and has been shown to fail when that mechanism is
removed.** A test that passes either way is not evidence, and does not belong.

Two mechanisms are under test, not one. Five tests hold the `flock` in
`_locked`. One holds the *separation* of the TTL window into its own file,
which is what stops a reader clobbering a writer — the durable store gets that
from `save(update_fields=[...])` and this store has to construct it.

Threads rather than processes for most of it, and that is not a weaker test:
`flock` is associated with the *open file description*, not the process, so two
`open()` calls contend even inside one interpreter. `fcntl.flock` releases the
GIL while it blocks, so a blocked thread really blocks. One test does use real
subprocesses, because the claim that retires the in-memory store is specifically
a cross-*process* claim.
"""

from __future__ import annotations

import asyncio
import json
import subprocess
import sys
import threading
import time

import pytest

from rakaia.jsonl_store import JsonlStreamStore
from rakaia.types import AppendOptions

PATH = "s"


def _run(*targets, timeout: float = 30) -> None:
    """Run the targets concurrently, surfacing the first failure."""
    errors: list[BaseException] = []

    def wrap(fn):
        def inner() -> None:
            try:
                fn()
            except BaseException as exc:
                errors.append(exc)

        return inner

    threads = [threading.Thread(target=wrap(t)) for t in targets]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=timeout)
    for t in threads:
        assert not t.is_alive(), "a racing writer deadlocked"
    if errors:
        raise errors[0]


@pytest.fixture
def root(tmp_path):
    return tmp_path / "streams"


@pytest.fixture
def store(root):
    s = JsonlStreamStore(root, segment_size=8)
    s.create(PATH)
    return s


def _offsets_on_disk(root, path: str = PATH) -> list[str]:
    """Every offset in the log, read straight off the files.

    Deliberately not through `read()`: a store method could agree with itself
    about an offset it never wrote. The files are the evidence.
    """
    out = []
    for segment in sorted((root / path).glob("*.jsonl")):
        out.extend(
            json.loads(line)["offset"]
            for line in segment.read_text().split("\n")
            if line
        )
    return out


def test_many_concurrent_writers_produce_no_duplicate_offsets(store, root):  # noqa: ARG001
    """Four writers, one stream, forty events, forty distinct offsets.

    The realistic case. Without the lock two writers read the same head from
    `meta.json`, both issue the id after it, and the log ends up with two
    records claiming one offset — a resume read would then skip an event.
    """
    writers = 4
    per_writer = 10

    def writer(n: int):
        def go() -> None:
            own = JsonlStreamStore(root, segment_size=8)
            for i in range(per_writer):
                own.append(PATH, json.dumps({"w": n, "i": i}).encode())

        return go

    _run(*[writer(n) for n in range(writers)])

    offsets = _offsets_on_disk(root)
    assert len(offsets) == writers * per_writer
    assert len(set(offsets)) == len(offsets), "two writers were handed one offset"
    assert offsets == sorted(offsets), "offsets were not issued in write order"


def test_a_second_writer_blocks_until_the_first_has_written(store, root):
    """The forced interleave: B must not read A's head mid-flight.

    Writer A takes the lock and holds it while B tries to append. If the lock
    holds, B waits and then reads the head A left behind. If it does not, B
    reads the stale head and issues the offset A has already taken.
    """
    got: dict[str, str] = {}
    a_holding = threading.Event()
    b_started = threading.Event()

    def writer_a() -> None:
        with store._locked(PATH) as (meta, buffer):
            store._write(meta, buffer, b'{"w": "a"}')
            got["a"] = meta_head(meta)
            a_holding.set()
            # Stay inside the lock until B has had a fair chance to reach it.
            assert b_started.wait(timeout=10)
            time.sleep(0.5)

    def writer_b() -> None:
        assert a_holding.wait(timeout=10)
        b_started.set()
        other = JsonlStreamStore(root, segment_size=8)
        result = other.append(PATH, b'{"w": "b"}')
        assert result.message is not None
        got["b"] = result.message.offset

    _run(writer_a, writer_b)

    assert got["a"] != got["b"], f"both writers took the same offset: {got}"
    assert got["b"] > got["a"], (
        f"B was handed an offset at or below A's ({got}). The lock did not hold."
    )
    assert _offsets_on_disk(root) == [got["a"], got["b"]]


def meta_head(meta) -> str:
    from rakaia.offsets import PLAIN

    return PLAIN.render(meta.head)


def test_a_reader_does_not_lose_a_concurrent_writers_head(root, monkeypatch):
    """A read extends the TTL window, and that must not cost an append.

    The durable store extends its window with a single-column row update, which
    cannot lose a concurrent writer's work. This store's metadata is one file
    replaced whole, so a reader that loaded the metadata, was descheduled, and
    then wrote it back would roll the head backwards over every append that
    landed in between — and the next append would reissue offsets that already
    exist. The window lives in its own file for exactly this reason.

    The interleave is forced, not raced. An earlier version of this test ran a
    reader and a writer flat out and asserted the head survived; it passed with
    the separation removed, because the gap between the reader's load and its
    write-back is a few microseconds and nothing ever landed inside it. A test
    that needs luck to see the bug will not see it.
    """
    reader_store = JsonlStreamStore(root, segment_size=8)
    reader_store.create(PATH, ttl_seconds=3600)
    reader_store.append(PATH, b'{"n": 0}')

    loaded = threading.Event()
    written = threading.Event()
    original = JsonlStreamStore._load_meta

    def pausing_load(self, path):
        meta = original(self, path)
        # Only the reader pauses, and only on its first load: it now holds a
        # snapshot of the metadata while the writer advances the real one.
        if self is reader_store and not loaded.is_set():
            loaded.set()
            assert written.wait(timeout=10)
        return meta

    monkeypatch.setattr(JsonlStreamStore, "_load_meta", pausing_load)

    def reader() -> None:
        reader_store.read(PATH)

    def writer() -> None:
        assert loaded.wait(timeout=10)
        own = JsonlStreamStore(root, segment_size=8)
        for i in range(1, 6):
            own.append(PATH, json.dumps({"n": i}).encode())
        written.set()

    _run(reader, writer)

    offsets = _offsets_on_disk(root)
    assert len(offsets) == 6
    fresh = JsonlStreamStore(root, segment_size=8)
    assert fresh.get_current_offset(PATH) == offsets[-1], (
        "the head fell behind the log: the reader wrote back stale metadata"
    )
    result = fresh.append(PATH, b'{"n": 6}')
    assert result.message is not None
    assert result.message.offset not in offsets, "an existing offset was reissued"


def test_an_append_cannot_overtake_a_committed_close(store, root):
    """Once a close is written, an append that was already in flight is refused.

    The durable store gets this from the stream row lock. Here the closing
    writer holds the file lock, so the appending writer cannot have decided
    `closed=False` before the close landed — it has not read the metadata yet.
    """
    order: list[str] = []
    closing = threading.Event()
    append_started = threading.Event()

    def closer() -> None:
        with store._locked(PATH) as (meta, _buffer):
            meta.closed = True
            closing.set()
            assert append_started.wait(timeout=10)
            time.sleep(0.5)
            order.append("closed")

    def appender() -> None:
        assert closing.wait(timeout=10)
        append_started.set()
        own = JsonlStreamStore(root, segment_size=8)
        result = own.append(PATH, b'{"late": true}')
        order.append("appended")
        assert result.stream_closed is True, (
            "an append overtook a committed close and was accepted"
        )
        assert result.message is None

    _run(closer, appender)

    assert order == ["closed", "appended"]
    assert _offsets_on_disk(root) == []


def test_a_close_cannot_be_won_twice(store, root):
    """Two closers, one winner.

    Forced rather than raced: the first closer holds the lock with the close
    already written to its metadata, and the second must therefore find the
    stream closed. Four closers running flat out settled on one winner most of
    the time even with the lock removed, which makes it a coin toss, not a test.
    """
    verdicts: dict[str, bool] = {}
    holding = threading.Event()
    second_started = threading.Event()

    def first() -> None:
        with store._locked(PATH) as (meta, _buffer):
            meta.closed = True
            verdicts["first"] = False
            holding.set()
            assert second_started.wait(timeout=10)
            time.sleep(0.5)

    def second() -> None:
        assert holding.wait(timeout=10)
        second_started.set()
        own = JsonlStreamStore(root, segment_size=8)
        result = own.close_stream(PATH)
        assert result is not None
        verdicts["second"] = result.already_closed

    _run(first, second)

    assert verdicts["second"] is True, (
        "the second closer did not see the first one's close"
    )
    assert store.get(PATH).closed is True


def test_a_batch_waits_for_an_append_in_flight(store, root):
    """A batch draws its whole run of ids from the head an in-flight append left.

    The durable store gets this from allocating its offset block inside the
    transaction that holds the stream row. Here the batch must wait for the
    lock, then read the advanced head — if it reads the stale one, its twenty
    ids start on top of the append's.

    Note what is *not* asserted: that the batch's records are contiguous. They
    are, but they would be with no lock at all, because a batch is a single
    `write()`. The first version of this test checked only that, and so proved
    nothing.
    """
    got: dict[str, list[str]] = {}
    holding = threading.Event()
    batch_started = threading.Event()

    def appender() -> None:
        with store._locked(PATH) as (meta, buffer):
            store._write(meta, buffer, b'{"single": true}')
            got["single"] = [meta_head(meta)]
            holding.set()
            assert batch_started.wait(timeout=10)
            time.sleep(0.5)

    def batcher() -> None:
        assert holding.wait(timeout=10)
        batch_started.set()
        own = JsonlStreamStore(root, segment_size=8)
        results = own.append_many(
            PATH, [(json.dumps({"b": i}).encode(), AppendOptions()) for i in range(20)]
        )
        got["batch"] = [r.message.offset for r in results if r.message]

    _run(appender, batcher)

    assert len(got["batch"]) == 20
    assert min(got["batch"]) > got["single"][0], (
        f"the batch overlapped the in-flight append: {got['single']} vs "
        f"{got['batch'][:3]}"
    )
    offsets = _offsets_on_disk(root)
    assert len(offsets) == 21
    assert len(set(offsets)) == 21, "a batch and an append shared an offset"


_CHILD = """
import json, os, sys, time
from rakaia.jsonl_store import JsonlStreamStore

root, path, tag, count, gate = sys.argv[1], sys.argv[2], sys.argv[3], int(sys.argv[4]), sys.argv[5]
store = JsonlStreamStore(root, segment_size=8)

# Announce readiness, then block until every sibling is also up. Without this
# the children serialise themselves by interpreter startup — a few tens of
# milliseconds each, far longer than the appends — and the test passes with the
# lock removed, having never had two writers in flight at once.
open(os.path.join(gate, "ready-" + tag), "w").close()
deadline = time.time() + 30
while not os.path.exists(os.path.join(gate, "go")):
    if time.time() > deadline:
        raise SystemExit("gate never opened")
    time.sleep(0.005)

for i in range(count):
    store.append(path, json.dumps({"tag": tag, "i": i}).encode())
"""


def test_separate_processes_append_to_one_log_without_collision(
    store,  # noqa: ARG001 - creates the stream the children append to
    root,
    tmp_path,
):
    """The cross-process claim, tested across real processes.

    Everything else in this file runs in one interpreter, where `flock` still
    contends but the writers share an address space. This is the claim that
    would let the in-memory store be retired: four operating-system processes,
    no shared state but the directory, no lost or duplicated offsets.
    """
    child = tmp_path / "child.py"
    child.write_text(_CHILD)
    gate = tmp_path / "gate"
    gate.mkdir()

    procs = [
        subprocess.Popen(
            [sys.executable, str(child), str(root), PATH, f"p{n}", "40", str(gate)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )
        for n in range(4)
    ]
    deadline = time.time() + 60
    while len(list(gate.glob("ready-*"))) < len(procs):
        assert time.time() < deadline, "a child never started"
        time.sleep(0.01)
    (gate / "go").touch()

    for p in procs:
        _out, err = p.communicate(timeout=60)
        assert p.returncode == 0, err.decode()

    offsets = _offsets_on_disk(root)
    assert len(offsets) == 160
    assert len(set(offsets)) == 160, "two processes were handed one offset"
    assert offsets == sorted(offsets)

    reader = JsonlStreamStore(root, segment_size=8)
    assert len(reader.read(PATH)[0]) == 160
    assert reader.get_current_offset(PATH) == offsets[-1]


_WRITER = """
import json, os, sys, time
from rakaia.jsonl_store import JsonlStreamStore

root, path, delay = sys.argv[1], sys.argv[2], float(sys.argv[3])
time.sleep(delay)
JsonlStreamStore(root).append(path, json.dumps({"from": "another process"}).encode())
"""


@pytest.mark.asyncio
async def test_a_long_poll_wakes_for_a_write_from_another_process(
    store, root, tmp_path
):
    """The reason `wait_for_messages` polls instead of waiting on an event.

    The in-memory store notifies its waiters with an `asyncio.Event`, which is
    correct there and useless here: the append that a live subscriber is waiting
    for comes from a worker, a management command, another web process. This is
    that case, and nothing but the filesystem connects the two sides.
    """
    writer = tmp_path / "writer.py"
    writer.write_text(_WRITER)
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(writer),
        str(root),
        PATH,
        "0.2",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        head = store.get_current_offset(PATH)
        messages, timed_out, closed = await store.wait_for_messages(PATH, head, 10.0)
    finally:
        _out, err = await proc.communicate()
        assert proc.returncode == 0, err.decode()

    assert timed_out is False
    assert closed is False
    assert [json.loads(m.data) for m in messages] == [{"from": "another process"}]


@pytest.mark.asyncio
async def test_a_long_poll_still_times_out_when_nothing_writes(store):
    """The other half: a waiter with no writer returns empty, not hung."""
    head = store.get_current_offset(PATH)
    messages, timed_out, closed = await store.wait_for_messages(PATH, head, 0.3)

    assert (messages, timed_out, closed) == ([], True, False)


@pytest.mark.asyncio
async def test_the_protocol_server_serves_a_write_from_another_process(
    store, root, tmp_path
):
    """The same claim one layer up, over HTTP.

    A client long-polls the protocol server; the event it is waiting for is
    written by a separate operating-system process. This is the live-tailing
    path a deployment actually runs, and it is the one an in-memory store
    cannot serve at all once there is more than one worker.
    """
    from tests.asgi_client import asgi_client

    # The protocol server keys a stream by the request path as it arrives —
    # leading slash included — so the writer has to name the stream the way the
    # server will, not the way a direct store call would.
    served = f"/{PATH}"
    store.create(served)

    writer = tmp_path / "writer.py"
    writer.write_text(_WRITER)
    proc = await asyncio.create_subprocess_exec(
        sys.executable,
        str(writer),
        str(root),
        served,
        "0.3",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    try:
        async with asgi_client(store, long_poll_timeout=10.0) as ac:
            head = store.get_current_offset(served)
            response = await ac.get(f"{served}?offset={head}&live=long-poll")
    finally:
        _out, err = await proc.communicate()
        assert proc.returncode == 0, err.decode()

    assert response.status_code == 200
    assert b"another process" in response.content
