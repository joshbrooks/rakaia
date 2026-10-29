"""Stateful properties of the file store across crashes and restarts (GitHub #346).

A process can die after the log line is written and before `meta.json` is, and
the next process opens the same directory. Whatever happens, an offset handed
out must never be handed out again, and nothing acknowledged may go missing.
"""

from __future__ import annotations

import shutil
import tempfile

import pytest
from hypothesis import HealthCheck, settings
from hypothesis import strategies as st
from hypothesis.stateful import RuleBasedStateMachine, invariant, rule

from rakaia.jsonl_store import JsonlStreamStore

PATH = "s"


class _Killed(BaseException):
    """Stands in for the process dying; not an `Exception`, so nothing swallows it."""


class JsonlStoreMachine(RuleBasedStateMachine):
    def __init__(self):
        super().__init__()
        self.root = tempfile.mkdtemp(prefix="rakaia-jsonl-")
        # A tiny segment so batches cross segment boundaries.
        self.store = self._open()
        self.store.create(PATH)
        self.acknowledged: list[tuple[str, bytes]] = []
        self.crashed: list[bytes] = []
        self.counter = 0

    def _open(self) -> JsonlStreamStore:
        return JsonlStreamStore(self.root, segment_size=4, fsync=False)

    def _payload(self) -> bytes:
        self.counter += 1
        return f"event-{self.counter}".encode()

    def teardown(self):
        shutil.rmtree(self.root, ignore_errors=True)

    @rule(size=st.integers(1, 5))
    def append_batch(self, size):
        events = [(self._payload(), None) for _ in range(size)]
        results = self.store.append_many(PATH, events)
        for (data, _), result in zip(events, results, strict=True):
            assert result.message is not None
            self.acknowledged.append((result.message.offset, data))

    @rule()
    def append_then_die_before_meta(self):
        data = self._payload()

        def die(_path, _meta):
            raise _Killed

        self.store._save_meta = die  # type: ignore[method-assign]
        with pytest.raises(_Killed):
            self.store.append(PATH, data)
        # The process is gone: nothing of this object is used again.
        self.crashed.append(data)
        self.store = self._open()

    @rule()
    def reopen(self):
        self.store = self._open()

    @invariant()
    def offsets_are_unique_and_increasing(self):
        offsets = [m.offset for m in self.store.read(PATH)[0]]
        assert offsets == sorted(set(offsets)), f"offsets repeat or go back: {offsets}"
        acked = [o for o, _ in self.acknowledged]
        assert acked == sorted(set(acked)), f"acknowledged offsets: {acked}"

    @invariant()
    def every_acknowledged_event_reads_back_in_order(self):
        read = [(m.offset, m.data) for m in self.store.read(PATH)[0]]
        remaining = iter(read)
        for entry in self.acknowledged:
            assert entry in remaining, f"lost or reordered {entry}; log holds {read}"


JsonlStoreMachine.TestCase.settings = settings(
    max_examples=50,
    stateful_step_count=20,
    deadline=None,
    suppress_health_check=[HealthCheck.too_slow],
)

TestJsonlStore = JsonlStoreMachine.TestCase
