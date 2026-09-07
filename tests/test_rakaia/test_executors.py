"""Tests for rakaia.executors: the CollectingExecutor (dry-run / verification)
and the RecordingExecutor (apply through, and keep what went past)."""

from __future__ import annotations

import pytest

from rakaia.effects import ApplyReport, Effect, Upsert
from rakaia.executors import (
    CollectingExecutor,
    InMemoryProjections,
    RecordingExecutor,
)
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay
from rakaia.seed import seed_stream
from rakaia.store import StreamStore


def _eff(i: int) -> Effect:
    return Upsert(model_label="x.X", lookup={"id": i}, defaults={})


class TestCollectingExecutor:
    def test_records_and_orders_effects(self):
        ex = CollectingExecutor()
        ex.apply([_eff(1), _eff(2)])
        ex.apply([_eff(3)])
        assert [e.lookup["id"] for e in ex.effects] == [1, 2, 3]

    def test_starts_empty(self):
        assert CollectingExecutor().effects == []

    def test_replay_with_collecting_executor_returns_all_effects(self):
        store = StreamStore()
        reg = HandlerRegistry()

        def h(event):
            return Upsert(
                model_label="x.X",
                lookup={"id": event["id"]},
                defaults={"name": event["name"]},
            )

        reg.register("h", "s", h, 0, None)
        seed_stream("s", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}], store=store)

        ex = CollectingExecutor()
        result = replay(store, "s", ex, handler_registry=reg)

        assert result.effects_applied == 2
        assert [e.lookup["id"] for e in ex.effects] == [1, 2]
        assert [e.defaults["name"] for e in ex.effects] == ["a", "b"]


@pytest.mark.parametrize("n", [0, 1, 5])
def test_apply_length_matches_input(n: int):
    ex = CollectingExecutor()
    ex.apply([_eff(i) for i in range(n)])
    assert len(ex.effects) == n


class _Boom(Exception):
    pass


class _Inner:
    """A minimal inner executor: keeps each batch, and can be told to raise."""

    def __init__(self, report=None, raise_on=None):
        self.batches: list[list[Effect]] = []
        self._report = report
        self._raise_on = raise_on

    def apply(self, effects):
        batch = list(effects)
        self.batches.append(batch)
        if self._raise_on is not None and len(self.batches) == self._raise_on:
            raise _Boom("inner executor failed")
        return self._report


class TestRecordingExecutor:
    def test_records_what_reached_the_inner_executor_in_order(self):
        inner = _Inner()
        rec = RecordingExecutor(inner)
        rec.apply([_eff(1), _eff(2)])
        rec.apply([_eff(3)])

        assert [e.lookup["id"] for e in rec.effects] == [1, 2, 3]
        assert [[e.lookup["id"] for e in b] for b in inner.batches] == [[1, 2], [3]]

    def test_starts_empty(self):
        assert RecordingExecutor(_Inner()).effects == []

    def test_the_inner_report_passes_through_unchanged(self):
        report = ApplyReport(upserts_created=1, upserts_written=2, upserts_skipped=3)
        rec = RecordingExecutor(_Inner(report=report))

        assert rec.apply([_eff(1)]) is report

    def test_a_none_report_passes_through_as_none(self):
        assert RecordingExecutor(_Inner()).apply([_eff(1)]) is None

    def test_a_generator_is_materialised_before_the_inner_executor_sees_it(self):
        """The recorder walks the batch, so a one-shot iterable would otherwise
        reach the inner executor already exhausted -- recorded as verified,
        applied nowhere."""
        inner = _Inner()
        rec = RecordingExecutor(inner)
        rec.apply(e for e in [_eff(1), _eff(2)])

        assert [e.lookup["id"] for e in rec.effects] == [1, 2]
        assert [e.lookup["id"] for e in inner.batches[0]] == [1, 2]

    def test_an_inner_exception_propagates_and_what_reached_it_stays_recorded(self):
        rec = RecordingExecutor(_Inner(raise_on=2))
        rec.apply([_eff(1)])
        with pytest.raises(_Boom):
            rec.apply([_eff(2)])

        assert [e.lookup["id"] for e in rec.effects] == [1, 2]

    def test_replay_through_the_recorder_records_and_applies(self):
        store = StreamStore()
        reg = HandlerRegistry()

        def h(event):
            return Upsert(
                model_label="x.X",
                lookup={"id": event["id"]},
                defaults={"name": event["name"]},
            )

        reg.register("h", "s", h, 0, None)
        seed_stream("s", [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}], store=store)

        proj = InMemoryProjections()
        rec = RecordingExecutor(proj)
        result = replay(store, "s", rec, handler_registry=reg)

        assert result.effects_applied == 2
        assert [e.lookup["id"] for e in rec.effects] == [1, 2]
        assert [r["name"] for r in proj.rows("x.X")] == ["a", "b"]
