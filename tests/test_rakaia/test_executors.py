"""Tests for rakaia.executors: the CollectingExecutor (dry-run / verification)."""

from __future__ import annotations

import json

import pytest

from rakaia.effects import Effect
from rakaia.executors import CollectingExecutor
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay
from rakaia.store import StreamStore


def _eff(i: int) -> Effect:
    return Effect(
        op="update_or_create",
        model_label="x.X",
        lookup={"id": i},
        defaults={},
    )


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
            return Effect(
                op="update_or_create",
                model_label="x.X",
                lookup={"id": event["id"]},
                defaults={"name": event["name"]},
            )

        reg.register("h", "s", h, 0, None)
        store.create("s")
        for ev in [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}]:
            store.append("s", json.dumps(ev).encode("utf-8"))

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
