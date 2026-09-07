"""Writes, then deletes, then retires — decided in one place (#256).

The rule was stated three times: once for the buffer that decides where a batch
boundary has to fall, and once inside each executor's apply loop. Nothing pinned
the buffer against either executor, so two of the three could agree while the
third drifted. This suite asserts all three read the same rule object *and* that
each of them applies what that object says.

Both halves earn their place. Restoring a local copy to the buffer was caught by
the identity half only; restoring the three hard-coded passes to an executor left
the import in place and was caught by the behaviour half only.
"""

from __future__ import annotations

from importlib import import_module
from types import ModuleType

import pytest

from django_rakaia.effect_executor import DjangoExecutor
from rakaia.effects import ApplyReport, Delete, Effect, Upsert
from rakaia.executors import InMemoryProjections
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay
from rakaia.seed import seed_stream
from rakaia.store import StreamStore

# Imported by name, not as `from rakaia import replay`: the package re-exports
# the `replay` *function* under that name, so the attribute lookup would hand
# back a function and every assertion below would be about the wrong object.
effects_module = import_module("rakaia.effects")
replay_module = import_module("rakaia.replay")
executors_module = import_module("rakaia.executors")
effect_executor = import_module("django_rakaia.effect_executor")

MATCH = "s"


def _retires_first(effect: Effect) -> int:
    """The shared rule, stood on its head: retires, then deletes, then writes.

    Nothing may depend on the ordering being this one — only on every site
    reading it from the same place. A site that consults the rule reverses with
    it; a site carrying its own copy does not.
    """
    return 2 - effects_module._write_order_rank(effect)


class _Batches:
    """An executor that records how many calls carried the stage's effects."""

    def __init__(self) -> None:
        self.batches: list[list[Effect]] = []

    def apply(self, effects) -> ApplyReport:
        self.batches.append(list(effects))
        return ApplyReport()


@pytest.mark.parametrize(
    "module",
    [replay_module, executors_module, effect_executor],
    ids=["buffer", "in_memory_executor", "django_executor"],
)
def test_every_site_reads_the_one_rule(module: ModuleType) -> None:
    """One rule object, three readers — not three functions that agree today."""
    assert module._write_order_rank is effects_module._write_order_rank


def test_the_buffer_takes_its_batch_boundary_from_the_rule(monkeypatch) -> None:
    """A delete then a write is two batches only because the rule says a write
    outranks a delete. Under a reversed rule the pair is orderable as emitted,
    so the buffer must hold them in one batch."""
    monkeypatch.setattr(replay_module, "_write_order_rank", _retires_first)
    store = StreamStore()
    seed_stream("s", [{"id": "e0", "n": 0}, {"id": "e1", "n": 1}], store=store)
    registry = HandlerRegistry()
    registry.register(
        name="h",
        event_match=MATCH,
        fn=lambda ev: (
            Delete("app.R", {"id": "x"})
            if ev["n"] == 0
            else Upsert("app.R", {"id": "x"}, {"n": 1})
        ),
        effective_from=0,
        stage=0,
    )
    ex = _Batches()

    replay(store, "s", ex, handler_registry=registry, event_match=MATCH)

    assert [len(b) for b in ex.batches] == [2]


def test_the_in_memory_executor_takes_its_passes_from_the_rule(monkeypatch) -> None:
    """Write then delete leaves no row; reversed, the delete runs first and the
    write survives."""
    monkeypatch.setattr(executors_module, "_write_order_rank", _retires_first)
    ex = InMemoryProjections()

    ex.apply([Upsert("app.R", {"id": "x"}, {"n": 1}), Delete("app.R", {"id": "x"})])

    assert [(r["id"], r["n"]) for r in ex.rows("app.R")] == [("x", 1)]


@pytest.mark.django_db
def test_the_django_executor_takes_its_passes_from_the_rule(monkeypatch) -> None:
    """The same batch, the same reversal, on the durable executor."""
    monkeypatch.setattr(effect_executor, "_write_order_rank", _retires_first)
    from tests.test_django_rakaia.models import Alert

    lookup = {"stream_key": "s", "alert_type": "a", "field_key": ""}
    DjangoExecutor().apply(
        [
            Upsert("test_django_rakaia.Alert", lookup, {"message": "kept"}),
            Delete("test_django_rakaia.Alert", lookup),
        ]
    )

    assert [a.message for a in Alert.objects.all()] == ["kept"]
