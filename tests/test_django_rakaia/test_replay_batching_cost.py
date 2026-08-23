"""What buffering a stage actually buys, in statements (#207).

The two claims from the issue, measured against the durable executor rather than
asserted. Both are about a *replay*, which is the point: `test_update_batching.py`
already shows the collapse working when a caller hands `apply()` a full fan-out
itself, and it always did. What it could never do was engage from the path that
generates the effects, because that path called `apply()` once per event and a run
of one is never collapsed.
"""

from __future__ import annotations

import pytest
from django.core.exceptions import FieldError
from django.db import connection
from django.test.utils import CaptureQueriesContext

from django_rakaia.effect_executor import DjangoExecutor
from rakaia.effects import Update, Upsert
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay
from rakaia.seed import seed_stream
from rakaia.store import StreamStore

from .models import FinanceLine

pytestmark = pytest.mark.django_db

MODEL = "test_django_rakaia.FinanceLine"
MATCH = "s"


def _statements(ctx: CaptureQueriesContext, verb: str) -> list[str]:
    return [
        q["sql"]
        for q in ctx.captured_queries
        if q["sql"].lstrip().upper().startswith(verb)
    ]


def _replay_nine(executor) -> CaptureQueriesContext:
    """Nine events, each fanning one identical `Update` at its own row — the
    shape the issue measured on an SF 2.3 form save."""
    ids = [f"r{i}" for i in range(9)]
    for i in ids:
        FinanceLine.objects.create(submission_id=i, suku="s", delta=0)

    store = StreamStore()
    seed_stream("s", [{"id": i} for i in ids], store=store)

    registry = HandlerRegistry()
    registry.register(
        name="h",
        event_match=MATCH,
        fn=lambda ev: Update(
            model_label=MODEL, lookup={"submission_id": ev["id"]}, defaults={"delta": 7}
        ),
        effective_from=0,
        stage=0,
    )

    with CaptureQueriesContext(connection) as ctx:
        replay(store, "s", executor, handler_registry=registry, event_match=MATCH)
    return ctx


def test_a_nine_event_replay_now_enters_one_transaction():
    """Nine `apply()` calls meant nine SAVEPOINT/RELEASE pairs — 18 statements,
    which the issue measured as 13% of a form save. One batch, one pair.

    Mutation: leave `ctx.buffer` at None in `run_passes` and this reports nine.
    """
    ctx = _replay_nine(DjangoExecutor())

    savepoints = _statements(ctx, "SAVEPOINT")
    assert len(savepoints) == 1, "one atomic block for the stage, not one per event"
    assert len(_statements(ctx, "RELEASE")) == 1


def test_the_collapse_added_in_203_finally_engages_from_a_replay():
    """The issue's actual complaint: the batching is unreachable from the path
    that produces the effects. Nine updates, one statement.

    Mutation: as above — without buffering each run has one member, the
    `len(run) == 1` branch wins, and this reports nine UPDATEs.
    """
    ctx = _replay_nine(DjangoExecutor(batch_updates=True))

    updates = _statements(ctx, "UPDATE")
    assert len(updates) == 1
    assert " IN (" in updates[0].upper()
    assert set(FinanceLine.objects.values_list("delta", flat=True)) == {7}


def test_the_rows_are_the_same_without_the_collapse_enabled():
    """Buffering is not the collapse. With `batch_updates` off the statements
    stay one per row — only the transaction count changes — and either way the
    rows land identically."""
    ctx = _replay_nine(DjangoExecutor())

    assert len(_statements(ctx, "UPDATE")) == 9
    assert set(FinanceLine.objects.values_list("delta", flat=True)) == {7}


def test_a_failure_part_way_through_a_pass_discards_the_whole_batch():
    """The consumer-visible cost of batching, and the reason it is in UPGRADING.

    `DjangoExecutor` wraps each `apply()` in a transaction. One call per event
    meant an earlier event's write survived a later event's failure; one call per
    pass means it does not. Nothing is *wrong* either way — the exception still
    propagates and the replay still fails — but per-event commit granularity
    within a pass is no longer on offer, and a consumer could have leaned on it.

    Measured both ways rather than asserted: with `ctx.buffer` forced to None the
    surviving rows are `['good']`, and with batching they are `[]`. That contrast
    is the whole content of the upgrade note.
    """
    store = StreamStore()
    seed_stream("s", [{"id": "good"}, {"id": "bad"}], store=store)

    registry = HandlerRegistry()
    registry.register(
        name="h",
        event_match=MATCH,
        effective_from=0,
        stage=0,
        # The second event writes a column that does not exist, so its write
        # fails inside the batch the first event's write is also in.
        fn=lambda ev: Upsert(
            MODEL,
            {"submission_id": ev["id"]},
            {"suku": "s", "delta": 1} if ev["id"] == "good" else {"nope": 1},
        ),
    )

    with pytest.raises(FieldError):
        replay(
            store, "s", DjangoExecutor(), handler_registry=registry, event_match=MATCH
        )

    assert list(FinanceLine.objects.values_list("submission_id", flat=True)) == [], (
        "the earlier event's write is rolled back with the batch it shared"
    )
