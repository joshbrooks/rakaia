"""Tests for DjangoProjectionReader + end-to-end staged replay over the ORM."""

from __future__ import annotations

import json

import pytest

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.projection_reader import DjangoProjectionReader
from django_rakaia.store import get_store
from rakaia.effects import Effect
from rakaia.registry import HandlerRegistry, UpcasterRegistry
from rakaia.replay import merge_replay, replay

from .models import Area


@pytest.mark.django_db
class TestDjangoProjectionReader:
    def test_get_returns_row_or_none(self):
        Area.objects.create(name="present")
        reader = DjangoProjectionReader()
        assert reader.get("test_django_rakaia.Area", name="present").name == "present"
        assert reader.get("test_django_rakaia.Area", name="absent") is None

    def test_filter_and_query(self):
        for name in ("a", "b", "c"):
            Area.objects.create(name=name)
        reader = DjangoProjectionReader()
        assert reader.filter("test_django_rakaia.Area", name="b").count() == 1
        assert reader.query("test_django_rakaia.Area").count() == 3


def _ref_handler(event):
    return Effect(
        op="update_or_create",
        model_label="test_django_rakaia.Area",
        lookup={"name": event["name"]},
        defaults={},
    )


def _dep_handler(event, reader):
    ref = reader.get("test_django_rakaia.Area", name=event["ref"])
    tag = "FOUND" if ref is not None else "MISSING"
    return Effect(
        op="update_or_create",
        model_label="test_django_rakaia.Area",
        lookup={"name": f"{event['key']}->{tag}"},
        defaults={},
    )


# The dependent event arrives BEFORE the reference it needs.
_EVENTS = [
    {"schema_version": 1, "kind": "DEP", "key": "dep-1", "ref": "the-ref"},
    {"schema_version": 1, "kind": "REF", "key": "ref-1", "name": "the-ref"},
]


@pytest.mark.django_db
class TestStagedReplayOverOrm:
    def test_stage1_reads_stage0_committed_rows(self):
        store = get_store()
        store.delete("s")
        store.create("s")
        for event in _EVENTS:
            store.append("s", json.dumps(event).encode("utf-8"))

        reg = HandlerRegistry()
        reg.register("ref", "REF", _ref_handler, 0, None, match_field="kind", stage=0)
        reg.register("dep", "DEP", _dep_handler, 0, None, match_field="kind", stage=1)

        replay(
            store,
            "s",
            DjangoExecutor(),
            handler_registry=reg,
            upcaster_registry=UpcasterRegistry(),
            reader=DjangoProjectionReader(),
        )

        # Stage 0 created the reference Area before stage 1 read it, so the
        # dependent — despite arriving first in the stream — resolves FOUND.
        assert Area.objects.filter(name="dep-1->FOUND").exists()
        assert not Area.objects.filter(name="dep-1->MISSING").exists()


def _finance_line_handler(event):
    return Effect(
        op="update_or_create",
        model_label="test_django_rakaia.FinanceLine",
        lookup={"submission_id": event["key"]},
        defaults={"suku": event["suku"], "delta": event["delta"]},
    )


def _balance_reducer(reader):
    from rakaia.projections import reconcile_aggregate

    groups: dict[str, int] = {}
    for line in reader.query("test_django_rakaia.FinanceLine"):
        groups[line.suku] = groups.get(line.suku, 0) + line.delta
    return reconcile_aggregate(
        "test_django_rakaia.Balance",
        {},
        "suku",
        {s: {"total": t} for s, t in groups.items()},
    )


_FINANCE_EVENTS = [
    {"schema_version": 1, "kind": "FINANCE", "key": "f1", "suku": "A", "delta": 100},
    {"schema_version": 1, "kind": "FINANCE", "key": "f2", "suku": "A", "delta": -30},
    {"schema_version": 1, "kind": "FINANCE", "key": "f3", "suku": "B", "delta": 50},
]


@pytest.mark.django_db
class TestReducerOverOrm:
    def test_reducer_recomputes_aggregate_via_reconcile_aggregate(self):
        from .models import Balance

        store = get_store()
        store.delete("s")
        store.create("s")
        for event in _FINANCE_EVENTS:
            store.append("s", json.dumps(event).encode("utf-8"))

        reg = HandlerRegistry()
        reg.register(
            "finance",
            "FINANCE",
            _finance_line_handler,
            0,
            None,
            match_field="kind",
            stage=0,
        )
        reg.register_reducer("balance", 1, _balance_reducer)

        replay(
            store,
            "s",
            DjangoExecutor(),
            handler_registry=reg,
            upcaster_registry=UpcasterRegistry(),
            reader=DjangoProjectionReader(),
        )

        assert Balance.objects.get(suku="A").total == 70  # 100 - 30
        assert Balance.objects.get(suku="B").total == 50


@pytest.mark.django_db
class TestMergeReplayOverOrm:
    def test_merge_two_finance_streams_then_reduce(self):
        from .models import Balance

        store = get_store()
        for path in ("fin/a", "fin/b"):
            store.delete(path)
            store.create(path)
        store.append(
            "fin/a",
            json.dumps(
                {
                    "schema_version": 1,
                    "kind": "FINANCE",
                    "key": "f1",
                    "suku": "A",
                    "delta": 100,
                    "ts": "2026-01-01T00:00:00Z",
                }
            ).encode("utf-8"),
        )
        store.append(
            "fin/b",
            json.dumps(
                {
                    "schema_version": 1,
                    "kind": "FINANCE",
                    "key": "f2",
                    "suku": "A",
                    "delta": -30,
                    "ts": "2026-01-01T01:00:00Z",
                }
            ).encode("utf-8"),
        )
        store.append(
            "fin/b",
            json.dumps(
                {
                    "schema_version": 1,
                    "kind": "FINANCE",
                    "key": "f3",
                    "suku": "B",
                    "delta": 50,
                    "ts": "2026-01-01T02:00:00Z",
                }
            ).encode("utf-8"),
        )

        reg = HandlerRegistry()
        reg.register(
            "finance",
            "FINANCE",
            _finance_line_handler,
            0,
            None,
            match_field="kind",
            stage=0,
        )
        reg.register_reducer("balance", 1, _balance_reducer)

        merge_replay(
            store,
            ["fin/a", "fin/b"],
            DjangoExecutor(),
            handler_registry=reg,
            upcaster_registry=UpcasterRegistry(),
            reader=DjangoProjectionReader(),
        )

        assert Balance.objects.get(suku="A").total == 70  # 100 - 30 across streams
        assert Balance.objects.get(suku="B").total == 50
