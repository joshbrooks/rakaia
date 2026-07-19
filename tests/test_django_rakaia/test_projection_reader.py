"""Tests for DjangoProjectionReader + end-to-end staged replay over the ORM."""

from __future__ import annotations

import json

import pytest

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.projection_reader import DjangoProjectionReader
from django_rakaia.store import get_store
from rakaia.effects import Effect
from rakaia.registry import HandlerRegistry, UpcasterRegistry
from rakaia.replay import replay

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
