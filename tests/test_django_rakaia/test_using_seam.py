"""The `using=` connection-alias seam (#68 item 2).

`DjangoExecutor` and `DjangoProjectionReader` can target a named database alias,
so a full from-scratch rebuild can be replayed into a *disposable* database and
verified without touching production — the honest, full-ORM-fidelity answer to
"an in-memory shadow the reader can see", reusing the real executor + reader
instead of a bespoke in-memory engine.
"""

from __future__ import annotations

import json

import pytest

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.projection_reader import DjangoProjectionReader
from django_rakaia.store import get_store
from rakaia.effects import Effect, Ref
from rakaia.registry import HandlerRegistry, UpcasterRegistry
from rakaia.replay import replay

from .models import Area, FinanceLine

pytestmark = pytest.mark.django_db(databases=["default", "overlay"])


class TestExecutorUsing:
    def test_writes_go_to_the_named_alias_only(self):
        DjangoExecutor(using="overlay").apply(
            [
                Effect(
                    op="update_or_create",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "OverlayOnly"},
                    defaults={},
                )
            ]
        )
        assert Area.objects.using("overlay").filter(name="OverlayOnly").exists()
        assert not Area.objects.using("default").filter(name="OverlayOnly").exists()

    def test_default_alias_unchanged_when_using_none(self):
        DjangoExecutor().apply(
            [
                Effect(
                    op="update_or_create",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "DefaultRow"},
                    defaults={},
                )
            ]
        )
        assert Area.objects.using("default").filter(name="DefaultRow").exists()
        assert not Area.objects.using("overlay").filter(name="DefaultRow").exists()


class TestReaderUsing:
    def test_reader_reads_from_the_named_alias(self):
        Area.objects.using("overlay").create(name="Only")
        assert (
            DjangoProjectionReader(using="overlay").get(
                "test_django_rakaia.Area", name="Only"
            )
            is not None
        )
        # The default-alias reader does not see the overlay row.
        assert (
            DjangoProjectionReader(using="default").get(
                "test_django_rakaia.Area", name="Only"
            )
            is None
        )

    def test_filter_and_query_route_to_alias(self):
        Area.objects.using("overlay").create(name="a")
        Area.objects.using("overlay").create(name="b")
        reader = DjangoProjectionReader(using="overlay")
        assert reader.filter("test_django_rakaia.Area", name="a").count() == 1
        assert reader.query("test_django_rakaia.Area").count() == 2
        assert DjangoProjectionReader().query("test_django_rakaia.Area").count() == 0


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


_EVENTS = [
    {"schema_version": 1, "kind": "DEP", "key": "d1", "ref": "the-ref"},
    {"schema_version": 1, "kind": "REF", "key": "r1", "name": "the-ref"},
]


class TestFromScratchRebuildProof:
    """Replay the whole log into the disposable `overlay` DB; `default` — standing
    in for production — is never written."""

    def test_staged_rebuild_links_in_overlay_leaving_default_empty(self):
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
            DjangoExecutor(using="overlay"),
            handler_registry=reg,
            upcaster_registry=UpcasterRegistry(),
            reader=DjangoProjectionReader(using="overlay"),
        )

        # The dependent — which arrives before its reference — links in the
        # overlay, from an empty start.
        assert Area.objects.using("overlay").filter(name="d1->FOUND").exists()
        # Production (default) was never touched.
        assert Area.objects.using("default").count() == 0

    def test_refs_resolve_under_the_alias(self):
        # A produces=/Ref pair also resolves against the overlay connection.
        DjangoExecutor(using="overlay").apply(
            [
                Effect(
                    op="update_or_create",
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "Zone"},
                    defaults={},
                    produces="area",
                ),
                Effect(
                    op="update_or_create",
                    model_label="test_django_rakaia.FinanceLine",
                    lookup={"submission_id": "s1"},
                    defaults={"suku": "x", "delta": Ref("area")},
                ),
            ]
        )
        area = Area.objects.using("overlay").get(name="Zone")
        line = FinanceLine.objects.using("overlay").get(submission_id="s1")
        assert line.delta == area.pk
        assert FinanceLine.objects.using("default").count() == 0
