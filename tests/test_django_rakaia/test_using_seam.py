"""The `using=` connection-alias seam (#68 item 2).

`DjangoExecutor` and `DjangoProjectionReader` can target a named database alias,
so a full from-scratch rebuild can be replayed into a *disposable* database and
verified without touching production — the honest, full-ORM-fidelity answer to
"an in-memory shadow the reader can see", reusing the real executor + reader
instead of a bespoke in-memory engine.
"""

from __future__ import annotations

import pytest

from django_rakaia.effect_executor import DjangoExecutor
from django_rakaia.projection_reader import DjangoProjectionReader
from django_rakaia.store import get_store
from rakaia.effects import Ref, Upsert
from rakaia.registry import HandlerRegistry, UpcasterRegistry
from rakaia.replay import replay
from rakaia.seed import seed_stream

from .models import Area, FinanceLine

pytestmark = pytest.mark.django_db(databases=["default", "overlay"])


class TestExecutorUsing:
    def test_writes_go_to_the_named_alias_only(self):
        DjangoExecutor(using="overlay").apply(
            [
                Upsert(
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
                Upsert(
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
    return Upsert(
        model_label="test_django_rakaia.Area",
        lookup={"name": event["name"]},
        defaults={},
    )


def _dep_handler(event, reader):
    ref = reader.get("test_django_rakaia.Area", name=event["ref"])
    tag = "FOUND" if ref is not None else "MISSING"
    return Upsert(
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
        seed_stream("s", _EVENTS, store=store)

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
                Upsert(
                    model_label="test_django_rakaia.Area",
                    lookup={"name": "Zone"},
                    defaults={},
                    produces="area",
                ),
                Upsert(
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


class TestTheStreamHeadFollowsItsAlias:
    """Reading a stream's head must come from the database the stream is on.

    `Stream.get_next_offset_block` allocates from the alias its row came from
    (#159), and the head has to be read the same way or a rebuild against a
    scratch database reports the live stream's position.

    This is the read half of that invariant, and it was uncovered: dropping the
    alias from the allocation path fails a test, dropping it from `current_offset`
    used to fail nothing. It matters more now the watermark is authoritative here
    — the previous `max(entries, watermark)` read had the entries half following
    the alias, so a scratch database at least reported its own entries when they
    were higher. Reading the watermark alone without the alias would report the
    live high-water outright.
    """

    def _stream_on(self, alias: str, path: str, blocks: int):
        from django_rakaia.models import Stream

        stream = Stream.objects.using(alias).create(stream_id=path)
        for _ in range(blocks):
            stream.get_next_offset_block(1)
        return stream

    def test_the_head_reports_its_own_databases_high_water(self):
        from django_rakaia.offsets import format_offset

        # Same path on both aliases, deliberately at different positions.
        self._stream_on("default", "shared", blocks=7)
        overlay_stream = self._stream_on("overlay", "shared", blocks=2)

        assert overlay_stream.current_offset == format_offset(2), (
            "the head was read from the wrong database — a rebuild against a "
            "scratch alias would report the live stream's position"
        )

    def test_the_default_head_is_unaffected_by_the_overlay(self):
        from django_rakaia.models import Stream
        from django_rakaia.offsets import format_offset

        self._stream_on("default", "shared", blocks=7)
        self._stream_on("overlay", "shared", blocks=2)

        default_stream = Stream.objects.using("default").get(stream_id="shared")
        assert default_stream.current_offset == format_offset(7)
