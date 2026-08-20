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


class TestTheLogFollowsItsAlias:
    """`DjangoStreamStore(using=...)` — the log joins the seam the rest already sits on.

    `DjangoExecutor` and `DjangoProjectionReader` can both be aimed at a named
    database, which is what lets a rebuild be replayed into a throwaway one and
    verified at full ORM fidelity (ADR 0003). The log could not, so the rebuild
    could not read its own events from the guarded alias — and `hermeticity.py`
    shipped a six-line drain-to-memory as a *docstring* instead.

    This is the third adapter on an existing seam, not a new seam: two things
    already vary across it.
    """

    def _store(self, alias: str | None = None):
        from django_rakaia.django_store import DjangoStreamStore

        return DjangoStreamStore(using=alias)

    def test_create_puts_the_stream_on_the_named_alias(self):
        from django_rakaia.models import Stream

        self._store("overlay").create("s")

        assert Stream.objects.using("overlay").filter(stream_id="s").exists()
        assert not Stream.objects.using("default").filter(stream_id="s").exists()

    def test_append_puts_the_event_and_entry_on_the_named_alias(self):
        from django_rakaia.models import StreamEntry, StreamEvent

        store = self._store("overlay")
        store.create("s")
        store.append("s", b'{"n": 1}')

        assert StreamEvent.objects.using("overlay").count() == 1
        assert StreamEntry.objects.using("overlay").count() == 1
        assert StreamEvent.objects.using("default").count() == 0
        assert StreamEntry.objects.using("default").count() == 0

    def test_the_offset_high_water_is_allocated_on_the_named_alias(self):
        # The watermark is the sole authority for a stream's head once advanced,
        # so a rebuild whose offsets came off the live database would read its
        # positions from production (#159, and the read side of #175).
        from django_rakaia.models import StreamOffsetWatermark

        store = self._store("overlay")
        store.create("s")
        store.append("s", b'{"n": 1}')

        assert (
            StreamOffsetWatermark.objects.using("overlay")
            .filter(stream_path="s")
            .exists()
        )
        assert (
            not StreamOffsetWatermark.objects.using("default")
            .filter(stream_path="s")
            .exists()
        )

    def test_read_returns_what_that_alias_holds(self):
        store = self._store("overlay")
        store.create("s")
        store.append("s", b'{"n": 1}')

        messages, _ = store.read("s")
        assert [m.data for m in messages] == [b'{"n": 1}']

    def test_two_aliases_are_separate_logs_at_the_same_path(self):
        default_store = self._store()
        overlay_store = self._store("overlay")
        default_store.create("shared")
        overlay_store.create("shared")
        default_store.append("shared", b'{"where": "default"}')
        overlay_store.append("shared", b'{"where": "overlay"}')

        assert [m.data for m in default_store.read("shared")[0]] == [
            b'{"where": "default"}'
        ]
        assert [m.data for m in overlay_store.read("shared")[0]] == [
            b'{"where": "overlay"}'
        ]

    def test_a_stream_on_another_alias_is_absent_not_empty(self):
        # `has()` must not report a stream this store cannot read. An "exists but
        # empty" answer is the one a resuming subscriber would misread.
        self._store("overlay").create("s")

        assert self._store("overlay").has("s")
        assert not self._store("default").has("s")
        assert self._store("default").get("s") is None

    def test_the_envelope_survives_the_alias(self):
        # `write_enveloped_event` derives its alias from the stream row it was
        # handed, so the event has to land beside its entry or one save is split
        # across two databases (#159).
        from rakaia import AppendOptions

        store = self._store("overlay")
        store.create("s")
        store.append(
            "s",
            b'{"n": 1}',
            AppendOptions(label="update", metadata={"user": 7}, event_ts=1234.0),
        )

        messages, _ = store.read("s")
        assert messages[0].label == "update"
        assert messages[0].metadata == {"user": 7}
        assert messages[0].event_ts == 1234.0

    def test_listing_and_deleting_stay_on_the_alias(self):
        overlay, default = self._store("overlay"), self._store()
        overlay.create("only-overlay")
        default.create("only-default")

        assert overlay.list_paths() == ["only-overlay"]
        assert default.list_paths() == ["only-default"]

        assert overlay.delete("only-overlay") is True
        assert default.has("only-default"), "deleting on one alias touched the other"

    def test_the_head_is_reported_from_the_alias(self):
        from django_rakaia.offsets import format_offset

        overlay, default = self._store("overlay"), self._store()
        overlay.create("shared")
        default.create("shared")
        for _ in range(3):
            default.append("shared", b"{}")
        overlay.append("shared", b"{}")

        assert overlay.get_current_offset("shared") == format_offset(1)
        assert default.get_current_offset("shared") == format_offset(3)


class TestTheBulkAndFencedPathsFollowTheAliasToo:
    """The two paths that do *not* go through `write_enveloped_event`.

    A single append derives its alias from the stream row it was handed (#159),
    so events and entries land correctly even without the store's own accessors.
    Two paths bypass that: `append_many` bulk-creates events and entries
    directly, and producer fencing reads and writes `StreamProducer`. Both were
    green with the alias stripped out of those accessors until these cases
    existed — the seam looked covered because the common path covers itself.
    """

    def _store(self, alias: str | None = None):
        from django_rakaia.django_store import DjangoStreamStore

        return DjangoStreamStore(using=alias)

    def test_a_batch_lands_entirely_on_the_named_alias(self):
        from django_rakaia.models import StreamEntry, StreamEvent

        store = self._store("overlay")
        store.create("s")
        store.append_many("s", [(b'{"n": 1}', None), (b'{"n": 2}', None)])

        assert StreamEvent.objects.using("overlay").count() == 2
        assert StreamEntry.objects.using("overlay").count() == 2
        assert StreamEvent.objects.using("default").count() == 0
        assert StreamEntry.objects.using("default").count() == 0

    def test_a_batch_is_readable_back_from_the_alias(self):
        store = self._store("overlay")
        store.create("s")
        store.append_many("s", [(b'{"n": 1}', None), (b'{"n": 2}', None)])

        assert [m.data for m in store.read("s")[0]] == [b'{"n": 1}', b'{"n": 2}']

    def test_producer_state_is_recorded_on_the_named_alias(self):
        from django_rakaia.models import StreamProducer
        from rakaia import AppendOptions

        store = self._store("overlay")
        store.create("s")
        store.append(
            "s", b"{}", AppendOptions(producer_id="p", producer_epoch=1, producer_seq=0)
        )

        assert StreamProducer.objects.using("overlay").filter(producer_id="p").exists()
        assert (
            not StreamProducer.objects.using("default").filter(producer_id="p").exists()
        )

    def test_fencing_reads_the_state_from_its_own_alias(self):
        # The consequence of getting this wrong: a producer fenced on one
        # database would be admitted on another, because its state was invisible.
        # Same producer id and epoch on both aliases, at different sequences.
        from rakaia import AppendOptions

        overlay, default = self._store("overlay"), self._store()
        for store in (overlay, default):
            store.create("s")
            store.append(
                "s",
                b"{}",
                AppendOptions(producer_id="p", producer_epoch=1, producer_seq=0),
            )
        overlay.append(
            "s", b"{}", AppendOptions(producer_id="p", producer_epoch=1, producer_seq=1)
        )

        # seq 1 is a duplicate on overlay (already at 1) and the next in line on
        # default (still at 0). One store must not see the other's progress.
        dup = overlay.append(
            "s", b"{}", AppendOptions(producer_id="p", producer_epoch=1, producer_seq=1)
        )
        fresh = default.append(
            "s", b"{}", AppendOptions(producer_id="p", producer_epoch=1, producer_seq=1)
        )
        assert dup.message is None, "overlay should have seen seq 1 as a duplicate"
        assert fresh.message is not None, "default should have accepted seq 1"


class TestTheDefaultAliasIsUnchanged:
    """No `using=` must behave exactly as before — this is a Tier 1 constructor."""

    def test_no_argument_targets_the_default_database(self):
        from django_rakaia.django_store import DjangoStreamStore
        from django_rakaia.models import Stream

        DjangoStreamStore().create("s")

        assert Stream.objects.using("default").filter(stream_id="s").exists()

    def test_using_none_is_the_same_as_omitting_it(self):
        from django_rakaia.django_store import DjangoStreamStore

        store = DjangoStreamStore(using=None)
        store.create("s")
        store.append("s", b'{"n": 1}')

        assert [m.data for m in store.read("s")[0]] == [b'{"n": 1}']
