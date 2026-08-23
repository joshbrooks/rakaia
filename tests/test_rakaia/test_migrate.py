"""Moving a stream between backends, and being told what survived (#229).

`migrate_stream` makes one promise and refuses to make another. It copies the
events, and it *reports* whether the copy preserved offsets — it does not claim
it will. These tests hold both halves: that the copy is faithful where it can
be, and that the report is right when it cannot be, because a wrong "cursors are
fine" is worse than no migration tool at all.
"""

from __future__ import annotations

import json

import pytest

from rakaia.jsonl_store import JsonlStreamStore
from rakaia.migrate import migrate_all, migrate_stream
from rakaia.store import StreamStore
from rakaia.subscription import poll
from rakaia.types import AppendOptions, StreamNotFound

PATH = "s"


@pytest.fixture
def files(tmp_path):
    return JsonlStreamStore(tmp_path / "a", fsync=False)


@pytest.fixture
def other_files(tmp_path):
    return JsonlStreamStore(tmp_path / "b", fsync=False)


def _seed(store, n=5, path=PATH, **create):
    store.create(path, **create)
    for i in range(n):
        store.append(
            path,
            json.dumps({"n": i}).encode(),
            AppendOptions(label="created", metadata={"i": i}, event_ts=1000.0 + i),
        )
    return store


class TestBetweenTwoEntryCountingStores:
    """Two stores that both number entries can line up exactly, and do."""

    def test_offsets_and_cursors_survive(self, files, other_files):
        _seed(files)
        before = [m.offset for m in files.read(PATH)[0]]

        result = migrate_stream(files, other_files, PATH)

        assert result.events == 5
        assert result.offsets_preserved is True
        assert result.head_preserved is True
        assert result.cursors_valid is True
        assert [m.offset for m in other_files.read(PATH)[0]] == before

    def test_a_saved_cursor_reads_as_caught_up_on_the_copy(self, files, other_files):
        """The claim a caller actually cares about, stated as a consumer would
        experience it: resume against the copy and get nothing new, rather than
        a `rewound` that throws away derived state."""
        _seed(files)
        cursor = poll(files, PATH, None).cursor

        migrate_stream(files, other_files, PATH)

        resumed = poll(other_files, PATH, cursor)
        assert resumed.caught_up is True
        assert resumed.rewound is False

    def test_a_mid_stream_cursor_resumes_on_exactly_the_same_events(
        self, files, other_files
    ):
        _seed(files, n=10)
        partial = files.read(PATH)[0][3].offset

        migrate_stream(files, other_files, PATH)

        assert [json.loads(m.data) for m in other_files.read(PATH, partial)[0]] == [
            json.loads(m.data) for m in files.read(PATH, partial)[0]
        ]

    def test_the_envelope_crosses_intact(self, files, other_files):
        _seed(files, n=3)

        migrate_stream(files, other_files, PATH)

        copied = other_files.read(PATH)[0]
        original = files.read(PATH)[0]
        assert [(m.label, m.metadata, m.event_ts) for m in copied] == [
            (m.label, m.metadata, m.event_ts) for m in original
        ]

    def test_the_logical_timestamp_is_not_reset_to_the_migration(
        self, files, other_files
    ):
        """`event_ts` is part of the event, not of the transport. A copy that
        let it default would move every event's logical time to the moment of
        the migration and silently reorder any envelope-ordered merge."""
        _seed(files, n=3)

        migrate_stream(files, other_files, PATH)

        assert [m.event_ts for m in other_files.read(PATH)[0]] == [
            1000.0,
            1001.0,
            1002.0,
        ]

    def test_the_content_type_and_expiry_cross(self, files, other_files):
        files.create(
            PATH, content_type="application/json", expires_at="2099-01-01T00:00:00Z"
        )
        files.append(PATH, b'{"n": 1}')

        migrate_stream(files, other_files, PATH)

        copied = other_files.get(PATH)
        assert copied.content_type == "application/json"
        assert copied.expires_at == "2099-01-01T00:00:00Z"

    def test_a_closed_stream_arrives_closed(self, files, other_files):
        _seed(files, n=2)
        files.close_stream(PATH)

        result = migrate_stream(files, other_files, PATH)

        assert other_files.get(PATH).closed is True
        assert len(other_files.read(PATH)[0]) == 2, "closing must not eat the events"
        assert result.offsets_preserved is True

    def test_a_json_mode_element_that_is_itself_an_array_is_not_reflattened(
        self, files, other_files
    ):
        """The copy has to cancel the flatten the target will apply.

        A JSON-mode stream stores already-flattened elements. Re-appending an
        element that is itself an array would flatten it a second time, turning
        one event into several and shifting every offset after it.
        """
        files.create(PATH, content_type="application/json")
        files.append(PATH, b"[[1, 2], [3, 4]]")
        assert len(files.read(PATH)[0]) == 2

        result = migrate_stream(files, other_files, PATH)

        assert result.events == 2
        assert [m.data for m in other_files.read(PATH)[0]] == [
            m.data for m in files.read(PATH)[0]
        ]
        assert result.offsets_preserved is True

    def test_the_stream_seq_fence_crosses(self, files, other_files):
        _seed(files, n=2)
        files.append(PATH, b'{"n": 9}', AppendOptions(seq="020"))

        migrate_stream(files, other_files, PATH)

        assert other_files.get(PATH).last_seq == "020"


class TestBetweenStoresThatCountDifferently:
    """The in-memory store counts bytes; the others count entries. Nothing can
    make those line up, and the report has to say so plainly."""

    def test_offsets_are_not_preserved_and_the_report_says_so(self, other_files):
        memory = _seed(StreamStore(), n=4)

        result = migrate_stream(memory, other_files, PATH)

        assert result.events == 4
        assert result.offsets_preserved is False
        assert result.cursors_valid is False
        assert any("cursor" in note for note in result.notes)

    def test_the_events_still_cross_intact(self, other_files):
        memory = _seed(StreamStore(), n=4)

        migrate_stream(memory, other_files, PATH)

        assert [json.loads(m.data) for m in other_files.read(PATH)[0]] == [
            {"n": i} for i in range(4)
        ]
        assert [m.label for m in other_files.read(PATH)[0]] == ["created"] * 4

    def test_a_cursor_from_the_source_is_not_quietly_accepted(self, other_files):
        """The failure this whole report exists to prevent.

        A compound cursor carried onto an entry-counting store is refused rather
        than resolved to some position of its own — and `cursors_valid` said as
        much before anyone tried it.
        """
        from rakaia.offsets import ForeignOffset

        memory = _seed(StreamStore(), n=4)
        cursor = poll(memory, PATH, None).cursor
        result = migrate_stream(memory, other_files, PATH)

        assert result.cursors_valid is False
        with pytest.raises(ForeignOffset):
            poll(other_files, PATH, cursor)


class TestWhenTheOffsetsCannotLineUp:
    def test_a_recreated_source_reports_its_head_as_unpreserved(
        self, files, other_files
    ):
        """A stream recreated at a path resumes numbering above the mark it
        retired (#34), so its head sits above its last event. Copying the events
        cannot reproduce that head, and a consumer parked at it would rewind."""
        _seed(files, n=3)
        files.delete(PATH)
        files.create(PATH)

        result = migrate_stream(files, other_files, PATH)

        assert result.events == 0
        assert result.head_preserved is False
        assert result.cursors_valid is False
        assert any("head" in note for note in result.notes)

    def test_offsets_with_a_gap_do_not_survive_and_are_reported(
        self, files, other_files
    ):
        _seed(files, n=2)
        files.delete(PATH)
        files.create(PATH)
        files.append(PATH, b'{"n": 99}')
        assert files.read(PATH)[0][0].offset != "00000000000000000001"

        result = migrate_stream(files, other_files, PATH)

        assert result.offsets_preserved is False
        assert result.cursors_valid is False


class TestRefusals:
    def test_an_absent_source_stream_raises(self, files, other_files):
        with pytest.raises(StreamNotFound):
            migrate_stream(files, other_files, "nope")

    def test_a_populated_target_is_refused_rather_than_interleaved(
        self, files, other_files
    ):
        _seed(files, n=2)
        _seed(other_files, n=1)

        with pytest.raises(ValueError, match="already holds events"):
            migrate_stream(files, other_files, PATH)

        assert len(other_files.read(PATH)[0]) == 1, "the refusal wrote nothing"

    def test_an_empty_target_stream_is_not_an_obstacle(self, files, other_files):
        """Refusing a *populated* target is the rule; a created-but-empty one is
        the normal case for a target prepared in advance."""
        _seed(files, n=2)
        other_files.create(PATH)

        result = migrate_stream(files, other_files, PATH)

        assert result.events == 2


class TestNotes:
    def test_a_ttl_stream_says_its_window_restarts(self, files, other_files):
        files.create(PATH, ttl_seconds=3600)
        files.append(PATH, b'{"n": 1}')

        result = migrate_stream(files, other_files, PATH)

        assert any("TTL" in note for note in result.notes)

    def test_producer_state_is_reported_as_not_carried(self, files, other_files):
        import asyncio

        files.create(PATH)
        asyncio.run(
            files.append_with_producer(
                PATH,
                b'{"n": 1}',
                AppendOptions(producer_id="p", producer_epoch=0, producer_seq=0),
            )
        )

        result = migrate_stream(files, other_files, PATH)

        assert any("fencing" in note for note in result.notes)

    def test_a_clean_copy_has_nothing_to_report(self, files, other_files):
        _seed(files, n=3)

        assert migrate_stream(files, other_files, PATH).notes == ()


class TestMigrateAll:
    def test_every_stream_crosses(self, files, other_files):
        _seed(files, n=2, path="one")
        _seed(files, n=3, path="two")

        results = migrate_all(files, other_files)

        assert {r.path: r.events for r in results} == {"one": 2, "two": 3}
        assert all(r.cursors_valid for r in results)
        assert sorted(other_files.list_paths()) == ["one", "two"]

    def test_a_source_that_cannot_list_is_a_clear_failure(self, other_files):
        class Unlistable:
            """A `ReadableStore` and nothing more — which is legal, and is why
            `migrate_all` has to check rather than assume."""

            def read(self, path, offset=None):  # noqa: ARG002
                return [], True

        with pytest.raises(TypeError, match="cannot list its streams"):
            migrate_all(Unlistable(), other_files)
