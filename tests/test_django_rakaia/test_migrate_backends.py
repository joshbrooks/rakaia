"""Moving a live log between the two durable backends (#229).

`test_rakaia/test_migrate.py` covers the migration rules. This covers the pair a
deployment would actually move between — the database-backed store and the
file-backed one — because that is the case where the answer is not obvious in
advance: both number entries, so their offsets *can* line up, and the whole
value of the report is that it says whether they did.
"""

from __future__ import annotations

import json

import pytest

from django_rakaia.consumer import django_consumer
from django_rakaia.django_store import DjangoStreamStore
from django_rakaia.subscription import commit_cursor, poll_consumer
from rakaia.jsonl_store import JsonlStreamStore
from rakaia.migrate import migrate_all, migrate_stream
from rakaia.subscription import poll
from rakaia.types import AppendOptions, StreamNotFound

PATH = "s"


@pytest.fixture
def files(tmp_path):
    return JsonlStreamStore(tmp_path / "streams", fsync=False)


def _seed(store, n=5, path=PATH):
    store.create(path)
    for i in range(n):
        store.append(
            path,
            json.dumps({"n": i}).encode(),
            AppendOptions(label="created", metadata={"i": i}, event_ts=1000.0 + i),
        )
    return store


@pytest.mark.django_db
class TestDatabaseToFiles:
    def test_offsets_and_cursors_survive(self, files):
        db = _seed(DjangoStreamStore())
        before = [m.offset for m in db.read(PATH)[0]]

        result = migrate_stream(db, files, PATH)

        assert result.events == 5
        assert result.cursors_valid is True
        assert [m.offset for m in files.read(PATH)[0]] == before
        assert result.notes == ()

    def test_a_consumer_resumes_on_the_copy_without_rewinding(self, files):
        """The end-to-end claim: a consumer with a committed watermark against
        the database picks up on the files as though nothing happened."""
        db = _seed(db_store := DjangoStreamStore(), n=3) and db_store
        first = poll_consumer(db, "worker", PATH)
        commit_cursor("worker", PATH, first.cursor)
        db.append(PATH, b'{"n": 99}')

        migrate_stream(db, files, PATH)

        resumed = poll(files, PATH, first.cursor)
        assert resumed.rewound is False
        assert [json.loads(m.data) for m in resumed.messages] == [{"n": 99}]

    def test_the_envelope_crosses(self, files):
        db = _seed(DjangoStreamStore(), n=3)

        migrate_stream(db, files, PATH)

        assert [(m.label, m.metadata, m.event_ts) for m in files.read(PATH)[0]] == [
            (m.label, m.metadata, m.event_ts) for m in db.read(PATH)[0]
        ]


@pytest.mark.django_db
class TestFilesToDatabase:
    def test_offsets_and_cursors_survive(self, files):
        _seed(files)
        before = [m.offset for m in files.read(PATH)[0]]
        db = DjangoStreamStore()

        result = migrate_stream(files, db, PATH)

        assert result.cursors_valid is True
        assert [m.offset for m in db.read(PATH)[0]] == before

    def test_a_closed_file_stream_arrives_closed_in_the_database(self, files):
        _seed(files, n=2)
        files.close_stream(PATH)
        db = DjangoStreamStore()

        migrate_stream(files, db, PATH)

        assert db.get(PATH).closed is True
        assert len(db.read(PATH)[0]) == 2

    def test_every_stream_crosses(self, files):
        _seed(files, n=2, path="one")
        _seed(files, n=3, path="two")
        db = DjangoStreamStore()

        results = migrate_all(files, db)

        assert {r.path: r.events for r in results} == {"one": 2, "two": 3}
        assert all(r.cursors_valid for r in results)


@pytest.mark.django_db
class TestTheRoundTrip:
    def test_a_log_can_be_carried_between_stores_and_arrive_intact(
        self, files, tmp_path
    ):
        """Database to files to a second set of files, offsets intact throughout.

        The property that makes the file store usable as a transport or staging
        format and not only as a destination.
        """
        db = _seed(DjangoStreamStore(), n=4)
        original = [(m.offset, m.data, m.label) for m in db.read(PATH)[0]]
        elsewhere = JsonlStreamStore(tmp_path / "elsewhere", fsync=False)

        assert migrate_stream(db, files, PATH).cursors_valid is True
        assert migrate_stream(files, elsewhere, PATH).cursors_valid is True

        assert [
            (m.offset, m.data, m.label) for m in elsewhere.read(PATH)[0]
        ] == original

    def test_a_log_cannot_be_copied_back_into_the_store_it_was_deleted_from(
        self, files
    ):
        """Out, delete, back in — and the offsets do not come back with it.

        Deleting a stream retires its offsets permanently, because a store must
        never reissue a position a subscriber may already have consumed (#34).
        So the obvious way to rebuild or compact a log in place does not
        preserve numbering, and the report has to say which of the several
        possible reasons this was.
        """
        db = _seed(DjangoStreamStore(), n=4)

        migrate_stream(db, files, PATH)
        db.delete(PATH)
        back = migrate_stream(files, DjangoStreamStore(), PATH)

        assert back.events == 4, "the events themselves still cross"
        assert back.offsets_preserved is False
        assert back.cursors_valid is False
        assert any("retired its offsets" in note for note in back.notes)


@pytest.mark.django_db(transaction=True)
class TestAConsumerThatHasRunSurvivesTheMove:
    """A consumer saves the stream's stamp beside its position, and stops when it
    changes (#350). A move that keeps every offset must keep the stamp, in both
    directions, or the copy that `cursors_valid` vouches for is refused."""

    @staticmethod
    def _run(store):
        seen = []
        result = django_consumer(store, PATH, "worker").run(
            lambda m: seen.append(json.loads(m.data)) or None, on_error="halt"
        )
        return result, seen

    def test_database_to_files(self, files):
        db = _seed(DjangoStreamStore(), n=3)
        self._run(db)
        db.append(PATH, b'{"n": 99}')

        migrate_stream(db, files, PATH)
        result, seen = self._run(files)

        assert result.status == "advanced"
        assert seen == [{"n": 99}]

    def test_files_to_database(self, files):
        _seed(files, n=3)
        self._run(files)
        files.append(PATH, b'{"n": 99}')
        db = DjangoStreamStore()

        migrate_stream(files, db, PATH)
        result, seen = self._run(db)

        assert result.status == "advanced"
        assert seen == [{"n": 99}]

    def test_a_missing_stream_cannot_be_given_a_stamp(self):
        with pytest.raises(StreamNotFound):
            DjangoStreamStore().adopt_generation(PATH, "feedface")

    def test_the_stamp_is_saved(self):
        db = DjangoStreamStore()
        db.create(PATH)

        db.adopt_generation(PATH, "feedface")

        assert DjangoStreamStore().get_generation(PATH) == "feedface"
