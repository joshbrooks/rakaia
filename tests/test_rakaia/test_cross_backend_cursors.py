"""What a saved position does when it meets the wrong store (ADR 0006, #233).

ADR 0006 states which pairs of stores refuse each other's cursors and which
pair accepts them silently. That table is the whole reason the ADR exists — the
rule it decides ("changing a backend is a copy") follows entirely from the pair
that fails without saying so.

Nothing below refers to a row by its position. Three successive drafts of these
docstrings miscounted the table — a store added to a cell called a new row, two
directions of one pair called two rows — so the numbering is simply not used.
Each case is named by what it does instead, which cannot fall out of step with
a table someone reorders.

A table in a Markdown file rots. This pins it where it is decided, the way
`test_producer_fencing_table.py` pins the fencing outcomes (#176). If someone
gives `JsonlStreamStore` its own offset format, or lands #232, these go red and
the ADR has to be amended rather than quietly left wrong.

The two entry-counting stores are modelled here by two `JsonlStreamStore`s at
different roots. That is not a stand-in for the durable store: the behaviour
under test belongs to `rakaia.offsets`, which sees a *format*, not a class, and
`test_django_rakaia/test_offset_format_pin.py` pins the fact that makes the pair
real — that `DjangoStreamStore` issues the same format this one does.
"""

from __future__ import annotations

import json

import pytest

from rakaia.jsonl_store import JsonlStreamStore
from rakaia.offsets import COMPOUND, PLAIN, ForeignOffset, format_of
from rakaia.store import StreamStore
from rakaia.subscription import poll

PATH = "s"


def _seed(store, n, marker="e"):
    store.create(PATH)
    for i in range(n):
        store.append(PATH, json.dumps({marker: i}).encode())
    return store


@pytest.fixture
def memory():
    return _seed(StreamStore(), 3)


@pytest.fixture
def files(tmp_path):
    return _seed(JsonlStreamStore(tmp_path / "a", fsync=False), 3)


class TestTheFormatsEachStoreIssues:
    """Which store mints which format — the fact the table is derived from,
    rather than a row of it."""

    def test_the_in_memory_store_issues_compound(self, memory):
        assert format_of(memory.get_current_offset(PATH)) is COMPOUND

    def test_the_file_store_issues_plain(self, files):
        """`PLAIN` is named for the durable store and deliberately shared, so a
        copy between the two entry-counting stores preserves offsets exactly."""
        assert format_of(files.get_current_offset(PATH)) is PLAIN


class TestAcrossFormatsACursorIsRefused:
    """Across two *different* formats, both directions. Byte-compatible or not,
    a format that belongs to another store is refused rather than resolved to a
    position of its own."""

    def test_a_file_cursor_is_refused_by_the_in_memory_store(self, memory, files):
        cursor = poll(files, PATH, None).cursor
        with pytest.raises(ForeignOffset):
            poll(memory, PATH, cursor)

    def test_an_in_memory_cursor_is_refused_by_the_file_store(self, memory, files):
        cursor = poll(memory, PATH, None).cursor
        with pytest.raises(ForeignOffset):
            poll(files, PATH, cursor)


class TestWithinOneFormatACursorIsAccepted:
    """The case that fails silently, and the reason ADR 0006 is a decision
    rather than a note.

    Two stores issuing the same format are indistinguishable to `offsets.after`,
    so a cursor crosses between them unchallenged. Both directions are pinned
    because they fail *differently*, and only one of them fails safely — an
    operator cannot derive that difference from behaviour.
    """

    def test_a_cursor_ahead_of_the_new_head_is_reported_as_rewound(self, tmp_path):
        """The safe direction: switching to an emptier log re-reads rather than
        skipping. Over-cautious, and loud enough to notice."""
        busy = _seed(JsonlStreamStore(tmp_path / "busy", fsync=False), 40, "OLD")
        cursor = poll(busy, PATH, None).cursor

        fresh = _seed(JsonlStreamStore(tmp_path / "fresh", fsync=False), 5, "NEW")
        resumed = poll(fresh, PATH, cursor)

        assert resumed.rewound is True
        assert len(resumed.messages) == 5

    def test_a_cursor_below_the_new_head_silently_skips_events(self, tmp_path):
        """The failure ADR 0006 exists to rule out.

        A position from one store, resumed against another that is already past
        it, is accepted without a rewind and without an error — and every event
        before it is skipped permanently. This asserts the *defect*, so that it
        is a decision to leave it and not an oversight; #232 is what changes it,
        and this test is where that change announces itself.
        """
        short = _seed(JsonlStreamStore(tmp_path / "short", fsync=False), 3, "OLD")
        cursor = poll(short, PATH, None).cursor

        longer = _seed(JsonlStreamStore(tmp_path / "long", fsync=False), 10, "NEW")
        resumed = poll(longer, PATH, cursor)

        assert resumed.rewound is False, "a rewind here would make this loud"
        delivered = [json.loads(m.data)["NEW"] for m in resumed.messages]
        assert delivered == [3, 4, 5, 6, 7, 8, 9]
        assert 0 not in delivered, "events 0-2 are skipped, silently"
