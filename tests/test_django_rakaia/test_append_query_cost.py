"""What one append costs in queries, pinned statement by statement.

A durable append is a handful of statements, and every one of them is easy to
duplicate by accident: an admission check that re-reads the row it was handed, a
high-water read repeated either side of a get-or-create, an offset re-derived
from a `MAX()` scan over an answer already held in one row. None of that changes
a single assertion in the rest of the suite — the append still lands, at the
right offset, with the right envelope — so the cost drifts up silently and is
only ever noticed from the outside, by someone reading their own query log
(#202, which reported 13 and found three duplicates in it).

So these tests assert the *set of statements*, not a number: one read of the
stream row, one of the high-water, one insert each for the event and the entry,
one update of the high-water. Naming the tables is what makes a failure say
which statement came back rather than only that the total moved.

**Transaction control is filtered out**, because it is not portable: Postgres
reports `BEGIN`/`COMMIT` through the cursor and SQLite does not, so a raw total
would be two different numbers on the two CI legs. `_data_queries` keeps the
statements that touch a table.

`transaction=True`, like every other test that reaches a `select_for_update()`
here: the counts are measured against a real transaction the store opened
itself, not one pytest-django lent it.
"""

from __future__ import annotations

import re

import pytest
from django.db import connection
from django.test.utils import CaptureQueriesContext

from django_rakaia.django_store import DjangoStreamStore
from rakaia.types import AppendOptions

# BEGIN / COMMIT / SAVEPOINT / RELEASE SAVEPOINT / ROLLBACK, in the shapes
# Django's backends emit them.
_TRANSACTION_CONTROL = re.compile(
    r"^\s*(BEGIN|COMMIT|ROLLBACK|SAVEPOINT|RELEASE\s+SAVEPOINT)\b", re.IGNORECASE
)


def _data_queries(ctx: CaptureQueriesContext) -> list[str]:
    """The captured statements that touch a table, oldest first."""
    return [
        q["sql"]
        for q in ctx.captured_queries
        if q["sql"] and not _TRANSACTION_CONTROL.match(q["sql"])
    ]


def _shapes(ctx: CaptureQueriesContext) -> list[str]:
    """Each data statement as `VERB table`, e.g. `SELECT rakaia_stream`.

    Enough to tell one statement of an append from another, and stable across
    backends — Postgres quotes identifiers and SQLite quotes them too, but the
    parameter placeholders and casts differ.
    """
    shapes = []
    for sql in _data_queries(ctx):
        verb = sql.split(None, 1)[0].upper()
        tables = re.findall(r'"(rakaia_\w+)"', sql)
        shapes.append(f"{verb} {tables[0] if tables else '?'}")
    return shapes


@pytest.mark.django_db(transaction=True)
class TestAppendQueryCost:
    def _warm(self, path: str = "s") -> DjangoStreamStore:
        """A store with `path` created and appended to once.

        The *first* append to a path is deliberately dearer: it creates the
        high-water row and seeds it from a `MAX(offset)` scan, because a
        `high` of 0 means either "new path" or "install upgraded across
        migration 0005", and only the entry table can tell those apart (see
        `Stream.get_next_offset_block`). That is paid once per path, ever.
        These tests measure the steady state every append after it pays.
        """
        store = DjangoStreamStore()
        store.create(path)
        store.append(path, b'{"n": 0}')
        return store

    def test_one_append_is_five_statements(self) -> None:
        store = self._warm()
        with CaptureQueriesContext(connection) as ctx:
            store.append("s", b'{"n": 1}', AppendOptions(label="x"))

        assert _shapes(ctx) == [
            # The admission checks and the write are one step, so the row is
            # read under a lock -- once. It used to be read twice: an unlocked
            # read before the transaction to reap an expired stream, then the
            # locked one inside it (#202).
            "SELECT rakaia_stream",
            "INSERT rakaia_streamevent",
            # The high-water, under its own lock, and advanced. No `MAX(offset)`
            # scan beside it: the watermark is authoritative once advanced, and
            # re-deriving the head would be the one statement here whose cost
            # grows with the length of the stream.
            "SELECT rakaia_streamoffsetwatermark",
            "UPDATE rakaia_streamoffsetwatermark",
            "INSERT rakaia_streamentry",
        ], _data_queries(ctx)

    def test_the_cost_does_not_grow_with_the_stream(self) -> None:
        """Append 50 more and the 50th costs what the 1st did.

        The guard against a re-derived offset: a `MAX(offset)` scan or an
        entry-table count would keep every assertion above true while making
        each append dearer than the last on a long stream.
        """
        store = self._warm()
        with CaptureQueriesContext(connection) as first:
            store.append("s", b'{"n": 1}')
        for n in range(2, 50):
            store.append("s", b'{"n": %d}' % n)
        with CaptureQueriesContext(connection) as fiftieth:
            store.append("s", b'{"n": 50}')

        assert _shapes(fiftieth) == _shapes(first)

    def test_an_append_that_closes_costs_one_more_update(self) -> None:
        """`Stream-Closed: true` adds the close, and nothing else.

        In particular it does not re-read the stream row it is closing.
        """
        store = self._warm()
        with CaptureQueriesContext(connection) as ctx:
            store.append("s", b'{"last": 1}', AppendOptions(close=True))

        assert _shapes(ctx) == [
            "SELECT rakaia_stream",
            "INSERT rakaia_streamevent",
            "SELECT rakaia_streamoffsetwatermark",
            "UPDATE rakaia_streamoffsetwatermark",
            "INSERT rakaia_streamentry",
            "UPDATE rakaia_stream",
        ], _data_queries(ctx)

    def test_a_refused_append_writes_nothing_and_reads_once(self) -> None:
        """A closed stream is refused for the price of the read that refuses it.

        The expiry reap moved *out* of the transaction and into a rollback
        (`_locked_write`), so this is also the case where a second stream read
        would be easiest to reintroduce: nothing else here touches the row.
        """
        store = self._warm()
        store.close_stream("s")
        with CaptureQueriesContext(connection) as ctx:
            result = store.append("s", b'{"late": 1}')

        assert result.message is None and result.stream_closed
        assert _shapes(ctx) == ["SELECT rakaia_stream"], _data_queries(ctx)

    @pytest.mark.parametrize("batch", [1, 4, 20])
    def test_append_many_is_flat_in_the_batch_size(self, batch: int) -> None:
        """The claim in `append_many`'s docstring, held to the query log.

        One transaction, one high-water lock, one `bulk_create` each for the
        events and the entries -- so a batch of 20 issues exactly what a batch
        of 1 does. This is the property that took a nine-event save from ~95
        queries to ~20 for the reporter of #202, and it is worth pinning: a
        per-item `save()` slipped into the loop would leave every other test
        passing.
        """
        store = self._warm()
        with CaptureQueriesContext(connection) as ctx:
            store.append_many("s", [(b'{"n": 1}', AppendOptions())] * batch)

        assert _shapes(ctx) == [
            "SELECT rakaia_stream",
            "INSERT rakaia_streamevent",
            "SELECT rakaia_streamoffsetwatermark",
            "UPDATE rakaia_streamoffsetwatermark",
            "INSERT rakaia_streamentry",
        ], _data_queries(ctx)
