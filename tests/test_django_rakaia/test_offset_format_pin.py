"""`DjangoStreamStore` issues `PLAIN` — the fact ADR 0006's table turns on.

`test_rakaia/test_cross_backend_cursors.py` pins what happens when two stores
share an offset format, using two file-backed stores. This is the assertion that
makes that pair the *real* one: the durable store and the file store issue the
same format, so a cursor really does cross between them unchallenged in a
deployment.

Split out rather than folded in there because it needs a database, and the
module it pins the behaviour in does not.
"""

from __future__ import annotations

import pytest

from django_rakaia.django_store import DjangoStreamStore
from rakaia.offsets import PLAIN, format_of

pytestmark = pytest.mark.django_db

PATH = "s"


def test_the_durable_store_issues_the_plain_format():
    """Shared with `JsonlStreamStore` deliberately: both count entries, which is
    what lets a copy between them preserve every offset exactly. The cost is
    that `offsets.after` cannot tell one's cursor from the other's — ADR 0005's
    amended table, and ADR 0006's silent row."""
    store = DjangoStreamStore()
    store.create(PATH)
    store.append(PATH, b'{"n": 1}')

    assert format_of(store.get_current_offset(PATH)) is PLAIN


def test_a_durable_offset_is_accepted_by_the_file_store(tmp_path):
    """The pair, end to end, with the real durable store on one side.

    Not `ForeignOffset` — which is what every other pair of stores raises, and
    what this one raised too until a third store reused `PLAIN`. Asserting the
    acceptance keeps it a recorded decision rather than a surprise.
    """
    from rakaia.jsonl_store import JsonlStreamStore

    durable = DjangoStreamStore()
    durable.create(PATH)
    for i in range(3):
        durable.append(PATH, b'{"n": %d}' % i)
    durable_offset = durable.get_current_offset(PATH)

    files = JsonlStreamStore(tmp_path / "files", fsync=False)
    files.create(PATH)
    for i in range(10):
        files.append(PATH, b'{"n": %d}' % i)

    # No raise: the file store parses a position the durable store minted.
    messages, _ = files.read(PATH, durable_offset)
    assert len(messages) == 7
