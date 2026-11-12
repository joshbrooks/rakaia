#!/usr/bin/env python3
"""`sqlite_replay` — replay a stream into a real database file, with no Django.

Rakaia says replay does not need Django, and until `SqliteExecutor` the only
executor that wrote to an actual database was the Django one. This replays an
order stream into a SQLite file using nothing but the standard library, then
reads the result back through `SqliteProjectionReader` and through a plain
`sqlite3` connection.

  * a replay is idempotent  -- running it twice leaves the same rows
  * the file is the result  -- a second connection sees what the first wrote
  * a batch is all or none  -- a batch that fails part way leaves no trace

Runs as a plain script. No Django, no settings, no migrations:

    just sqlite-replay-demo
    # or: uv run python examples/sqlite_replay/demo.py
"""

from __future__ import annotations

import json
import sqlite3
import sys
import tempfile
from pathlib import Path

from rakaia import (
    AppendOptions,
    HandlerRegistry,
    SqliteExecutor,
    SqliteProjectionReader,
    StreamStore,
    Upsert,
    replay,
)

ORDER = "shop.Order"
EVENTS = [
    {"order_id": "A-1", "status": "placed", "total_cents": 1250},
    {"order_id": "A-2", "status": "placed", "total_cents": 400},
    {"order_id": "A-1", "status": "shipped", "total_cents": 1250},
]


def order_totals(event: dict) -> Upsert:
    """The latest state of each order. Amounts are whole cents: rows are stored
    as JSON, which has no decimal type."""
    return Upsert(
        model_label=ORDER,
        lookup={"order_id": event["order_id"]},
        defaults={"status": event["status"], "total_cents": event["total_cents"]},
    )


def _hdr(title: str) -> None:
    print(f"\n{title}")
    print("-" * 62)


def build_store() -> tuple[StreamStore, HandlerRegistry]:
    store = StreamStore()
    store.create("orders", content_type="application/json")
    for event in EVENTS:
        store.append("orders", json.dumps(event).encode(), AppendOptions())
    registry = HandlerRegistry()
    registry.register("order_totals", "orders", order_totals, 0, None)
    return store, registry


def section_replay(db: Path) -> None:
    _hdr("[1] Replay into a SQLite file, twice")
    store, registry = build_store()
    executor = SqliteExecutor(db)

    result = replay(store, "orders", executor, handler_registry=registry)
    print(f"    {result.events_processed} events -> {result.effects_applied} effects")
    replay(store, "orders", executor, handler_registry=registry)

    reader = SqliteProjectionReader(executor.connection)
    orders = sorted(reader.query(ORDER), key=lambda r: r.order_id)
    assert [(o.order_id, o.status) for o in orders] == [
        ("A-1", "shipped"),
        ("A-2", "placed"),
    ]
    print("    A-1 shipped, A-2 placed; the second replay added nothing ✓")
    executor.connection.close()


def section_plain_sqlite(db: Path) -> None:
    _hdr("[2] The file is an ordinary SQLite database")
    plain = sqlite3.connect(db)
    rows = plain.execute(f'SELECT pk, row FROM "{ORDER}" ORDER BY pk').fetchall()
    plain.close()
    assert [pk for pk, _ in rows] == [1, 2]
    assert json.loads(rows[0][1]) == {
        "order_id": "A-1",
        "status": "shipped",
        "total_cents": 1250,
    }
    print(f'    SELECT pk, row FROM "{ORDER}" -> {len(rows)} rows of JSON ✓')


def section_all_or_nothing(db: Path) -> None:
    _hdr("[3] A batch that fails part way leaves no trace")
    executor = SqliteExecutor(db)
    before = [r.order_id for r in executor.reader().query(ORDER)]
    try:
        executor.apply(
            [
                Upsert(model_label=ORDER, lookup={"order_id": "A-3"}, defaults={}),
                # A set is not JSON, so this cannot be stored.
                Upsert(
                    model_label=ORDER,
                    lookup={"order_id": "A-4"},
                    defaults={"tags": {"rush"}},
                ),
            ]
        )
    except TypeError:
        pass
    else:  # pragma: no cover
        raise AssertionError("expected the second upsert to fail")
    after = [r.order_id for r in executor.reader().query(ORDER)]
    assert before == after, (before, after)
    print(f"    A-3 was written, then rolled back; still {len(after)} orders ✓")
    executor.connection.close()


def section_no_django() -> None:
    _hdr("[4] No Django was imported")
    loaded = sorted(m for m in sys.modules if m.split(".")[0] == "django")
    assert not loaded, loaded
    print("    nothing named django is in sys.modules ✓")


def main() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        db = Path(tmp) / "orders.db"
        section_replay(db)
        section_plain_sqlite(db)
        section_all_or_nothing(db)
    section_no_django()
    print("\nAll sqlite_replay checks passed ✓")


if __name__ == "__main__":
    main()
