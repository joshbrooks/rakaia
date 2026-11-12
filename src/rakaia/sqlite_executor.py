"""Replay into a plain SQLite database, with nothing but the standard library.

`SqliteExecutor` is an `Executor` that writes effects to a SQLite file (or an
in-memory database), and `SqliteProjectionReader` reads those rows back. Each
model label is a table, created on first use, named exactly as the label
(``"app.Alert"``) so ``sqlite3`` can read it too. A table has an integer ``pk``
and one ``row`` column holding the row as JSON, so there is no schema to migrate:
a new column is just a new key. Values must therefore be JSON-serialisable.

A batch is applied inside one SAVEPOINT, so it lands whole or not at all. Lookups
match exactly, ``__in`` and ``__isnull``, as `InMemoryProjections` does, which this
is held to by the same conformance suites.

Known limits, all from storing rows as JSON in SQLite: a column nothing wrote
raises ``AttributeError`` when read (Django gives ``None``); tuples come back as
lists and integer dict keys as strings; and labels that differ only by case name
one table when writing (SQLite table names are case-insensitive), yet reads,
updates and deletes look the table up by exact spelling, so through the other
casing they find nothing.
"""

from __future__ import annotations

import json
import re
import sqlite3
from collections.abc import Iterable, Iterator
from contextlib import contextmanager
from os import PathLike
from types import SimpleNamespace
from typing import Any

from .applying import RowAccessor, UpsertOutcome, apply_effects
from .effects import (
    ApplyReport,
    Delete,
    Effect,
    Exclude,
    Retire,
    SpareKeys,
    Update,
    Upsert,
)
from .executors import InMemoryProjections, MultipleRowsMatched, _in_database_order

__all__ = ["SqliteExecutor", "SqliteProjectionReader"]

_PLAIN_KEY = re.compile(r"[A-Za-z_][A-Za-z0-9_]*\Z")


def _table(model_label: str) -> str:
    """The label as a quoted SQL identifier."""
    return '"' + model_label.replace('"', '""') + '"'


def _create(connection: sqlite3.Connection, model_label: str) -> None:
    connection.execute(
        f"CREATE TABLE IF NOT EXISTS {_table(model_label)} "
        "(pk INTEGER PRIMARY KEY AUTOINCREMENT, row TEXT NOT NULL)"
    )


def _exists(connection: sqlite3.Connection, model_label: str) -> bool:
    found = connection.execute(
        "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = ?",
        (model_label,),
    ).fetchone()
    return found is not None


def _narrowing(lookup: dict[str, Any]) -> tuple[str, list[Any]]:
    """SQL that keeps every row `lookup` could match, and maybe fewer.

    Only plain exact matches on text and numbers are pushed down (``pk`` as the
    column, anything else out of the JSON); operator keys are left alone. The
    caller still checks each row it gets back, so this is an optimisation and
    never the answer.
    """
    clauses: list[str] = []
    params: list[Any] = []
    for key, value in lookup.items():
        if (
            "__" in key
            or not isinstance(value, (str, int, float))
            or isinstance(value, bool)
        ):
            continue
        if key == "pk":
            clauses.append("pk = ?")
            params.append(value)
        elif _PLAIN_KEY.match(key):
            clauses.append(f"json_extract(row, '$.{key}') = ?")
            params.append(value)
    return (" WHERE " + " AND ".join(clauses)) if clauses else "", params


def _matching(
    connection: sqlite3.Connection, model_label: str, lookup: dict[str, Any]
) -> list[dict[str, Any]]:
    """Matching rows in pk order, each with its ``pk``; none if there is no table."""
    if not _exists(connection, model_label):
        return []
    where, params = _narrowing(lookup)
    found = connection.execute(
        f"SELECT pk, row FROM {_table(model_label)}{where} ORDER BY pk", params
    )
    rows = [{**json.loads(raw), "pk": pk} for pk, raw in found]
    return [r for r in rows if InMemoryProjections._matches(r, lookup)]


def _connect(database: str | PathLike[str] | sqlite3.Connection) -> sqlite3.Connection:
    if isinstance(database, sqlite3.Connection):
        return database
    return sqlite3.connect(database)


class SqliteProjectionReader:
    """A `ProjectionReader` over the tables `SqliteExecutor` writes.

    Rows come back as `SimpleNamespace`, so a handler reads ``row.suku`` as it
    would on an ORM instance, with ``pk`` as the primary key. A model with no
    table yet reads as empty. Values are what JSON gives back, and a column
    nothing wrote raises ``AttributeError`` (see the module docstring).
    """

    def __init__(self, connection: sqlite3.Connection) -> None:
        self.connection = connection

    def get(self, model_label: str, /, **lookup: Any) -> Any | None:
        """The first row matching `lookup` in pk order, or None."""
        found = _matching(self.connection, model_label, lookup)
        return SimpleNamespace(**found[0]) if found else None

    def filter(self, model_label: str, /, **lookup: Any) -> list[Any]:
        """The rows matching `lookup`."""
        return [
            SimpleNamespace(**r)
            for r in _matching(self.connection, model_label, lookup)
        ]

    def query(self, model_label: str, /) -> list[Any]:
        """Every row of the model."""
        return self.filter(model_label)


class SqliteExecutor:
    """An `Executor` that applies effects to a SQLite database.

    Pass a path, or an open `sqlite3.Connection` to share one with a reader or
    with the rest of your application; the default is a private in-memory
    database. It applies through `rakaia.applying.apply_effects`, so ordering,
    `Ref` resolution and the report are the same code as every other executor.

    A batch that raises, including from an unresolvable `Ref` or a value JSON
    cannot hold, is rolled back whole. When your connection has no transaction
    open, each `apply` commits at the end of the batch; when it has one open, that
    stays open and nothing is committed, so the commit is yours. The ``pk`` column
    is the only source of a row's ``pk``: a lookup may name it (an upsert then
    creates the row with that ``pk``), but a ``pk`` in ``defaults`` or a retire's
    ``patch`` raises `ValueError`. An upsert whose lookup matches more than
    one row raises `MultipleRowsMatched`. `skip_unchanged` is not implemented, so
    an upsert is never reported as skipped.
    """

    def __init__(
        self, database: str | PathLike[str] | sqlite3.Connection = ":memory:"
    ) -> None:
        self._owns_connection = not isinstance(database, sqlite3.Connection)
        self.connection = _connect(database)

    def close(self) -> None:
        """Close the connection if this executor opened it from a path; one you
        passed in is yours to close."""
        if self._owns_connection:
            self.connection.close()

    def reader(self) -> SqliteProjectionReader:
        """A reader over the same connection."""
        return SqliteProjectionReader(self.connection)

    @contextmanager
    def _batch(self) -> Iterator[None]:
        self.connection.execute("SAVEPOINT rakaia_batch")
        try:
            yield
        except BaseException:
            self.connection.execute("ROLLBACK TO rakaia_batch")
            self.connection.execute("RELEASE rakaia_batch")
            raise
        self.connection.execute("RELEASE rakaia_batch")

    def apply(self, effects: Iterable[Effect]) -> ApplyReport:
        with self._batch():
            return apply_effects(
                effects,
                upsert=self._upsert,
                update_run=self._update_run,
                delete=self._delete,
                retire=self._retire,
            )

    @staticmethod
    def _refuse_pk(values: dict[str, Any] | None) -> None:
        if values and "pk" in values:
            raise ValueError("pk is the table's own column and cannot be written")

    def _store(self, model_label: str, row: dict[str, Any]) -> None:
        stored = {k: v for k, v in row.items() if k != "pk"}
        self.connection.execute(
            f"UPDATE {_table(model_label)} SET row = ? WHERE pk = ?",
            (json.dumps(stored), row["pk"]),
        )

    def _upsert(self, eff: Upsert) -> tuple[UpsertOutcome, RowAccessor]:
        label = eff.model_label
        self._refuse_pk(eff.defaults)
        _create(self.connection, label)
        existing = _matching(self.connection, label, eff.lookup)
        if len(existing) > 1:
            raise MultipleRowsMatched(
                f"{label} upsert lookup {eff.lookup} matched {len(existing)} rows"
            )
        if existing:
            row = existing[0]
            row.update(eff.defaults or {})
            self._store(label, row)
            outcome: UpsertOutcome = "written"
        else:
            fields = {k: v for k, v in eff.lookup.items() if "__" not in k}
            fields.update(eff.defaults or {})
            named_pk = fields.pop("pk", None)
            cursor = self.connection.execute(
                f"INSERT INTO {_table(label)} (pk, row) VALUES (?, ?)",
                (named_pk, json.dumps(fields)),
            )
            row = {**fields, "pk": cursor.lastrowid}
            outcome = "created"
        return (
            outcome,
            lambda field: row["pk"] if field in ("pk", "id") else row.get(field),
        )

    def _update_run(self, effects: list[Update]) -> None:
        for eff in effects:
            self._refuse_pk(eff.defaults)
            if not eff.defaults:
                continue
            for row in _matching(self.connection, eff.model_label, eff.lookup):
                row.update(eff.defaults)
                self._store(eff.model_label, row)

    def _delete(self, eff: Delete) -> None:
        doomed = _matching(self.connection, eff.model_label, eff.lookup)
        if isinstance(eff.spare, Exclude) and eff.spare.lookup:
            spare = eff.spare.lookup
            doomed = [r for r in doomed if not InMemoryProjections._matches(r, spare)]
        elif isinstance(eff.spare, SpareKeys):
            keys = eff.spare.keys
            doomed = [r for r in doomed if not InMemoryProjections._spared(r, keys)]
        for row in doomed:
            self.connection.execute(
                f"DELETE FROM {_table(eff.model_label)} WHERE pk = ?", (row["pk"],)
            )

    def _retire(self, eff: Retire) -> list[dict[str, Any]]:
        if not eff.patch:
            raise ValueError("retire effect requires a non-empty patch")
        self._refuse_pk(eff.patch)
        targets = _matching(self.connection, eff.model_label, eff.lookup)
        if eff.spare is not None:
            keys = eff.spare.keys
            targets = [r for r in targets if not InMemoryProjections._spared(r, keys)]
        flipped: list[dict[str, Any]] = []
        if eff.transition is not None:
            cols = list(eff.transition.key_fields)
            flipped = _in_database_order(
                [{c: r.get(c) for c in cols} for r in targets], cols
            )
        for row in targets:
            row.update(eff.patch)
            self._store(eff.model_label, row)
        return flipped
