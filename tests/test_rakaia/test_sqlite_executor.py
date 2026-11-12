"""`SqliteExecutor` and `SqliteProjectionReader` against the shared contracts,
plus what only a real database can show: rows that outlive the executor, a batch
that is all or nothing, and no Django on the import path."""

from __future__ import annotations

import sqlite3
import subprocess
import sys

import pytest

from rakaia.effects import Delete, Retire, Update, Upsert
from rakaia.executors import MultipleRowsMatched
from rakaia.sqlite_executor import SqliteExecutor, SqliteProjectionReader
from rakaia.testing.executor_contract import ExecutorContract, ExecutorSeam
from rakaia.testing.projection_reader_contract import (
    ProjectionReaderContract,
    ReaderSeam,
)

MODEL = "app.Alert"


class TestSqliteExecutorContract(ExecutorContract):
    @pytest.fixture
    def seam(self) -> ExecutorSeam:
        executor = SqliteExecutor()
        return ExecutorSeam(
            executor=executor,
            reader=SqliteProjectionReader(executor.connection),
            model=MODEL,
            multiple_rows_error=MultipleRowsMatched,
        )


class TestSqliteProjectionReaderContract(ProjectionReaderContract):
    @pytest.fixture
    def seam(self) -> ReaderSeam:
        executor = SqliteExecutor()
        return ReaderSeam(
            apply=executor.apply,
            make_reader=lambda _effects: SqliteProjectionReader(executor.connection),
            model=MODEL,
        )


def _alert(key: str, **defaults) -> Upsert:
    return Upsert(
        model_label=MODEL,
        lookup={"stream_key": "s", "field_key": key},
        defaults=defaults,
    )


def test_rows_are_in_the_database_file_not_in_the_executor(tmp_path):
    path = tmp_path / "projections.db"
    executor = SqliteExecutor(path)
    executor.apply([_alert("a", severity="error")])
    executor.connection.close()

    reader = SqliteProjectionReader(sqlite3.connect(path))
    assert reader.get(MODEL, field_key="a").severity == "error"


def test_a_batch_that_fails_part_way_leaves_nothing_behind():
    """The second upsert cannot be stored (a value JSON cannot hold) after the
    first has been. Neither may stay."""
    executor = SqliteExecutor()
    executor.apply([_alert("kept")])

    with pytest.raises(TypeError):
        executor.apply([_alert("a"), _alert("b", message=object())])

    reader = SqliteProjectionReader(executor.connection)
    assert [r.field_key for r in reader.query(MODEL)] == ["kept"]


def test_a_failed_batch_does_not_keep_its_earlier_delete():
    executor = SqliteExecutor()
    executor.apply([_alert("a")])

    with pytest.raises(ValueError):
        executor.apply(
            [
                Delete(MODEL, {"stream_key": "s"}),
                Retire(MODEL, {"stream_key": "s"}, patch={}),
            ]
        )

    reader = SqliteProjectionReader(executor.connection)
    assert [r.field_key for r in reader.query(MODEL)] == ["a"]


def test_a_batch_inside_the_callers_transaction_leaves_it_open():
    executor = SqliteExecutor()
    connection = executor.connection
    connection.execute("BEGIN")
    executor.apply([_alert("a")])
    assert connection.in_transaction
    connection.rollback()

    assert SqliteProjectionReader(connection).query(MODEL) == []


def test_reading_a_model_never_written_is_empty():
    reader = SqliteProjectionReader(sqlite3.connect(":memory:"))

    assert reader.get("app.Nothing", x=1) is None
    assert reader.query("app.Nothing") == []


def test_importing_it_imports_no_django():
    code = (
        "import sys, rakaia.sqlite_executor; "
        "bad = [m for m in sys.modules if m.split('.')[0] in "
        "('django', 'django_rakaia')]; "
        "assert not bad, bad"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


# -- lookups by pk, and the SQL-building guards -------------------------------


def _both(*batches):
    """Apply the same batches to SQLite and in memory; return the two readers."""
    from rakaia.executors import InMemoryProjections

    sqlite, memory = SqliteExecutor(), InMemoryProjections()
    for batch in batches:
        sqlite.apply(batch)
        memory.apply(batch)
    return sqlite.reader(), memory


def _rows(reader, label=MODEL, **lookup):
    return [vars(r) for r in reader.filter(label, **lookup)]


def _seeded(*more):
    return _both([_alert("a", severity="x"), _alert("b", severity="x")], *more)


def test_get_by_pk_matches_in_memory():
    sqlite, memory = _seeded()

    assert vars(sqlite.get(MODEL, pk=2)) == vars(memory.get(MODEL, pk=2))
    assert sqlite.get(MODEL, pk=99) is None


def test_update_by_pk_matches_in_memory():
    sqlite, memory = _seeded(
        [Update(MODEL, {"pk": 2}, defaults={"severity": "changed"})]
    )

    assert _rows(sqlite) == _rows(memory)
    assert sqlite.get(MODEL, pk=2).severity == "changed"


def test_delete_by_pk_matches_in_memory():
    sqlite, memory = _seeded([Delete(MODEL, {"pk": 1})])

    assert _rows(sqlite) == _rows(memory)
    assert [r.field_key for r in sqlite.query(MODEL)] == ["b"]


def test_retire_by_pk_matches_in_memory():
    sqlite, memory = _seeded([Retire(MODEL, {"pk": 1}, patch={"resolved_at": "t"})])

    assert _rows(sqlite) == _rows(memory)
    assert sqlite.get(MODEL, pk=1).resolved_at == "t"


def test_an_upsert_that_names_its_pk_stores_it_as_the_pk():
    sqlite, memory = _both(
        [Upsert(MODEL, {"pk": 5, "field_key": "a"}, defaults={"severity": "x"})]
    )

    assert sqlite.get(MODEL, field_key="a").pk == memory.get(MODEL, field_key="a").pk
    assert vars(sqlite.get(MODEL, pk=5)) == vars(memory.get(MODEL, pk=5))


def test_a_pk_is_not_stored_in_the_json_column():
    executor = SqliteExecutor()
    created = Upsert(MODEL, {"pk": 5, "field_key": "a"}, defaults={"severity": "x"})
    executor.apply([created])
    executor.apply(
        [Upsert(MODEL, {"field_key": "a"}, defaults={"severity": "y"})]
    )  # the update path stores too
    executor.apply([Update(MODEL, {"pk": 5}, defaults={"message": "m"})])

    [(raw,)] = executor.connection.execute(f'SELECT row FROM "{MODEL}"').fetchall()
    assert "pk" not in raw


def test_writing_a_pk_through_defaults_or_a_patch_is_refused():
    executor = SqliteExecutor()
    executor.apply([_alert("a")])

    with pytest.raises(ValueError):
        executor.apply([_alert("a", pk=9)])
    with pytest.raises(ValueError):
        executor.apply([Retire(MODEL, {"stream_key": "s"}, patch={"pk": 9})])
    assert executor.reader().get(MODEL, field_key="a").pk == 1


def test_an_update_that_writes_a_pk_is_refused():
    executor = SqliteExecutor()
    executor.apply([_alert("a")])

    with pytest.raises(ValueError):
        executor.apply([Update(MODEL, {"field_key": "a"}, defaults={"pk": 9})])


@pytest.mark.parametrize(
    "bad",
    [
        Update(MODEL, {"field_key": "a"}, defaults={"pk": 9}),
        Retire(MODEL, {"field_key": "a"}, patch={"pk": 9}),
        Upsert(MODEL, {"field_key": "z"}, defaults={"pk": 9}),
    ],
)
def test_a_pk_refusal_rolls_back_the_good_effects_before_it(bad):
    executor = SqliteExecutor()
    executor.apply([_alert("a", severity="before")])

    with pytest.raises(ValueError):
        executor.apply([_alert("new"), _alert("a", severity="after"), bad])

    rows = executor.reader().query(MODEL)
    assert [(r.field_key, r.severity) for r in rows] == [("a", "before")]


def test_a_pk_named_in_a_lookup_is_not_in_the_json_right_after_the_create():
    executor = SqliteExecutor()
    executor.apply(
        [Upsert(MODEL, {"pk": 5, "field_key": "a"}, defaults={"severity": "x"})]
    )

    [(raw,)] = executor.connection.execute(f'SELECT row FROM "{MODEL}"').fetchall()
    assert "pk" not in raw


@pytest.mark.parametrize("isolation_level", ["", None])
def test_a_failed_batch_leaves_no_transaction_open(isolation_level):
    executor = SqliteExecutor(
        sqlite3.connect(":memory:", isolation_level=isolation_level)
    )

    with pytest.raises(TypeError):
        executor.apply([_alert("a"), _alert("b", message=object())])

    assert not executor.connection.in_transaction


@pytest.mark.parametrize("isolation_level", ["", None])
def test_a_good_batch_is_committed_when_the_caller_has_no_transaction(
    isolation_level, tmp_path
):
    path = tmp_path / "p.db"
    executor = SqliteExecutor(sqlite3.connect(path, isolation_level=isolation_level))
    executor.apply([_alert("a")])

    assert not executor.connection.in_transaction
    other = SqliteProjectionReader(sqlite3.connect(path))
    assert [r.field_key for r in other.query(MODEL)] == ["a"]


def test_a_label_with_a_double_quote_is_its_own_table():
    label = 'app."Odd"'
    executor = SqliteExecutor()
    executor.apply([Upsert(label, {"k": "a"}, defaults={"v": 1})])

    assert executor.reader().get(label, k="a").v == 1


def test_a_hostile_lookup_key_is_only_a_key():
    sqlite, memory = _seeded()
    hostile = {"stream_key'] OR 1=1 --": "zzz"}

    assert _rows(sqlite, **hostile) == _rows(memory, **hostile) == []


def test_an_operator_key_is_not_an_exact_match_in_sql():
    """`field_key__in="ab"` is an operator on `field_key`, not a column of that
    name holding "ab"; both executors must agree on what it matches."""
    sqlite, memory = _seeded()

    assert _rows(sqlite, field_key__in="ab") == _rows(memory, field_key__in="ab")
    assert len(_rows(sqlite, field_key__in="ab")) == 2


def test_a_value_json_cannot_hold_fails_an_update_and_keeps_the_old_value():
    executor = SqliteExecutor()
    executor.apply([_alert("a", message="first")])

    with pytest.raises(TypeError):
        executor.apply([_alert("a", message=object())])

    assert executor.reader().get(MODEL, field_key="a").message == "first"


def test_close_closes_a_connection_the_executor_opened(tmp_path):
    executor = SqliteExecutor(tmp_path / "p.db")
    executor.close()

    with pytest.raises(sqlite3.ProgrammingError):
        executor.connection.execute("SELECT 1")


def test_close_leaves_a_connection_the_caller_passed_in():
    connection = sqlite3.connect(":memory:")
    SqliteExecutor(connection).close()

    connection.execute("SELECT 1")
