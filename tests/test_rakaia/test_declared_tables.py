"""A handler's declared reads and writes (GitHub #375), checked during replay."""

from __future__ import annotations

import copy
import json
import warnings

import pytest

from rakaia.declared_tables import (
    CheckedReader,
    UndeclaredTableWarning,
    canonical_tables,
)
from rakaia.effects import Delete, ExternalEffect, Retire, Update, Upsert
from rakaia.executors import InMemoryProjections
from rakaia.registry import (
    HANDLERS_META_STREAM,
    HandlerRegistry,
    UpcasterRegistry,
    register_handler,
    register_simple,
)
from rakaia.replaying import replay
from rakaia.seed import seed_stream
from rakaia.store import StreamStore


@pytest.fixture
def store() -> StreamStore:
    return StreamStore()


_EVENTS = [
    {"schema_version": 1, "form_type": "F", "key": "a", "project": "P1"},
    {"schema_version": 1, "form_type": "F", "key": "b", "project": "P1"},
]


def _row(event):
    return Upsert(model_label="app.Row", lookup={"key": event["key"]}, defaults={})


def _row_and_audit(event):
    return [
        _row(event),
        Upsert(model_label="app.Audit", lookup={"key": event["key"]}, defaults={}),
    ]


def _set_status(event):
    return Update(
        model_label="app.Row", lookup={"key": event["key"]}, defaults={"status": 1}
    )


def _set_status_and_owner(event):
    return Update(
        model_label="app.Row",
        lookup={"key": event["key"]},
        defaults={"status": 1, "owner": 2},
    )


def _retire(event):
    return Retire(
        model_label="app.Row", lookup={"key": event["key"]}, patch={"gone": True}
    )


def _filter_projects(event, reader):
    list(reader.filter("app.Project", name=event["project"]))


def _query_projects(_event, reader):
    list(reader.query("app.Project"))


def _drop(event):
    return Delete(model_label="app.Row", lookup={"key": event["key"]})


def _email(event):
    return ExternalEffect(kind="email", payload={"to": event["key"]})


def _link(event, reader):
    reader.get("app.Project", name=event["project"])
    return Update(
        model_label="app.Row", lookup={"key": event["key"]}, defaults={"project": 1}
    )


def _replay(store: StreamStore, reg: HandlerRegistry) -> list[str]:
    """Replay `_EVENTS` and return the text of every UndeclaredTableWarning."""
    seed_stream("s", _EVENTS, store=store)
    proj = InMemoryProjections()
    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        replay(
            store,
            "s",
            proj,
            handler_registry=reg,
            upcaster_registry=UpcasterRegistry(),
            reader=proj,
        )
    return [str(w.message) for w in caught if w.category is UndeclaredTableWarning]


def _reg(fn, stage: int = 0, **declared) -> HandlerRegistry:
    reg = HandlerRegistry()
    reg.register("h", "F", fn, 0, match_field="form_type", stage=stage, **declared)
    return reg


class TestWrites:
    def test_a_declared_table_is_quiet(self, store: StreamStore):
        assert _replay(store, _reg(_row, writes={"app.Row"})) == []

    def test_a_table_left_out_is_named_once_per_replay(self, store: StreamStore):
        # Two events emit the same undeclared write; one warning, not two.
        messages = _replay(store, _reg(_row_and_audit, writes={"app.Row"}))
        assert len(messages) == 1
        assert "'h'" in messages[0] and "'app.Audit'" in messages[0]

    def test_an_update_needs_only_the_columns_it_sets(self, store: StreamStore):
        assert _replay(store, _reg(_set_status, writes={"app.Row.status"})) == []

    def test_an_update_setting_an_undeclared_column_names_it(self, store: StreamStore):
        messages = _replay(
            store, _reg(_set_status_and_owner, writes={"app.Row.status"})
        )
        assert len(messages) == 1 and "'app.Row.owner'" in messages[0]

    def test_a_declared_table_covers_every_column(self, store: StreamStore):
        assert _replay(store, _reg(_set_status_and_owner, writes={"app.Row"})) == []

    def test_retiring_rows_needs_the_table_not_a_column(self, store: StreamStore):
        messages = _replay(store, _reg(_retire, writes={"app.Row.gone"}))
        assert len(messages) == 1 and "'app.Row'" in messages[0]

    def test_removing_rows_needs_the_table_not_a_column(self, store: StreamStore):
        messages = _replay(store, _reg(_drop, writes={"app.Row.status"}))
        assert len(messages) == 1 and "'app.Row'" in messages[0]

    def test_an_effect_outside_the_database_is_not_checked(self, store: StreamStore):
        assert _replay(store, _reg(_email, writes=set())) == []

    def test_an_undeclared_handler_is_not_checked(self, store: StreamStore):
        assert _replay(store, _reg(_row_and_audit)) == []


class TestReads:
    def test_a_declared_read_is_quiet(self, store: StreamStore):
        reg = _reg(_link, stage=1, reads={"app.Project"}, writes={"app.Row.project"})
        assert _replay(store, reg) == []

    def test_a_read_left_out_is_named(self, store: StreamStore):
        reg = _reg(_link, stage=1, reads=set(), writes={"app.Row.project"})
        messages = _replay(store, reg)
        assert len(messages) == 1
        assert "read 'app.Project'" in messages[0] and "reads=" in messages[0]

    def test_a_declared_column_covers_reading_its_table(self, store: StreamStore):
        # A reader hands back whole rows, so which column is used cannot be seen.
        reg = _reg(_link, stage=1, reads={"app.Project.name"}, writes={"app.Row"})
        assert _replay(store, reg) == []

    def test_undeclared_reads_are_not_checked(self, store: StreamStore):
        assert _replay(store, _reg(_link, stage=1, writes={"app.Row"})) == []

    @pytest.mark.parametrize("fn", [_filter_projects, _query_projects])
    def test_every_reader_call_is_checked(self, store: StreamStore, fn):
        messages = _replay(store, _reg(fn, stage=1, reads=set()))
        assert len(messages) == 1 and "read 'app.Project'" in messages[0]

    def test_the_wrapped_reader_can_be_copied(self):
        checked = CheckedReader(InMemoryProjections(), frozenset())
        assert copy.copy(checked).missing == []


class TestRegistering:
    def test_a_string_is_one_name_not_its_characters(self):
        assert canonical_tables("app.Row", "writes") == frozenset({"app.Row"})

    @pytest.mark.parametrize("bad", ["Row", "app.", "a.b.c.d", ".Row"])
    def test_a_malformed_name_is_refused(self, bad: str):
        with pytest.raises(ValueError, match=r"app\.Model"):
            canonical_tables({bad}, "writes")

    def test_the_same_tables_again_is_a_no_op(self):
        reg = _reg(_row, writes={"app.Row"})
        again = reg.register(
            "h", "F", _row, 0, match_field="form_type", writes=["app.Row"]
        )
        assert again.writes == frozenset({"app.Row"})

    def test_different_tables_for_the_same_handler_are_refused(self):
        reg = _reg(_row, writes={"app.Row"})
        with pytest.raises(ValueError, match="already registered"):
            reg.register(
                "h", "F", _row, 0, match_field="form_type", writes={"app.Other"}
            )

    def test_register_simple_passes_them_through(self):
        reg = HandlerRegistry()
        register_simple("h", "s", registry=reg, reads="app.Project", writes="app.Row")(
            _row
        )
        (version,) = reg.resolve("s", 0)
        assert version.reads == frozenset({"app.Project"})
        assert version.writes == frozenset({"app.Row"})

    def test_the_decorator_passes_them_through(self):
        reg = HandlerRegistry()
        register_handler(
            "h", "s", 0, registry=reg, reads="app.Project", writes={"app.Row"}
        )(_row)
        (version,) = reg.resolve("s", 0)
        assert version.reads == frozenset({"app.Project"})
        assert version.writes == frozenset({"app.Row"})


class TestNotInTheMetaStream:
    def test_declaring_tables_records_nothing_new(self, store: StreamStore):
        # A store-backed registry that gains declarations after upgrading must
        # append nothing: the record is the same handler as before.
        HandlerRegistry(store=store).register(
            "h", "F", _row, 0, match_field="form_type"
        )
        before = store.read(HANDLERS_META_STREAM)[0]
        HandlerRegistry(store=store).register(
            "h", "F", _row, 0, match_field="form_type", writes={"app.Row"}
        )
        after = store.read(HANDLERS_META_STREAM)[0]
        assert len(after) == len(before) == 1
        payload = json.loads(after[0].data)
        assert "writes" not in payload and "reads" not in payload
