"""Tests for rakaia.projections: the reconcile_children / reconcile_by_key helpers."""

from __future__ import annotations

import pytest

from rakaia.projections import reconcile_by_key, reconcile_children


def _reconcile(items: list[dict]):
    return reconcile_children(
        model_label="app.Child",
        parent_lookup={"parent_id": 7},
        child_key="idx",
        items=items,
        defaults_fn=lambda item: {"name": item["name"]},
    )


class TestReconcileChildren:
    def test_emits_upsert_per_item(self):
        effects = _reconcile([{"name": "a"}, {"name": "b"}])
        upserts = [e for e in effects if e.op == "update_or_create"]
        assert len(upserts) == 2
        assert all(e.model_label == "app.Child" for e in upserts)
        assert upserts[0].lookup == {"parent_id": 7, "idx": 0}
        assert upserts[0].defaults == {"name": "a"}
        assert upserts[1].lookup == {"parent_id": 7, "idx": 1}
        assert upserts[1].defaults == {"name": "b"}

    def test_emits_reconcile_delete(self):
        effects = _reconcile([{"name": "a"}, {"name": "b"}])
        deletes = [e for e in effects if e.op == "delete"]
        assert len(deletes) == 1
        assert deletes[0].model_label == "app.Child"
        assert deletes[0].lookup == {"parent_id": 7}
        assert deletes[0].exclude == {"idx__in": [0, 1]}

    def test_empty_items_deletes_all(self):
        effects = _reconcile([])
        assert [e for e in effects if e.op == "update_or_create"] == []
        deletes = [e for e in effects if e.op == "delete"]
        assert len(deletes) == 1
        assert deletes[0].lookup == {"parent_id": 7}
        # exclude on an empty keep-set spares nothing -> deletes every child.
        assert deletes[0].exclude == {"idx__in": []}

    def test_upserts_precede_delete(self):
        # The delete is emitted last so a CaptureExecutor sees the natural
        # "write current, then prune" order (the DjangoExecutor reorders anyway).
        effects = _reconcile([{"name": "a"}])
        assert [e.op for e in effects] == ["update_or_create", "delete"]

    def test_parent_lookup_not_mutated(self):
        parent = {"parent_id": 7}
        reconcile_children(
            model_label="app.Child",
            parent_lookup=parent,
            child_key="idx",
            items=[{"name": "a"}],
            defaults_fn=lambda item: {"name": item["name"]},
        )
        assert parent == {"parent_id": 7}


def _by_key(items, *, retire="delete", retire_filter=None):
    return reconcile_by_key(
        model_label="app.Alert",
        scope={"stream_key": "sub-1"},
        key_fields=("alert_type", "field_key"),
        items=items,
        key_fn=lambda v: {
            "alert_type": v["alert_type"],
            "field_key": v.get("field_key", ""),
        },
        defaults_fn=lambda v: {"severity": v.get("severity", "info")},
        retire_filter=retire_filter,
        retire=retire,
    )


class TestReconcileByKey:
    def test_upsert_merges_scope_and_composite_key(self):
        effects = _by_key(
            [{"alert_type": "ff4", "field_key": "row0", "severity": "error"}]
        )
        upserts = [e for e in effects if e.op == "update_or_create"]
        assert len(upserts) == 1
        assert upserts[0].model_label == "app.Alert"
        assert upserts[0].lookup == {
            "stream_key": "sub-1",
            "alert_type": "ff4",
            "field_key": "row0",
        }
        assert upserts[0].defaults == {"severity": "error"}

    def test_delete_retire_spares_composite_keys(self):
        effects = _by_key(
            [{"alert_type": "ff4", "field_key": "a"}, {"alert_type": "sf11"}]
        )
        retires = [e for e in effects if e.op == "delete"]
        assert len(retires) == 1
        assert retires[0].lookup == {"stream_key": "sub-1"}
        assert retires[0].spare_keys == [
            {"alert_type": "ff4", "field_key": "a"},
            {"alert_type": "sf11", "field_key": ""},
        ]

    def test_soft_delete_retire_emits_patch_and_open_guard(self):
        effects = _by_key(
            [{"alert_type": "ff4", "field_key": "a"}],
            retire={"resolved_at": "t1", "resolved_by": "system"},
            retire_filter={"alert_type__in": ["ff4", "sf11"]},
        )
        retires = [e for e in effects if e.op == "retire"]
        assert len(retires) == 1
        r = retires[0]
        # G1: retire is scoped by retire_filter distinct from the upsert key.
        # Open-guard: only rows whose patch fields are currently NULL are retired.
        assert r.lookup == {
            "stream_key": "sub-1",
            "alert_type__in": ["ff4", "sf11"],
            "resolved_at__isnull": True,
            "resolved_by__isnull": True,
        }
        assert r.patch == {"resolved_at": "t1", "resolved_by": "system"}
        assert r.spare_keys == [{"alert_type": "ff4", "field_key": "a"}]

    def test_empty_items_retire_spares_nothing(self):
        effects = _by_key([], retire={"resolved_at": "t1"})
        assert [e for e in effects if e.op == "update_or_create"] == []
        retires = [e for e in effects if e.op == "retire"]
        assert len(retires) == 1
        assert retires[0].spare_keys == []

    def test_upserts_precede_retire(self):
        effects = _by_key([{"alert_type": "ff4"}])
        assert [e.op for e in effects] == ["update_or_create", "delete"]

    def test_scope_not_mutated(self):
        scope = {"stream_key": "sub-1"}
        reconcile_by_key(
            model_label="app.Alert",
            scope=scope,
            key_fields=("alert_type",),
            items=[{"alert_type": "ff4"}],
            key_fn=lambda v: {"alert_type": v["alert_type"]},
            defaults_fn=lambda v: {},  # noqa: ARG005
        )
        assert scope == {"stream_key": "sub-1"}

    def test_key_fn_output_must_match_key_fields(self):
        with pytest.raises(ValueError, match="key_fields"):
            reconcile_by_key(
                model_label="app.Alert",
                scope={"stream_key": "sub-1"},
                key_fields=("alert_type", "field_key"),
                items=[{"alert_type": "ff4"}],
                key_fn=lambda v: {"alert_type": v["alert_type"]},  # missing field_key
                defaults_fn=lambda v: {},  # noqa: ARG005
            )
