"""Tests for rakaia.projections: the reconcile_children fan-out helper."""

from __future__ import annotations

from rakaia.projections import reconcile_children


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
