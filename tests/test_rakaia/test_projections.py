"""Tests for rakaia.projections: the reconcile_* fan-out helpers."""

from __future__ import annotations

import json

from rakaia.projections import (
    project_latest,
    reconcile_aggregate,
    reconcile_children,
    reconcile_tree,
)
from rakaia.types import StreamMessage


def _msg(payload: dict, *, label: str = "update", offset: str = "1") -> StreamMessage:
    return StreamMessage(
        data=json.dumps(payload).encode("utf-8"),
        offset=offset,
        timestamp=1.0,
        label=label,
    )


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


def _tree(nodes: list[dict]):
    return reconcile_tree(
        model_label="app.Node",
        scope_lookup={"submission_id": "s1"},
        node_key="node_id",
        nodes=nodes,
        id_fn=lambda n: n["id"],
        defaults_fn=lambda n: {"parent_node_id": n["parent"], "value": n["value"]},
    )


class TestReconcileTree:
    def test_emits_upsert_per_node_keyed_by_id(self):
        effects = _tree(
            [
                {"id": "A", "parent": "", "value": 0},
                {"id": "B", "parent": "A", "value": 10},
            ]
        )
        upserts = [e for e in effects if e.op == "update_or_create"]
        assert len(upserts) == 2
        assert all(e.model_label == "app.Node" for e in upserts)
        # keyed by the node's explicit id, not a positional index
        assert upserts[0].lookup == {"submission_id": "s1", "node_id": "A"}
        assert upserts[0].defaults == {"parent_node_id": "", "value": 0}
        assert upserts[1].lookup == {"submission_id": "s1", "node_id": "B"}

    def test_reconcile_delete_excludes_all_kept_ids(self):
        effects = _tree(
            [
                {"id": "A", "parent": "", "value": 0},
                {"id": "B", "parent": "A", "value": 10},
                {"id": "C", "parent": "A", "value": 20},
            ]
        )
        deletes = [e for e in effects if e.op == "delete"]
        assert len(deletes) == 1
        # one submission-scoped delete over the whole subtree, any depth
        assert deletes[0].lookup == {"submission_id": "s1"}
        assert deletes[0].exclude == {"node_id__in": ["A", "B", "C"]}

    def test_deep_nodes_keyed_the_same_way_regardless_of_depth(self):
        # A grandchild is keyed and reconciled identically to a top-level node —
        # that is what lets one delete prune orphans at any depth.
        effects = _tree(
            [
                {"id": "A", "parent": "", "value": 0},
                {"id": "B", "parent": "A", "value": 0},
                {"id": "D", "parent": "B", "value": 10},  # grandchild
            ]
        )
        upserts = [e for e in effects if e.op == "update_or_create"]
        assert upserts[2].lookup == {"submission_id": "s1", "node_id": "D"}
        deletes = [e for e in effects if e.op == "delete"]
        assert deletes[0].exclude == {"node_id__in": ["A", "B", "D"]}

    def test_empty_nodes_deletes_whole_subtree(self):
        effects = _tree([])
        assert [e for e in effects if e.op == "update_or_create"] == []
        deletes = [e for e in effects if e.op == "delete"]
        assert len(deletes) == 1
        assert deletes[0].lookup == {"submission_id": "s1"}
        assert deletes[0].exclude == {"node_id__in": []}

    def test_upserts_precede_delete(self):
        effects = _tree([{"id": "A", "parent": "", "value": 0}])
        assert [e.op for e in effects] == ["update_or_create", "delete"]

    def test_scope_lookup_not_mutated(self):
        scope = {"submission_id": "s1"}
        reconcile_tree(
            model_label="app.Node",
            scope_lookup=scope,
            node_key="node_id",
            nodes=[{"id": "A", "parent": "", "value": 0}],
            id_fn=lambda n: n["id"],
            defaults_fn=lambda n: {"value": n["value"]},
        )
        assert scope == {"submission_id": "s1"}


def _agg(groups, scope=None):
    return reconcile_aggregate(
        model_label="app.Balance",
        scope_lookup=scope if scope is not None else {},
        group_key="suku",
        groups=groups,
    )


class TestReconcileAggregate:
    def test_emits_upsert_per_group(self):
        effects = _agg({"Fatuberliu": {"total": 300}, "Maubara": {"total": -50}})
        upserts = [e for e in effects if e.op == "update_or_create"]
        assert len(upserts) == 2
        assert upserts[0].lookup == {"suku": "Fatuberliu"}
        assert upserts[0].defaults == {"total": 300}
        assert upserts[1].lookup == {"suku": "Maubara"}
        assert upserts[1].defaults == {"total": -50}

    def test_reconcile_delete_excludes_present_groups(self):
        effects = _agg({"Fatuberliu": {"total": 300}, "Maubara": {"total": -50}})
        deletes = [e for e in effects if e.op == "delete"]
        assert len(deletes) == 1
        assert deletes[0].lookup == {}
        # groups that vanished (no contributors) get their aggregate removed
        assert deletes[0].exclude == {"suku__in": ["Fatuberliu", "Maubara"]}

    def test_empty_groups_deletes_all(self):
        effects = _agg({})
        assert [e for e in effects if e.op == "update_or_create"] == []
        deletes = [e for e in effects if e.op == "delete"]
        assert deletes[0].exclude == {"suku__in": []}

    def test_scope_included_in_lookup_and_delete(self):
        effects = _agg({"north": {"total": 5}}, scope={"report_id": 42})
        upserts = [e for e in effects if e.op == "update_or_create"]
        assert upserts[0].lookup == {"report_id": 42, "suku": "north"}
        deletes = [e for e in effects if e.op == "delete"]
        assert deletes[0].lookup == {"report_id": 42}
        assert deletes[0].exclude == {"suku__in": ["north"]}

    def test_upserts_precede_delete(self):
        effects = _agg({"a": {"total": 1}})
        assert [e.op for e in effects] == ["update_or_create", "delete"]

    def test_scope_lookup_not_mutated(self):
        scope = {"report_id": 42}
        _agg({"a": {"total": 1}}, scope=scope)
        assert scope == {"report_id": 42}


class TestProjectLatest:
    def _project(self, messages):
        return project_latest(
            messages,
            "app.Submission",
            subject_field="key",
            subject_of=lambda e: e["key"],
            defaults_of=lambda _msg, e: {"fields": e["fields"], "status": e["status"]},
        )

    def test_one_upsert_per_subject(self):
        effects = self._project(
            [
                _msg({"key": "a", "fields": {"v": 1}, "status": 0}, label="create"),
                _msg({"key": "b", "fields": {"v": 1}, "status": 0}, label="create"),
            ]
        )
        upserts = [e for e in effects if e.op == "update_or_create"]
        assert len(upserts) == 2
        assert {tuple(e.lookup.items()) for e in upserts} == {
            (("key", "a"),),
            (("key", "b"),),
        }
        assert all(e.model_label == "app.Submission" for e in upserts)

    def test_last_event_wins(self):
        effects = self._project(
            [
                _msg({"key": "a", "fields": {"v": 1}, "status": 0}, label="create"),
                _msg({"key": "a", "fields": {"v": 2}, "status": 1}, label="update"),
            ]
        )
        assert len(effects) == 1
        assert effects[0].op == "update_or_create"
        assert effects[0].lookup == {"key": "a"}
        assert effects[0].defaults == {"fields": {"v": 2}, "status": 1}

    def test_tombstone_emits_delete(self):
        effects = self._project(
            [
                _msg({"key": "a", "fields": {"v": 1}, "status": 0}, label="create"),
                _msg({"key": "a", "fields": {"v": 1}, "status": 0}, label="delete"),
            ]
        )
        assert len(effects) == 1
        assert effects[0].op == "delete"
        assert effects[0].lookup == {"key": "a"}

    def test_recreate_after_tombstone_upserts(self):
        # create -> delete -> create: the latest event is live, so the row
        # reappears with the newest snapshot.
        effects = self._project(
            [
                _msg({"key": "a", "fields": {"v": 1}, "status": 0}, label="create"),
                _msg({"key": "a", "fields": {"v": 1}, "status": 0}, label="delete"),
                _msg({"key": "a", "fields": {"v": 3}, "status": 2}, label="create"),
            ]
        )
        assert len(effects) == 1
        assert effects[0].op == "update_or_create"
        assert effects[0].defaults == {"fields": {"v": 3}, "status": 2}

    def test_custom_tombstone_labels(self):
        effects = project_latest(
            [_msg({"key": "a", "fields": {}, "status": 0}, label="archived")],
            "app.Submission",
            subject_field="key",
            subject_of=lambda e: e["key"],
            defaults_of=lambda _msg, e: {"status": e["status"]},
            tombstone_labels=("archived",),
        )
        assert len(effects) == 1
        assert effects[0].op == "delete"

    def test_only_subjects_present_get_effects(self):
        # Incremental: a tail read with only 'a' touches only 'a' — an absent
        # subject is not deleted (only an explicit tombstone is).
        effects = self._project(
            [_msg({"key": "a", "fields": {}, "status": 0}, label="update")]
        )
        assert len(effects) == 1
        assert effects[0].lookup == {"key": "a"}
        assert effects[0].op == "update_or_create"
