r"""Sample nested-repeater submissions for the tree-reconcile spike.

One submission (`sub-1`) submitted twice. Each event carries the full tree.

    v1:            A                    v2:          A
                 /   \                              /   \
                B     C                            C     G
               / \     \                            \     \
             D=10 E=20  F=30                        F=30  H=5

v2 **prunes the entire B subtree** (B, D, E) and **adds** G→H. A correct replay
must leave `Node` holding exactly v2's five nodes; the D/E leaves — grandchildren
of the removed B — are the ones a one-level reconcile would orphan, and they are
what makes the `Total` rollup double-count.
"""

V1_TREE = {
    "id": "A", "value": 0, "children": [
        {"id": "B", "value": 0, "children": [
            {"id": "D", "value": 10, "children": []},
            {"id": "E", "value": 20, "children": []},
        ]},
        {"id": "C", "value": 0, "children": [
            {"id": "F", "value": 30, "children": []},
        ]},
    ],
}

V2_TREE = {
    "id": "A", "value": 0, "children": [
        {"id": "C", "value": 0, "children": [
            {"id": "F", "value": 30, "children": []},
        ]},
        {"id": "G", "value": 0, "children": [
            {"id": "H", "value": 5, "children": []},
        ]},
    ],
}

SUBMISSION = "sub-1"

V1_EVENT = {"schema_version": 1, "form_type": "SURVEY", "key": SUBMISSION,
            "tree": V1_TREE}
V2_EVENT = {"schema_version": 1, "form_type": "SURVEY", "key": SUBMISSION,
            "tree": V2_TREE}

# v1: leaves D+E+F = 60 across 6 nodes.
V1_NODES = {"A", "B", "C", "D", "E", "F"}
V1_TOTAL = 60
# v2 (correct): leaves F+H = 35 across 5 nodes.
V2_NODES = {"A", "C", "F", "G", "H"}
V2_TOTAL = 35
# The pruned subtree that must not survive.
PRUNED = {"B", "D", "E"}
# What naive replay wrongly leaves: v1 ∪ v2 nodes, leaves D+E+F+H = 65.
NAIVE_TOTAL = 65
