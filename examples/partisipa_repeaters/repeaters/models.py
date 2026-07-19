"""Projections for the tree-reconcile spike.

FormKit-Ninja's `SeparatedSubmission` has a self-FK ``repeater_parent`` — a
submission's repeaters nest to **unbounded** depth. When a submission is
re-processed with a restructured tree (a pruned subtree, moved nodes), stale
nested rows from the previous version are orphaned, and rollups double-count them
(the recurring bug behind `reconcile_separated_submissions` / issue #2252).

* `Node`  — one row per node in a submission's repeater tree, at any depth, with a
  self-referential ``parent_node_id``. Rebuilt by replaying the submission stream.
* `Total` — a rollup (sum of leaf values per submission), the thing that
  double-counts when orphaned nodes survive.
"""

from django.db import models


class Node(models.Model):
    submission_id = models.CharField(max_length=64)
    node_id = models.CharField(max_length=64)
    parent_node_id = models.CharField(max_length=64, default="")  # "" = root
    depth = models.IntegerField(default=0)
    value = models.IntegerField(default=0)
    is_leaf = models.BooleanField(default=False)

    class Meta:
        ordering = ["submission_id", "node_id"]
        unique_together = ["submission_id", "node_id"]

    def __str__(self) -> str:
        return f"{self.submission_id}:{self.node_id}@{self.depth}"


class Total(models.Model):
    """Rollup — sum of leaf values per submission. Wrong if orphans survive."""

    submission_id = models.CharField(max_length=64, unique=True)
    total = models.IntegerField(default=0)

    class Meta:
        ordering = ["submission_id"]

    def __str__(self) -> str:
        return f"{self.submission_id}: {self.total}"
