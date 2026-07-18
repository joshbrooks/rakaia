"""Projections for the staged-replay spike.

Both are *derived* read models rebuilt by replaying the `submissions` stream —
the rakaia analogue of Partisipa's `ida.Project` and an `ida_forms.*` typed form
row. `Project` is a **reference** entity (created by TF_6_1_1); `Sf12` is a
**dependent** projection (SF_1_2) that must link to the Project for its
`(suku, output)` — a Project that frequently does not exist yet when the SF form
is first processed.
"""

from django.db import models


class Project(models.Model):
    """Reference entity — one per (suku, output). Built in replay stage 0."""

    suku = models.CharField(max_length=64)
    output = models.CharField(max_length=64)
    name = models.CharField(max_length=128, default="")

    class Meta:
        ordering = ["suku", "output"]
        unique_together = ["suku", "output"]

    def __str__(self) -> str:
        return f"{self.name} ({self.suku}/{self.output})"


class Sf12(models.Model):
    """Dependent projection — links to a Project. Built in replay stage 1.

    `link_reason` mirrors Partisipa's `SeparatedSubmissionProject.link_reason`:
    ``NM`` = matched a project, ``NPO`` = no project/output match (unlinked).
    """

    submission_id = models.CharField(max_length=64, unique=True)
    suku = models.CharField(max_length=64, default="")
    output = models.CharField(max_length=64, default="")
    cost = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    project = models.ForeignKey(
        Project,
        on_delete=models.SET_NULL,
        null=True,
        blank=True,
        related_name="sf12_forms",
    )
    link_reason = models.CharField(max_length=8, default="")

    class Meta:
        ordering = ["submission_id"]

    def __str__(self) -> str:
        return f"{self.submission_id} -> {self.project_id or 'UNLINKED'}"
