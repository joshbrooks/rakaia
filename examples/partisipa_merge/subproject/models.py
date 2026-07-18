"""Projections for the multi-stream merge spike.

The subproject readiness view is derived from facts that, in real Partisipa, live
in *separate typed form pipelines* (SF / TF / FF) rather than one stream. These
read models are rebuilt by **merging** those streams into one deterministic order
and replaying it:

* `Project`    — progress per output (from the PROGRESS/TF pipeline).
* `Meeting`    — accountability meetings (from the MEETING/SF pipeline).
* `FinanceLine`— one raw row per finance event (from the FINANCE/FF pipeline).
* `Balance`    — replay-safe aggregate recomputed from `FinanceLine`.
* `Readiness`  — the derived subproject verdict (ready to close + failing reasons),
  the cross-form rollup Partisipa currently computes in SQL / Metabase.
"""

from django.db import models


class Project(models.Model):
    suku = models.CharField(max_length=64)
    output = models.CharField(max_length=64)
    percent = models.IntegerField(default=0)

    class Meta:
        ordering = ["suku", "output"]
        unique_together = ["suku", "output"]


class Meeting(models.Model):
    suku = models.CharField(max_length=64)
    meeting_id = models.CharField(max_length=64)
    verified = models.BooleanField(default=False)

    class Meta:
        ordering = ["suku", "meeting_id"]
        unique_together = ["suku", "meeting_id"]


class FinanceLine(models.Model):
    submission_id = models.CharField(max_length=64, unique=True)
    suku = models.CharField(max_length=64)
    account = models.CharField(max_length=16)  # operational | infrastructure
    delta = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    class Meta:
        ordering = ["submission_id"]


class Balance(models.Model):
    suku = models.CharField(max_length=64, unique=True)
    operational = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    infrastructure = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    class Meta:
        ordering = ["suku"]


class Readiness(models.Model):
    """The cross-stream subproject verdict — ready to close + failing reasons."""

    suku = models.CharField(max_length=64, unique=True)
    ready = models.BooleanField(default=False)
    reasons = models.JSONField(default=list)

    class Meta:
        ordering = ["suku"]

    def __str__(self) -> str:
        return f"{self.suku}: {'READY' if self.ready else 'NOT-READY'} {self.reasons}"
