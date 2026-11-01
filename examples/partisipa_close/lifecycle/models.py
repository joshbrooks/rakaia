"""Projections for the close-precondition state-machine spike.

The subproject *lifecycle* is a state machine keyed by `suku`: a cycle can only
be CLOSED (POM_1) once cross-form invariants hold — every project 100 % complete,
two accountability meetings verified, and both cash balances non-negative
(Partisipa's `close_preconditions.py`). These read models are all rebuilt by
replaying one stream:

* `Project`   — progress per output (reference fact, from PROGRESS forms).
* `Meeting`   — accountability meetings and whether each is verified.
* `FinanceLine` — one raw row per FINANCE event (the contributing rows an
  aggregate is recomputed from). Rows open when a cycle closes are `Retire`d —
  soft-deleted, stamped with the close event's own timestamp — so the money
  behind a closed cycle stays readable.
* `Balance`   — a **replay-safe aggregate**: recomputed (not incremented) from
  `FinanceLine` each replay, so re-running never double-counts.
* `CycleClose` — the state-machine output: whether a POM_1 close was ACCEPTED or
  REJECTED, and which preconditions failed. This is the *guarded transition* —
  its value is a pure function of the projected state at close time.
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
    retired_at = models.DateTimeField(null=True, blank=True)
    """When the line was soft-deleted, taken from the close event, never the clock.

    A `Retire` effect patches this instead of deleting the row, so the close's
    audit trail survives. The value is the *triggering event's* timestamp: replay
    the same stream a year later and the column still reads the year it closed,
    which is the difference between a history and a log of when someone happened
    to run the backfill.
    """

    class Meta:
        ordering = ["submission_id"]


class Balance(models.Model):
    """Replay-safe aggregate — recomputed from FinanceLine, never incremented."""

    suku = models.CharField(max_length=64, unique=True)
    operational = models.DecimalField(max_digits=12, decimal_places=2, default=0)
    infrastructure = models.DecimalField(max_digits=12, decimal_places=2, default=0)

    class Meta:
        ordering = ["suku"]


class CycleClose(models.Model):
    """The guarded-transition output: ACCEPTED / REJECTED + the failing reasons."""

    suku = models.CharField(max_length=64, unique=True)
    status = models.CharField(max_length=16, default="")  # ACCEPTED | REJECTED
    reasons = models.JSONField(default=list)

    class Meta:
        ordering = ["suku"]

    def __str__(self) -> str:
        return f"{self.suku}: {self.status} {self.reasons}"
