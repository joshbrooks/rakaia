"""Reference data and the one projection the consuming loop maintains.

`Suku` and `ReportingPeriod` are reference facts that arrive by other means —
an administrative import, an operator closing a month. They are not derived from
the stream, and that is the point: whether an event can be applied depends on
state the event does not carry, which is why an apply can fail today and succeed
tomorrow with no change to the event or the code.

`ProgressRow` is the derived table. One row per suku, output and reporting
period, written only by the consuming loop.
"""

from django.db import models


class Suku(models.Model):
    """A village, as the administrative reference data has it."""

    name = models.CharField(max_length=64, unique=True)

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name


class ReportingPeriod(models.Model):
    """A reporting month, and whether it still accepts rows."""

    period = models.CharField(max_length=16, unique=True)
    closed = models.BooleanField(default=False)

    class Meta:
        ordering = ["period"]

    def __str__(self) -> str:
        return f"{self.period}{'closed' if self.closed else 'open'}"


class ProgressRow(models.Model):
    """One repeating row of a progress form, projected."""

    suku = models.CharField(max_length=64)
    output = models.CharField(max_length=32)
    period = models.CharField(max_length=16)
    percent = models.IntegerField(default=0)

    class Meta:
        ordering = ["suku", "output", "period"]
        unique_together = ["suku", "output", "period"]

    def __str__(self) -> str:
        return f"{self.suku}/{self.output}/{self.period} {self.percent}%"
