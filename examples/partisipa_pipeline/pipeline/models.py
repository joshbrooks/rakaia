"""The tables the pipeline fills. Every one of them is rebuilt from the log.

Rows point at each other by the keys of the forms that created them, never by
database id. A project has no number of its own here: it is named by the key of
the registration form that created it. A rebuild into another database hands
out different ids, and a link stored as an id would read as a difference when
the two are compared, although nothing is wrong.
"""

from django.db import models


class Project(models.Model):
    """A project, created by its registration form and named by that form's key."""

    registration = models.CharField(max_length=64, unique=True)
    suku = models.CharField(max_length=64)
    output = models.CharField(max_length=32)
    name = models.CharField(max_length=128)
    budget = models.IntegerField()

    class Meta:
        ordering = ["name"]

    def __str__(self) -> str:
        return self.name


class ProgressReport(models.Model):
    """One monthly progress report for one output in one suku.

    ``total`` is not on the form. A later step adds it up from the lines.
    """

    key = models.CharField(max_length=64, unique=True)
    suku = models.CharField(max_length=64)
    output = models.CharField(max_length=32)
    period = models.CharField(max_length=16)
    total = models.IntegerField(default=0)

    class Meta:
        ordering = ["key"]

    def __str__(self) -> str:
        return self.key


class ProgressLine(models.Model):
    """One line of spending on a report: what it was for, and what it cost."""

    report_key = models.CharField(max_length=64)
    line_index = models.IntegerField()
    item = models.CharField(max_length=64)
    cost = models.IntegerField()

    class Meta:
        ordering = ["report_key", "line_index"]
        unique_together = ["report_key", "line_index"]


class ProjectLink(models.Model):
    """Which project a report belongs to, from the assignment a save recorded.

    Names the project's registration form, and says why it was chosen. There is
    no row until a save has assigned the report.
    """

    report_key = models.CharField(max_length=64, unique=True)
    registration = models.CharField(max_length=64)
    because = models.CharField(max_length=128, default="")


class ReportStatus(models.Model):
    """Where a report is in review: submitted, approved or returned."""

    report_key = models.CharField(max_length=64, unique=True)
    status = models.CharField(max_length=16)


class MonthReadiness(models.Model):
    """Whether a suku's month can be closed, and if not, why not."""

    suku = models.CharField(max_length=64)
    period = models.CharField(max_length=16)
    ready = models.BooleanField(default=False)
    reasons = models.CharField(max_length=256, default="")

    class Meta:
        ordering = ["suku", "period"]
        unique_together = ["suku", "period"]
