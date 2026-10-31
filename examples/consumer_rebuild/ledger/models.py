"""The one projection the consumer maintains.

Each row carries the *absolute* value an event names rather than a delta, which
is what makes applying the same event twice land on the same row with the same
value. The demo relies on that: a pass that dies halfway is delivered again from
the unchanged cursor, and the projection has to converge rather than double.
"""

from django.db import models


class LedgerRow(models.Model):
    """One keyed total, written only by the consuming loop."""

    key = models.CharField(max_length=64, unique=True)
    value = models.IntegerField(default=0)

    class Meta:
        ordering = ["key"]

    def __str__(self) -> str:
        return f"{self.key}={self.value}"
