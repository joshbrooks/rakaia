"""The database-backed `ProgressRows`: where an applied event actually lands.

The write is an ``update_or_create`` keyed on the row's own identity, so the same
event delivered twice leaves the same row — which is what the loop requires of an
apply, because a halted pass and a crash between applying and committing both
re-deliver.

It opens no transaction of its own and must not be called from inside one of the
caller's: the consumer refuses to start there, because the outcome and the
watermark would roll back with it.
"""

from __future__ import annotations

from .consumer import UnknownSuku
from .models import ProgressRow, ReportingPeriod, Suku


class DatabaseRows:
    """Progress rows in `ProgressRow`, gated by the reference tables."""

    def period_is_open(self, period: str) -> bool:
        return not ReportingPeriod.objects.filter(period=period, closed=True).exists()

    def upsert(self, *, suku: str, output: str, period: str, percent: int) -> None:
        if not Suku.objects.filter(name=suku).exists():
            raise UnknownSuku(suku)
        ProgressRow.objects.update_or_create(
            suku=suku,
            output=output,
            period=period,
            defaults={"percent": percent},
        )
