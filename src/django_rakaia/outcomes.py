"""Durable outcomes backed by the ``ConsumerOutcome`` model.

The third `OutcomeStore` after the in-memory reference and the JSONL files, and
the thin persistence layer `django_rakaia.subscription` is for cursors: the
decision about what to record is store-agnostic and lives in `rakaia.outcomes`,
and only the keeping of it is Django-shaped.

What is kept is the **encoded text**, exactly as the other two stores keep it —
ADR 0007 Decision 6b. The columns beside it exist so `latest` can filter and
order in SQL, not so a reader can reconstruct an outcome from them; the payload
is the record, and `decode` is the only way back out of it. That is what stops
three backends developing three opinions about what was stored.

**Lengths are checked here, not left to the database, and that is the one place
this store deliberately does more than pass a value through.** The columns are
bounded — the widths `ConsumerCursor` already uses — and the two databases this
suite runs on disagree about what a bounded column means: Postgres refuses an
over-long value, SQLite keeps it. ADR 0007 predicted exactly that ("a backend
with a bounded column accepts a name every other backend keeps and then refuses
or truncates it"), and a store whose behaviour depends on which database is under
it is the same defect the shared codec exists to prevent, one level down. So the
check runs in Python, before the insert, against the model's own declared widths
— read from the field, so the rule cannot drift from the column — and both
databases then refuse the same value with the same error.

Refusing rather than truncating, because a truncated subject is a *different*
subject: it would collapse two rows into one and report the first as speaking for
the rest, which is the defect the `subject` field was introduced to fix.

**What this store cannot promise on its own.** ADR 0007 keeps the record out of
the executor's transaction so a failure cannot discard the record of itself. One
level further out the same hole reopens, and this store cannot see it: a caller
that wraps the whole consume in `transaction.atomic()` and then rolls back takes
the outcome with it, because by default the write joins whatever transaction is
already open. Measured, and stated as a number in
`tests/test_django_rakaia/test_outcome_store_contract.py`.

`using=` is the way out that exists today. A store pointed at a database alias
the caller's transaction does not cover commits independently of it, so the
record survives the rollback — the same seam `DjangoStreamStore(using=…)` and
`DjangoExecutor(using=…)` already use. It is opt-in rather than the default
because a store that always opened its own connection would sit outside the test
harness's rollback as well, leaving rows behind between tests, and because every
other Django store here writes on the ambient connection. Making independence the
default is a decision about connection management, not about outcomes, and it is
not settled here.
"""

from __future__ import annotations

from rakaia.outcomes import Outcome, _order, decode, encode

from .models import ConsumerOutcome

#: The bounded columns, in the order a caller most likely got one wrong.
_BOUNDED = ("consumer", "stream_path", "subject", "offset")


def _check_widths(outcome: Outcome) -> None:
    """Refuse anything a bounded column cannot hold, identically on any database.

    Widths come from the model rather than from constants here, so widening a
    column widens the check with it and there is no second declaration to forget.
    """
    too_long = []
    for name in _BOUNDED:
        value = getattr(outcome, name)
        if value is None:
            continue
        limit = ConsumerOutcome._meta.get_field(name).max_length
        assert limit is not None  # every name in `_BOUNDED` is a CharField
        if len(value) > limit:
            too_long.append(
                f"{name} is {len(value)} characters, the column holds {limit}"
            )
    if too_long:
        raise ValueError(
            "This store keeps the names in bounded columns and one of them does not "
            f"fit: {'; '.join(too_long)}. Shorten it — truncating here would file the "
            "outcome under a subject that is not the one it is about."
        )


class DjangoOutcomeStore:
    """Outcomes kept as rows in ``rakaia_consumeroutcome``.

    Pass ``using`` to write and read on a named database alias instead of the
    default one, exactly as `DjangoStreamStore` does. ``using=None`` is the
    ambient connection, which means the write joins any transaction the caller
    already has open — see the module docstring for what that costs and when to
    reach for an alias instead.
    """

    def __init__(self, *, using: str | None = None) -> None:
        self._using = using

    def record(self, outcome: Outcome) -> None:
        _check_widths(outcome)
        # `encode` first and store what it returns: the same text the in-memory
        # store holds in a list and the file-backed one writes as a line. Nothing
        # here renders an outcome its own way, so there is nothing for the three
        # backends to disagree about.
        ConsumerOutcome.objects.using(self._using).create(
            consumer=outcome.consumer,
            stream_path=outcome.stream_path,
            subject=outcome.subject,
            offset=outcome.offset,
            attempt=outcome.attempt,
            payload=encode(outcome),
        )

    def latest(self, consumer: str, stream_path: str) -> list[Outcome]:
        best: dict[str, Outcome] = {}
        rows = (
            ConsumerOutcome.objects.using(self._using)
            .filter(consumer=consumer, stream_path=stream_path)
            .order_by("pk")
        )
        for row in rows:
            # The columns say which scope this is and so does the payload; the
            # payload wins, for the reason the file-backed store gives — the
            # record is what was stored, the columns are an index over it, and a
            # row hand-edited or written by something else must not be reported
            # to a consumer it does not belong to.
            record = decode(row.payload)
            if (
                record is None
                or record.consumer != consumer
                or record.stream_path != stream_path
            ):
                continue
            held = best.get(record.subject)
            # `>=`, and rows in primary-key order, so re-recording an attempt
            # replaces it: last write wins, as it does in the other two stores.
            if held is None or record.attempt >= held.attempt:
                best[record.subject] = record
        return sorted(best.values(), key=_order)
