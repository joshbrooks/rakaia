"""Durable outcomes backed by the ``ConsumerOutcome`` model.

The third `OutcomeStore` after the in-memory reference and the JSONL files, and
the thin persistence layer `django_rakaia.subscription` is for cursors: the
decision about what to record is store-agnostic and lives in `rakaia.outcomes`,
and only the keeping of it is Django-shaped.

What is kept is the **encoded text**, exactly as the other two stores keep it —
ADR 0007 Decision 6b. The payload is the record; the two columns beside it are the
scope index, so `latest` can narrow in SQL rather than decode the table. There are
two because `latest(consumer, stream_path)` is the only query — subject and offset
were columns for one round, written and never read, and the model says why they are
not columns now.

**The index is derived, never the raw value.** Each key column holds
``quote(value, safe="")`` cut to the column width, and that is not a new idea
here — it is `JsonlOutcomeStore._safe_name`, borrowed. That store already had to
turn an arbitrary consumer-supplied string into something a filesystem accepts as
one path segment; a ``varchar`` asks the same question with different edges, and
percent-encoded ASCII answers both at once. It is total (every string has one),
injective (two names never collide), free of NUL and every other byte a text
column can refuse, and identical on every backend.

That matters because the alternative was a list. An earlier version of this
module put raw values in the columns and checked their *length* before writing,
because SQLite keeps an over-long value and Postgres refuses it. The check was
correct and the approach was not: review immediately found NUL — kept by SQLite,
refused by Postgres — and then ``attempt`` overflowing ``int4``, and the next
round would have found something else. Enumerating the properties a column
objects to is the same mistake as enumerating the fields a codec must check, and
`rakaia.outcomes` had already paid five review rounds to learn it. Encoding
removes the enumeration instead of extending it.

**Cutting to the width is safe here, and was not before.** When a column held the
value, truncating meant filing a record under a name that is not its own — two
subjects collapsing into one, which is the defect `Outcome.subject` exists to
fix. When the column is an index, a prefix is still an index: two names sharing
one cut key both come back from the query — `latest` applies no ``LIMIT``, so a
cut key can only over-fetch and never under-fetch — and the payload comparison in
`latest` drops the one that does not belong. So the widths are a prefix length rather than
a capacity, and **`record` cannot refuse an outcome** — no length, no byte, and no
value a consumer can supply makes it raise. That totality is not a nicety: every
other `OutcomeStore` is total, and the consume loop being built alongside this
(#248) records from inside its own error handler, so a store that raised there
would turn one poisoned event into a stopped stream — the opposite of what
skipping a bad event is for. Being the one partial backend would make that
loop grow a guard for this store; staying total is the cheaper half.

**What this store cannot promise on its own.** ADR 0007 keeps the record out of
the executor's transaction so a failure cannot discard the record of itself. One
level further out the same hole reopens, and the core loop cannot see it: a
caller that wraps the whole consume in `transaction.atomic()` and then rolls back
takes the outcome with it, because by default the write joins whatever
transaction is already open. Measured, and stated as a number in
`tests/test_django_rakaia/test_outcome_store_contract.py`.

`using=` is the way out. A store pointed at a database alias the caller's
transaction does not cover commits independently of it, so the record survives
the rollback — the same seam `DjangoStreamStore(using=…)` and
`DjangoExecutor(using=…)` already use. It is opt-in rather than the default
because a store that always opened its own connection would sit outside the test
harness's rollback as well, leaving rows behind between tests, and because every
other Django store here writes on the ambient connection. Making independence the
default is a decision about connection management, not about outcomes, and it is
not settled here.
"""

from __future__ import annotations

from urllib.parse import quote

from rakaia.outcomes import Outcome, _order, decode, encode

from .models import ConsumerOutcome


def _key(column: str, value: str) -> str:
    """The indexed form of `value` for `column`: quoted, then cut to the width.

    The width is read from the model field rather than restated here, so widening
    a column widens the key with it and there is no second declaration to forget.
    Writes and reads both go through this, so a cut key still matches itself.
    """
    limit = ConsumerOutcome._meta.get_field(column).max_length
    assert limit is not None  # every key column is a CharField
    return quote(value, safe="")[:limit]


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
        """Append `outcome`. Total: nothing a caller can put in one makes this raise."""
        # `encode` first and store what it returns: the same text the in-memory
        # store holds in a list and the file-backed one writes as a line. Nothing
        # here renders an outcome its own way, so there is nothing for the three
        # backends to disagree about. The two key columns beside it are derived
        # from the same outcome and are only ever read back through `_key`. Every
        # other field of the outcome is in the payload and nowhere else.
        ConsumerOutcome.objects.using(self._using).create(
            consumer_key=_key("consumer_key", outcome.consumer),
            stream_path_key=_key("stream_path_key", outcome.stream_path),
            payload=encode(outcome),
        )

    def latest(self, consumer: str, stream_path: str) -> list[Outcome]:
        # The key columns narrow; the payload decides. A cut key can admit a row
        # belonging to a name that merely shares a prefix, so the comparison below
        # is not a belt-and-braces check on the query — it is what makes the query
        # safe to cut in the first place.
        rows = (
            ConsumerOutcome.objects.using(self._using)
            .filter(
                consumer_key=_key("consumer_key", consumer),
                stream_path_key=_key("stream_path_key", stream_path),
            )
            .order_by("pk")
        )
        best: dict[str, Outcome] = {}
        for row in rows:
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
