"""The durable half of `rakaia.consumer.Consumer`, and the guard core cannot hold.

`django_consumer` fills in the two places a consumer keeps things — the
``ConsumerCursor`` row for the watermark, the ``ConsumerOutcome`` rows for what
an apply could not do — so a caller names the stream and the consumer once and
gets a consumer that survives a restart and records by construction.

**It refuses to start inside a transaction the caller opened.** ADR 0007 keeps
the outcome out of the executor's transaction and then says plainly what it
cannot reach: one frame further out, a caller that wraps the run in
``atomic()`` and rolls back takes the record with it, measured at 0 of 1 outcomes
surviving. `consume` cannot see that — `rakaia` is stdlib-only and a core module
that could see an atomic block would be the tier violation
`tests/test_rakaia/test_tier_boundary.py` exists to refuse. This module can see
it, so it checks, and the hazard stops being prose in three docstrings.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass

from django.db import DEFAULT_DB_ALIAS, connections

from rakaia.consumer import Consumer
from rakaia.outcomes import Outcome
from rakaia.protocols import CursorStore
from rakaia.subscription import Consumed, OnErrorPolicy
from rakaia.types import StreamMessage

from .outcomes import DjangoOutcomeStore
from .subscription import commit_cursor, load_cursor


class CallerTransactionOpen(RuntimeError):
    """Raised when a run is started inside a transaction the caller opened.

    Everything the run records — the outcome for an event that could not be
    applied, and the watermark that says it was dealt with — would roll back with
    that transaction, leaving a stream that reads as clean. Run the consumer
    outside your own ``atomic()`` block; if the work `apply` does needs a
    transaction, open it inside `apply`, where it covers the effect and not the
    record of the effect failing.
    """


class DjangoCursorLedger:
    """A `CursorLedger` over the ``ConsumerCursor`` table.

    Pass ``using`` to read and write on a named database alias, exactly as
    `DjangoStreamStore` and `DjangoOutcomeStore` do.
    """

    def __init__(self, *, using: str | None = None) -> None:
        self._using = using

    def load(self, consumer: str, stream_path: str) -> str | None:
        return load_cursor(consumer, stream_path, using=self._using)

    def commit(self, consumer: str, stream_path: str, offset: str) -> None:
        commit_cursor(consumer, stream_path, offset, using=self._using)


@dataclass(frozen=True)
class DjangoConsumer(Consumer):
    """A `Consumer` that checks the caller has no transaction open.

    The check is on `run` rather than on construction because that is where the
    hazard is: a consumer built during startup and run later from a request
    handler is fine, and one built outside a transaction and run inside one is
    the case being refused.
    """

    alias: str = DEFAULT_DB_ALIAS
    """The alias whose connection the guard inspects — the one the cursor and
    outcome rows are written on, since it is that connection's transaction their
    survival depends on."""

    def run(
        self,
        apply: Callable[[StreamMessage], Iterable[Outcome] | None],
        *,
        on_error: OnErrorPolicy,
    ) -> Consumed:
        if connections[self.alias].in_atomic_block:
            raise CallerTransactionOpen(
                f"consumer {self.name!r} was started inside an open transaction "
                f"on {self.alias!r}. Everything it records would roll back with "
                "that transaction — see ADR 0007."
            )
        return super().run(apply, on_error=on_error)


def django_consumer(
    store: CursorStore,
    path: str,
    name: str,
    *,
    using: str | None = None,
) -> DjangoConsumer:
    """A consumer of `path`, named `name`, keeping both its cursor and its
    outcomes in the database.

    Args:
        store: where the events are — `DjangoStreamStore`, or any store that
            reports its head offset.
        path: the stream to consume.
        name: who the cursor and the outcomes belong to. Said once.
        using: the database alias to keep them on. The default alias joins any
            transaction the caller has open, which is exactly what `run` refuses
            to start inside; a separate alias commits independently, and is the
            way out described in `django_rakaia.outcomes`.
    """
    return DjangoConsumer(
        store=store,
        path=path,
        name=name,
        cursors=DjangoCursorLedger(using=using),
        outcomes=DjangoOutcomeStore(using=using),
        alias=using or DEFAULT_DB_ALIAS,
    )
