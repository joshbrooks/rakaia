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

**The connections it checks are the ones its stores name, not a fourth place to
say the same thing.** An earlier version carried the alias as its own field,
defaulted to ``default``, and only the factory kept it in step with the two
stores. Building the consumer directly with both stores on another alias then
left the guard inspecting a connection the rows were not on — it passed exactly
when it should have refused, which is worse than not checking, because a caller
reads the pass as permission. The alias is now asked of each store, so there is
nothing to keep in step, and a cursor and an outcome kept on two different
aliases are both covered.
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

    ``ATOMIC_REQUESTS = True`` is the usual way to meet this without having
    written an ``atomic()`` at all: it wraps every view in one, so a consumer run
    from a request handler raises here. That is the right answer rather than an
    inconvenience — the records really would roll back with the response — and
    the fix is to run the consumer outside the request, or on a database alias
    that setting does not cover.
    """


class DjangoConsumerCursorStore:
    """A `ConsumerCursorStore` over the ``ConsumerCursor`` table.

    Pass ``using`` to read and write on a named database alias, exactly as
    `DjangoStreamStore` and `DjangoOutcomeStore` do. The alias is a public
    attribute because the guard in `DjangoConsumer` asks each store which
    connection its rows live on rather than being told a second time.
    """

    def __init__(self, *, using: str | None = None) -> None:
        self.using = using

    def load(self, consumer: str, stream_path: str) -> str | None:
        return load_cursor(consumer, stream_path, using=self.using)

    def commit(self, consumer: str, stream_path: str, offset: str) -> None:
        commit_cursor(consumer, stream_path, offset, using=self.using)


_UNSET = object()


def _alias_of(target: object) -> str | None:
    """The alias `target` writes on, or ``None`` when it does not name one.

    A store that has no ``using`` is not database-backed, so no transaction of
    the caller's can swallow what it records and there is nothing to guard. That
    is the difference between "writes on the default alias" and "does not write
    to a database at all", and collapsing the two would refuse a perfectly safe
    run over an in-memory store.
    """
    using = getattr(target, "using", _UNSET)
    if using is _UNSET:
        return None
    return DEFAULT_DB_ALIAS if using is None else str(using)


@dataclass(frozen=True)
class DjangoConsumer(Consumer):
    """A `Consumer` that checks the caller has no transaction open.

    The check is on `run` rather than on construction because that is where the
    hazard is: a consumer built during startup and run later from a request
    handler is fine, and one built outside a transaction and run inside one is
    the case being refused.
    """

    def run(
        self,
        apply: Callable[[StreamMessage], Iterable[Outcome] | None],
        *,
        on_error: OnErrorPolicy,
    ) -> Consumed:
        for alias in self._aliases():
            if connections[alias].in_atomic_block:
                raise CallerTransactionOpen(
                    f"consumer {self.name!r} was started inside an open transaction "
                    f"on {alias!r}. Everything it records would roll back with "
                    "that transaction — see ADR 0007."
                )
        return super().run(apply, on_error=on_error)

    def _aliases(self) -> list[str]:
        """Every connection this consumer's records depend on, in a stable order.

        Both stores are asked, and the cursor and the outcomes may name different
        ones: a caller keeping outcomes on a separate alias to escape its own
        transaction still keeps the watermark wherever it said, and a rollback of
        either loses something.
        """
        named = (_alias_of(self.cursors), _alias_of(self.outcomes))
        return sorted({alias for alias in named if alias is not None})


def django_consumer(
    store: CursorStore,
    path: str,
    name: str,
    *,
    using: str | None = None,
    subject_of: Callable[[StreamMessage], str] | None = None,
    sequence_of: Callable[[StreamMessage], str] | None = None,
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
        subject_of: what a record written for a failed apply is *about*, given
            the message. Defaults to the event's position in the log.

            Worth passing whenever the consumer records outcomes of its own,
            because the two end up in one column on one screen: a record the
            consumer writes can name the row, and without this the records the
            loop writes for it name a position instead. Comparing those is the
            reason someone opens that screen.

            Two things follow from it being yours to choose. It is called **only
            when an apply fails**, so an exception from it surfaces on the path
            you least want to be surprised on, and it propagates out of the run
            rather than being recorded. And the subject is part of what makes a
            record distinct: `latest` keeps the newest per subject, so a
            function that gives two different events the same name shows one
            record where two were written. The default could not do either — an
            offset is always readable and always unique.
        sequence_of: what a message is ordered *within*, given the message.
            Defaults to the subject.

            Nothing reads it yet (ADR 0007 Decision 7), which is a weaker reason
            to pass it than it first appears: the field is written on every
            record either way, so leaving this out does not omit the grouping, it
            stores a copy of the subject in its place. The choice is between
            recording something true and recording noise. It is also shown, as
            "Sequence", on a record's detail page.
    """
    return DjangoConsumer(
        store=store,
        path=path,
        name=name,
        cursors=DjangoConsumerCursorStore(using=using),
        outcomes=DjangoOutcomeStore(using=using),
        subject_of=subject_of,
        sequence_of=sequence_of,
    )
