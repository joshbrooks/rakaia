"""A consumer you hold, rather than five arguments you repeat.

`consume` is the loop; this is the thing that runs it. It holds the store, the
stream path, the consumer's name, somewhere to keep the cursor and somewhere to
keep the outcomes, and hands them to the loop on every pass. The wiring in
`docs/subscriber-cursors.md` repeats the name and the path three times each, and
a name that is right in two of the three places is a consumer whose outcomes are
filed under one identity and whose watermark is kept under another.

**The outcome store is a required argument, and that is the point of this
module.** `consume(outcomes=None)` runs, applies, advances the cursor and records
nothing — which under ADR 0007 Decision 3 does not read as "nothing was
recorded", it reads as "every event succeeded". A consumer wired that way looks
correct in review, passes its own tests and reports a clean stream it never
checked. There is no equivalent shape here: a `Consumer` without somewhere to put
an outcome cannot be constructed.

`on_error` keeps having no default (Decision 5). The two modes have opposite
invariants — a live consumer skips a poisoned event, a rebuild must not — and a
module that guessed for the caller would be right for one of them and quietly
wrong for the other, which is the same failure as recording nothing.
"""

from __future__ import annotations

from collections.abc import Callable, Iterable
from dataclasses import dataclass
from typing import Protocol, runtime_checkable

from .outcomes import Outcome
from .protocols import CursorStore, OutcomeStore
from .subscription import Consumed, OnErrorPolicy, consume
from .types import StreamMessage


@runtime_checkable
class CursorLedger(Protocol):
    """Somewhere to keep a consumer's watermark between runs.

    The seam `consume`'s ``cursor`` and ``commit`` arguments describe together:
    one loads, the other stores, and both are keyed by the same
    ``(consumer, stream_path)`` pair. Passing them as two unrelated callables is
    what let a consumer read one key and write another.
    """

    def load(self, consumer: str, stream_path: str) -> str | None:
        """The last committed offset for this consumer and stream, or ``None``."""
        ...

    def commit(self, consumer: str, stream_path: str, offset: str) -> None:
        """Persist `offset` as the watermark. Called only after the message it
        belongs to has been applied and any outcome recorded."""
        ...


class InMemoryCursorLedger:
    """Watermarks in a dict: the reference `CursorLedger`, for tests and demos.

    The counterpart of `InMemoryOutcomeStore`, and just as unsuited to anything
    that has to survive a restart — `django_rakaia.consumer` supplies the durable
    pair.
    """

    def __init__(self) -> None:
        self._offsets: dict[tuple[str, str], str] = {}

    def load(self, consumer: str, stream_path: str) -> str | None:
        return self._offsets.get((consumer, stream_path))

    def commit(self, consumer: str, stream_path: str, offset: str) -> None:
        self._offsets[(consumer, stream_path)] = offset


@dataclass(frozen=True)
class Consumer:
    """One consumer of one stream, with everywhere it keeps things attached.

    Frozen, because every field is part of the identity the outcomes and the
    cursor are filed under: a consumer that could be repointed at another stream
    mid-run would commit one stream's watermark under another stream's key.
    """

    store: CursorStore
    """Where the events are. Any `ReadableStore` that also reports its head."""

    path: str
    """The stream this consumer reads."""

    name: str
    """Who the cursor and the outcomes belong to. Said once, here."""

    cursors: CursorLedger
    """Where the watermark is kept between runs."""

    outcomes: OutcomeStore
    """Where outcomes are kept. Required — see the module docstring."""

    subject_of: Callable[[StreamMessage], str] | None = None
    """What an outcome for a message is *about*. Defaults to its offset."""

    sequence_of: Callable[[StreamMessage], str] | None = None
    """What a message is ordered *within*. Defaults to the subject."""

    def run(
        self,
        apply: Callable[[StreamMessage], Iterable[Outcome] | None],
        *,
        on_error: OnErrorPolicy,
    ) -> Consumed:
        """Load the cursor, run one pass of `consume`, and report what it did.

        One pass, not a loop until caught up: how often to run is the caller's
        scheduling decision, and a method that blocked until the stream was
        exhausted would take it away from them.

        Args:
            apply: called once per message, as `consume` describes. Must be
                idempotent — a run that halts, or one that crashes between
                applying and committing, delivers the same message again.
            on_error: ``"skip"`` or ``"halt"``. No default, deliberately.
        """
        return consume(
            self.store,
            self.path,
            apply,
            consumer=self.name,
            on_error=on_error,
            cursor=self.cursors.load(self.name, self.path),
            commit=lambda offset: self.cursors.commit(self.name, self.path, offset),
            outcomes=self.outcomes,
            subject_of=self.subject_of,
            sequence_of=self.sequence_of,
        )
