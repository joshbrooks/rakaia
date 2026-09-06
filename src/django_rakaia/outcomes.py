"""SPIKE — a Django-backed `OutcomeStore`, to test whether the class is closed.

Not for merge as-is. This exists to answer one question: five review rounds each
found a value one backend kept and another refused, and the codec in
`rakaia.outcomes` was written to close that class by construction. A third backend
with constraints neither existing one has is the falsifiable test. If the shared
conformance suite and the cross-backend agreement tests catch this backend's limits
without anyone adding a case for them, the class is closed. If they pass while this
store truncates or raises, it is not.

The constraint the other two do not have is length: a `CharField` has a maximum and
`ConsumerCursor` sets the precedent for what those are.
"""

from __future__ import annotations

from django_rakaia.models_outcomes import EventOutcome
from rakaia.outcomes import Outcome, _order, decode, encode


class DjangoOutcomeStore:
    """Outcomes in a table, so `latest` survives a restart."""

    def __init__(self, using: str = "default"):
        self._using = using

    def record(self, outcome: Outcome) -> None:
        payload = encode(outcome)
        # `update_or_create`, not `create`: the shared contract says re-recording an
        # attempt replaces it, and a unique constraint on (consumer, path, subject,
        # attempt) refuses instead. The contract caught that on the first run of this
        # backend, with no Django-specific test written — which is the thing this
        # spike set out to measure.
        EventOutcome.objects.using(self._using).update_or_create(
            consumer=payload["consumer"],
            stream_path=payload["stream_path"],
            subject=payload["subject"],
            attempt=payload["attempt"],
            defaults={
                "offset": payload["offset"],
                "sequence_key": payload["sequence_key"],
                "stage": payload["stage"],
                "status": payload["status"],
                "reasons": payload["reasons"],
                "params": payload["params"],
            },
        )

    def latest(self, consumer: str, stream_path: str) -> list[Outcome]:
        rows = EventOutcome.objects.using(self._using).filter(
            consumer=consumer, stream_path=stream_path
        )
        best: dict[str, Outcome] = {}
        for row in rows:
            outcome = decode(
                {
                    "consumer": row.consumer,
                    "stream_path": row.stream_path,
                    "subject": row.subject,
                    "offset": row.offset,
                    "sequence_key": row.sequence_key,
                    "stage": row.stage,
                    "status": row.status,
                    "reasons": row.reasons,
                    "params": row.params,
                    "attempt": row.attempt,
                }
            )
            if outcome is None:
                continue
            held = best.get(outcome.subject)
            if held is None or outcome.attempt >= held.attempt:
                best[outcome.subject] = outcome
        return sorted(best.values(), key=_order)
