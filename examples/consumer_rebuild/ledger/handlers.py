"""The pure handler the consumer and a rebuild both go through.

It emits an `Upsert` keyed on the row's own name with an absolute value, so
replaying the same event twice writes the same row twice rather than applying a
delta twice.
"""

from __future__ import annotations

from rakaia import Upsert, register_handler

STREAM = "ledger:entries"
MODEL = "ledger.LedgerRow"


def ledger_row(event: dict) -> Upsert:
    """The effect one ledger event means: set this key to this absolute value."""
    return Upsert(
        model_label=MODEL,
        lookup={"key": event["key"]},
        defaults={"value": int(event["value"])},
    )


@register_handler(name="ledger_row", event_match=STREAM, effective_from=0)
def apply_ledger_row(event: dict) -> Upsert:
    return ledger_row(event)
