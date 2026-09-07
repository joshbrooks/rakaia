# Producer fencing & validation results

Rakaia's protocol server implements Kafka-style **idempotent producers** for
exactly-once append semantics: fire-and-forget writes with server-side
deduplication and zombie fencing. This page is the reference for the Python
result types that model that validation — the values `StreamStore.append()`
returns on the `producer_result` field of its `AppendResult`.

For the wire-level contract (the `Producer-Id` / `Producer-Epoch` /
`Producer-Seq` headers and their response codes) see
[Protocol specification §5.2.1 — Idempotent Producers](protocol.md#521-idempotent-producers).
These types are a **protocol-server (Tier 2) concern** — see
[Framework vs. protocol server](framework-vs-protocol-server.md). Framework-tier
code that only reads/writes through the Store contract never needs them.

## How it fits together

A write carrying the three producer headers is validated against the stream's
per-producer state before it is appended. `append()` returns an `AppendResult`
whose `producer_result` is one of the variants below; the ASGI handler
(`rakaia.handler`) maps each to an HTTP status. All three headers must be
supplied together or none at all (partial → `400`).

- **`ProducerState`** — the per-producer state the store tracks to make these
  decisions: `epoch`, `last_seq`, and `last_updated`. One per `producer_id`,
  held on the `Stream`.
- **`ClosedBy`** — the `(producer_id, epoch, seq)` tuple that performed the
  closing append, retained so a retried append-and-close is recognised as an
  idempotent success rather than a "stream closed" rejection.
- **`ProducerValidationResult`** — the union type aliasing the six result
  variants; use it in annotations.

## Result variants

| Result type | `status` | When it is returned | HTTP mapping |
| --- | --- | --- | --- |
| `ProducerAccepted` | `"accepted"` | Append accepted — new data, or a new epoch established (`epoch > current`, `seq == 0`). Carries `is_new` and the `proposed_state`. | `200 OK` (or `204` when no `producer_id`); echoes `Producer-Epoch` / `Producer-Seq` |
| `ProducerDuplicate` | `"duplicate"` | `seq <= last_seq` for the current epoch — a retried request already applied. Idempotent success, no re-append. Carries `last_seq`. | `204 No Content`; `Producer-Seq` = highest accepted seq |
| `ProducerStaleEpoch` | `"stale_epoch"` | `epoch < current` — a zombie/old producer session, fenced off. Carries `current_epoch`. | `403 Forbidden`; `Producer-Epoch` = current server epoch |
| `ProducerInvalidEpochSeq` | `"invalid_epoch_seq"` | A new epoch (`epoch > current`) that did not start at `seq == 0`. | `400 Bad Request` |
| `ProducerSequenceGap` | `"sequence_gap"` | `seq > last_seq + 1` within an epoch — a message was skipped. Carries `expected_seq` and `received_seq`. | `409 Conflict`; `Producer-Expected-Seq` / `Producer-Received-Seq` |
| `ProducerStreamClosed` | `"stream_closed"` | The target stream is already closed. | `409 Conflict`; `Stream-Closed: true` |

Only `ProducerAccepted` results in data being written; every other variant is a
no-op on the log. Match on the `status` string (a `Literal`) or `isinstance`
against the dataclass — both are exported from the top-level `rakaia` package.

## Example

```python
from rakaia import AppendOptions, ProducerAccepted, StreamStore

store = StreamStore()
store.create("orders")

result = store.append(
    "orders",
    b'{"id": 1}',
    AppendOptions(
        content_type="application/json",
        producer_id="worker-a",
        producer_epoch=0,
        producer_seq=0,
    ),
)

match result.producer_result:
    case ProducerAccepted():
        ...  # appended
    case None:
        ...  # no producer headers supplied — a plain append
    case rejected:
        # ProducerDuplicate / StaleEpoch / SequenceGap / InvalidEpochSeq / StreamClosed
        print("rejected:", rejected.status)
```
