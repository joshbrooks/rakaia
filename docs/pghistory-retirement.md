# pghistory parity — the design notes

> **Plain-language version:** [Why Rakaia exists](why-rakaia.md). This page is
> the detailed design behind it and assumes the vocabulary.
>
> Status: originally a design spike for
> [issue #11](https://github.com/joshbrooks/rakaia/issues/11), proved in
> [`examples/partisipa_history`](../examples/partisipa_history). **The envelope
> and the history read-model have since landed in core** — see
> [the event envelope](event-envelope.md) and
> [the history read-model](history-read-model.md). The "what making it
> first-class requires" list below is kept as a record and is now complete.

## The problem: the audit log is load-bearing

Partisipa's `Submission` table is tracked by `django-pghistory`. Retiring that in
favour of a rakaia stream is attractive — once `Submission` *is* a stream you have
two change logs (the stream and pghistory's `pgh_event` table), and unifying on the
stream makes history and recovery replay-native. But pghistory is not dead weight.
Three live consumers read it:

- **`GET /history` audit API** — returns each version's fields and a `+`/`~`/`-` diff
  marker, **plus the editing actor**.
- **Per-change actor context** — `pghistory.middleware.HistoryMiddleware` records who
  made each change; the API surfaces it.
- **`repair_blank_save_dataloss`** — a recovery command that restores a truncated
  submission from its historical *peak* (the snapshot with the most fields).

So a streams-based replacement is only safe if the stream can reproduce all three.
(`pgtrigger` — change_id sync sequences, soft-delete, protect, ordering — is a
**separate** dependency and is out of scope; it stays.)

## The gap: a plain append loses the audit metadata

The naive move is `stream.append(new_state)` — append the row's new fields on every
save. That records *what* the row became, but throws away the three things every
pghistory consumer reads: **who** changed it, **when**, and **what kind** of change
it was (create vs update vs delete). A history built from a fields-only stream can
list snapshots but cannot attribute or classify them.

## The proposal: an event envelope + a history read-model

Carry the audit metadata in a first-class **envelope** alongside the payload:

```json
{
  "schema_version": 1,
  "key": "sub-water-01",
  "op": "create",
  "actor": "aldina@pnds.tl",
  "ts": "2026-03-01T09:00:00Z",
  "fields": {"suku": "Fatuberliu", "output": "WATER", "beneficiaries": 120}
}
```

`op` is the one fact a plain append drops; everything the consumers need is
derivable from it (`op` → the `/history` `+`/`~`/`-` marker, and → the pghistory
trigger label `insert`/`update`/`delete`). From an enveloped stream, replay
materializes two projections:

- **`SubmissionRecord`** — current state, folded per event (last write wins). This
  is the `Submission`-table replacement.
- **`SubmissionHistoryEntry`** — one append-only row per event, keyed by
  `(submission_id, seq)` so re-replay is idempotent. This is the `/history`
  substrate and the recovery source.

Both are ordinary rakaia `Effect`s (including the `Delete` from #6), so the audit
log is not a special mechanism — it is just another projection of the log.

```python
# per event, in stream order — the same fold replay() performs
executor.apply(
    [
        Upsert(
            model_label="…SubmissionHistoryEntry",
            lookup={"submission_id": key, "seq": seq},
            defaults={
                "label": OP_TO_LABEL[op],
                "actor": actor,
                "ts": ts,
                "fields": fields,
            },
        ),
        Upsert(
            model_label="…SubmissionRecord",
            lookup={"submission_id": key},
            defaults={"fields": fields, "actor": actor, "updated_at": ts},
        ),
    ]
)
```

## Why the consumers are satisfied

| pghistory consumer | Streams equivalent | How |
|---|---|---|
| `/history` fields + `+`/`~`/`-` diff | `SubmissionHistoryEntry` | one row per event, `label` from `op` |
| Editing actor | `actor` on every entry | carried in the envelope |
| `repair_blank_save_dataloss` | a scan over the audit rows | `max(history snapshots, key=len)` — the pre-truncation snapshot never left the log |

The example asserts the stream-derived audit log reproduces a golden `pgh_event`
table **byte-for-byte** (order, label, actor, timestamp, canonical field snapshot),
and that stream recovery returns the same peak snapshot pghistory recovery does.

## Determinism, idempotency, and delete

- **Idempotent.** History rows are keyed by `(submission_id, seq)`; re-replaying the
  stream upserts the same rows. Current state is a deterministic fold in stream
  order.
- **Delete keeps history.** A `delete` envelope op removes the current `SubmissionRecord` but
  the create/update/delete events remain as history — the row goes away, its audit
  trail does not. That is exactly pghistory's behaviour and what an audit log must do.

## What making it first-class required — all since done

The spike carried the envelope as a JSON convention. Promoting it to core meant
three things, each of which has since landed:

1. ~~An **append surface** that accepts `actor` / `label` / `ts` / optional
   `causation` as structured metadata rather than payload keys.~~ Shipped as
   `provenance()`, `label_marker()` and `append_if_changed()` — see
   [the event envelope](event-envelope.md).
2. ~~Extending the durable **`StreamEvent`** model (which today has `event_type` /
   `created_at`) to persist that envelope.~~ Shipped: `StreamEvent.metadata` and
   `StreamEvent.event_ts` (`src/django_rakaia/models.py:262,265`).
3. ~~A **history read-model helper** so adopters get `SubmissionHistoryEntry`
   without hand-rolling it.~~ Shipped as `materialize_history()` /
   `history_effects()` — see [the history read-model](history-read-model.md).

## Migration path (issue #11)

1. Land the envelope + history read-model behind this parity spike.
2. Port `/history`, the admin event log, and `repair_blank_save_dataloss` onto the
   stream read-model.
3. Remove `@pghistory.track()` from `Submission`. Keep `pgtrigger`.

## Caveats

- `PghEventGolden` in the example is a faithful *model* of pghistory's `pgh_event`,
  not a live pghistory instance — enough to prove columns and snapshots match.
- One form type, snapshot-per-save. Real `Submission` history interacts with the
  flatten into `SeparatedSubmission`; that projector is the `formkit_submissions`
  spike's concern, and composes with this one (the flatten is a downstream
  projection of the same enveloped stream).
