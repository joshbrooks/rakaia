# pghistory retirement — a spike for a streams-native audit log

An evaluation prototype for [issue #11](https://github.com/joshbrooks/rakaia/issues/11),
following the [`formkit_submissions`](../formkit_submissions) adoption spike and the
[`partisipa_staged`](../partisipa_staged) late-binding spike.

> **This is a spike.** The event *envelope* and the history read-model here live in
> the example, not in rakaia core — they prototype the proposed API so we can prove
> the shape before building it. See [`docs/pghistory-retirement.md`](../../docs/pghistory-retirement.md)
> for the design.

## The question it answers

Partisipa's `Submission` table is tracked by `django-pghistory`, and that history is
**load-bearing**: it backs a user-facing `/history` audit API (with the editing actor,
via `HistoryMiddleware`), an admin event log, and the `repair_blank_save_dataloss`
disaster-recovery command. So "drop pghistory" is only safe if a rakaia stream can
reproduce all of it.

This spike shows it can — **if the stream carries an event envelope** (`op` / `actor`
/ `ts` alongside the payload). A plain `append(new_state)` records *what* the row
became but loses *who* / *when* / *what kind of change*, which is exactly what those
three consumers read.

## Run

```sh
just partisipa-history-demo
```

Or directly:

```sh
cd examples/partisipa_history
uv run python manage.py migrate
uv run python manage.py demo_history
```

Expected output:

```
Seeded 6 saves across 2 submissions (one later deleted); pghistory wrote 6 audit rows.
[1] PARITY — stream-derived audit vs golden pghistory:
    submission     lbl actor              snapshot
    sub-water-01   +   aldina@pnds.tl     {"beneficiaries":120,"output":"WATER","suku":"Fatuberliu"}
    ...
    → 6 rows reproduce pgh_event byte-for-byte ✓

[2] ENVELOPE — what a plain append(new_state) loses:
    fields-only stream: actor recoverable=False, create/update distinguishable=False → cannot serve /history or attribute a change.
    enveloped stream: 6/6 audit rows carry an actor + label.

[3] RECOVERY — restore the truncated submission:
    a blank save truncated sub-water-01 to 1 field(s) mid-history.
    peak snapshot from stream : {"beneficiaries":135,"cost":"1200.00","output":"WATER","suku":"Fatuberliu"}
    peak snapshot from pghist : {"beneficiaries":135,"cost":"1200.00","output":"WATER","suku":"Fatuberliu"}
    → stream recovery == pghistory recovery (4 fields); current row now healed to 4 fields ✓

[4] IDEMPOTENT — re-replay the whole stream:
    rows (6, 1) -> (6, 1) — idempotent ✓
    deleted submission row gone=True, but 2 history rows retained (create + delete).

All pghistory-retirement checks passed ✓
```

Each check is asserted hard — a regression raises `CommandError` and the command
exits non-zero.

## The three pghistory consumers, and how the stream serves each

| pghistory consumer (Partisipa) | Streams equivalent (this spike) | Check |
|---|---|---|
| `GET /history` audit API — per-version fields + `+`/`~`/`-` diff | `SubmissionHistoryEntry` derived from the stream | `[1] PARITY` |
| Per-change actor (`HistoryMiddleware` context) | `actor` on the envelope → on every audit row | `[2] ENVELOPE` |
| `repair_blank_save_dataloss` — restore peak snapshot | `stream_history.recover_peak_snapshot()` — one query over history | `[3] RECOVERY` |

`[1]` asserts the stream-derived audit log reproduces a golden `pgh_event` table
**byte-for-byte** (same order, label, actor, timestamp, and canonical field snapshot)
— the same prove-parity method as the `formkit_submissions` spike.

## How it maps to the proposal

| Proposed core feature (#11) | In this spike |
|---|---|
| First-class event envelope (`actor`/`label`/`ts`/`causation`) | `envelope.make_event` + the `op`/`actor`/`ts` keys |
| History read-model reproducing `/history` | `SubmissionHistoryEntry` + `stream_history.replay_history` |
| Streams-native recovery | `stream_history.recover_peak_snapshot` |
| Current-state projection (the `Submission` replacement) | `SubmissionRecord`, folded per event via `DjangoExecutor` |

Everything is applied as ordinary rakaia `Effect`s (including the `delete` op shipped
in #6), so nothing about the audit log is bespoke — it is just another projection.

## Files

* **`seed.py`** — the `SAVES` source of truth (create → edit → truncating blank save → fix → delete). Both paths are built from it, so any divergence is real.
* **`envelope.py`** — the proposed event envelope + the `op`↔label↔pgh-label mappings.
* **`pghistory_today.py`** — the status quo: populates a golden `pgh_event` table and its recovery query.
* **`stream_history.py`** — append envelopes, replay into `SubmissionRecord` + `SubmissionHistoryEntry`, and recover from the stream. Includes the fields-only strawman.
* **`models.py`** — the two derived projections + the golden pghistory model.
* **`management/commands/demo_history.py`** — seeds and runs all four asserted checks.

## Caveats

- `PghEventGolden` is a *faithful model* of `django-pghistory`'s `pgh_event`, not a
  live pghistory instance, and it is seeded from the same `SAVES` as the stream. So
  the PARITY check proves the stream **losslessly carries and reconstructs the audit
  shape** (order, label, actor, ts, snapshot) — not fidelity to a real pghistory
  install. The `+`/`~`/`-` labels are additionally checked against the seed's `op`
  directly, so that mapping isn't merely self-consistent. A production port would
  validate against a live `pgh_event` table.
- The envelope is a JSON convention here. Making it first-class means extending the
  append surface and the durable `StreamEvent` model (issue #11).
- `pgtrigger` (change_id sync sequences, soft-delete, protect, ordering) is a
  **separate** dependency and is unaffected — this spike is only about `pghistory`.
- One form type, snapshot-per-save. Real `Submission` history spans the flattening
  into `SeparatedSubmission`; that projector is the `formkit_submissions` spike's job.
