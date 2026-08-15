# Close-precondition state machine — a spike for guarded transitions

An evaluation prototype for [issue #7](https://github.com/joshbrooks/rakaia/issues/7),
building on the [`partisipa_staged`](../partisipa_staged) staged-replay spike. It is
the capstone of the Partisipa lifecycle: not "project one form into one table", but
**decide a cross-form state transition from the projected state**.

> **This is a spike.** The staged orchestrator (with per-stage aggregates) lives in
> the example, not rakaia core — it prototypes the shape before a core API lands.
> See [`docs/close-preconditions.md`](../../docs/close-preconditions.md) for the design.

## The workflow it validates

In Partisipa a subproject cycle is closed by a `POM_1` form, but only if
`close_preconditions.py` passes: **every project 100 % complete, two verified
accountability meetings, and both cash balances ≥ 0**. Those facts live in
different forms (`PROGRESS`, `MEETING`, `FINANCE`) and often arrive out of order.
Today that gate is enforced imperatively, outside the event log.

This spike expresses the gate as a **guarded transition** — a replay decides each
`POM_1` close purely from the projected state:

- **Fatuberliu** meets every precondition → close **ACCEPTED**.
- **Maubara** fails three ways → close **REJECTED** with exactly
  `incomplete_projects, insufficient_meetings, negative_operational_balance`.

Then the events that fix Maubara arrive, and re-replaying **self-heals** the same
`POM_1` close to ACCEPTED — no bespoke re-check, no code change.

## Run

```sh
just partisipa-close-demo
```

Or directly:

```sh
cd examples/partisipa_close
uv run python manage.py migrate
uv run python manage.py demo_close
```

Expected output:

```
Seeded 13 events for 2 sukus, each ending in a POM_1 close request.
[1] GATE — POM_1 close decided from preconditions:
    Fatuberliu   ACCEPTED  —
    Maubara      REJECTED  incomplete_projects, insufficient_meetings, negative_operational_balance
    → Fatuberliu ACCEPTED, Maubara REJECTED with its exact 3 failing reasons ✓

[2] SELF-HEAL — append the fixes for Maubara and re-replay:
    Maubara      ACCEPTED  —
    → same POM_1 close is now ACCEPTED — no backfill, no code change ✓

[3] REPLAY-SAFE — the Balance aggregate is recomputed:
    Maubara operational balance: 50.00 -> 50.00 across 2 extra replays.
    → recompute-not-increment: balance stable across replays ✓

[4] DETERMINISTIC — re-replay the whole stream:
    2 cycle-close decisions — unchanged ✓

All close-gate checks passed ✓
```

Each check is asserted hard — a regression raises `CommandError` and exits non-zero.

## What's new here vs. the earlier spikes

| Concept | Where |
|---|---|
| **Guarded transition** — an event ACCEPTED/REJECTED by a predicate over the projected state | `handlers.cycle_close` + `close_preconditions` (stage 2) |
| **Replay-safe aggregate** — recomputed from contributing rows, never incremented | `handlers.balance_rollup` (stage 1 `reduce`) |
| Staged replay + `refs` (from `partisipa_staged`) | `staged_replay.staged_replay` / `DjangoProjectionReader` |
| Per-event fold within a stage (last-write-wins, no batch collision) | `staged_replay.staged_replay` |

The three-stage plan: **stage 0** builds `Project`/`Meeting`/`FinanceLine` per event;
**stage 1** recomputes the `Balance` aggregate; **stage 2** evaluates the `POM_1`
close against all of it.

## The checks have teeth

Each assertion was verified to fail on an injected regression:

- a **vacuous guard** (always ACCEPTED) fails `[1] GATE` (Maubara wrongly accepted);
- an **incrementing** balance aggregate fails `[3] REPLAY-SAFE` (balance drifts
  50 → 150 across replays);
- `[2]` only passes because the guard re-evaluates the healed state.

`[3]`/`[4]` deliberately re-replay **onto the existing projections** (no reset), so
they test true idempotency of re-applying effects — a reset would mask a
non-replay-safe aggregate by clearing it first.

## Files

* **`seed.py`** — two sukus (one passing, one failing three ways) + the heal events, and the expected initial verdicts.
* **`handlers.py`** — the stage plan: per-event facts, the balance aggregate, and the `close_preconditions` guard.
* **`staged_replay.py`** — the staged orchestrator with per-stage `reduce` steps and the read-only `DjangoProjectionReader`.
* **`models.py`** — the five projections, incl. the replay-safe `Balance` and the `CycleClose` state.
* **`management/commands/demo_close.py`** — seeds and runs all four asserted checks.

## Caveats

- Single stream, two sukus, one dependency depth. Real Partisipa spans several form
  streams (issue #7 #2, multi-stream merge) and a deeper close gate.
- `refs` reads the *latest* projection — correct for a close decision evaluated at
  replay time. A time-correct "was this closeable as of date X" would want an
  as-of snapshot, an open question for the core API.
- The guarded-transition and per-stage-aggregate shapes are prototyped here; making
  them first-class (a declared `guard=`/`reduce=` on handlers) is the proposal this
  spike motivates.
