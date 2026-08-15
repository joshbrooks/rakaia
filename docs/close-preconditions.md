# Close-precondition state machine (design spike)

> Status: **design spike** for [issue #7](https://github.com/joshbrooks/rakaia/issues/7).
> Prototyped in [`examples/partisipa_close`](../examples/partisipa_close); not yet part
> of rakaia core. This page is the design; the example is the proof.

## The problem: a state transition gated by cross-form facts

Every rakaia projection so far has been an *unconditional* fold — each event
produces its effects regardless of the rest of the world. Partisipa's subproject
lifecycle needs something the earlier spikes don't cover: a **guarded
transition**. A `POM_1` form closes a cycle, but only if `close_preconditions.py`
holds — *all projects 100 % complete, two verified accountability meetings, both
cash balances ≥ 0*. Those facts come from different forms (`PROGRESS`, `MEETING`,
`FINANCE`), arrive out of order, and the close must be **rejected with specific
reasons** when they don't yet hold — then **accepted** once they do.

Today that gate is enforced imperatively, outside the log. Expressing it as a
replay makes the close decision a pure, auditable, self-healing function of the
stream.

## The proposal: guards + replay-safe aggregates on staged replay

Two capabilities layered on the staged replay from
[`staged-replay.md`](staged-replay.md):

### 1. A guarded transition

A handler whose effect is a function of a **predicate over the projected state**.
The predicate reads earlier stages through `refs` (the staged-replay accessor),
so it stays a pure function of the log:

```python
def close_preconditions(suku, refs) -> list[str]:
    reasons = []
    if any(p.percent < 100 for p in refs.filter(PROJECT, suku=suku)):
        reasons.append("incomplete_projects")
    if refs.filter(MEETING, suku=suku, verified=True).count() < 2:
        reasons.append("insufficient_meetings")
    bal = refs.get(BALANCE, suku=suku)
    if bal is None or bal.operational < 0:
        reasons.append("negative_operational_balance")
    ...
    return reasons

def cycle_close(event, refs):                       # stage 2
    reasons = close_preconditions(event["suku"], refs)
    return Upsert(model_label=CYCLE_CLOSE,
                  lookup={"suku": event["suku"]},
                  defaults={"status": "ACCEPTED" if not reasons else "REJECTED",
                            "reasons": reasons})
```

Because the verdict is derived, not stored, appending the missing facts and
re-replaying flips REJECTED → ACCEPTED with no backfill task. The close decision
self-heals the same way a late-arriving reference link does in the staged spike.

### 2. A replay-safe aggregate

The balance preconditions need a *sum*, and an increment is not replay-safe (it
doubles on re-replay). The fix is the aggregate analogue of `reconcile_children`:
**recompute** the total from the contributing rows on every replay and emit one
idempotent upsert.

```python
def balance_rollup(refs):                           # stage 1 reduce step
    effects = []
    for suku in distinct_sukus(refs):
        rows = refs.filter(FINANCE_LINE, suku=suku)
        effects.append(Upsert(model_label=BALANCE,
            lookup={"suku": suku},
            defaults={"operational": sum(r.delta for r in rows if r.account == "operational"),
                      "infrastructure": sum(r.delta for r in rows if r.account == "infrastructure")}))
    return effects
```

This is the crux of Partisipa's double-count bug class: recompute-not-increment
makes re-replay a no-op on the totals.

## The stage plan

| Stage | Kind | Produces |
|---|---|---|
| 0 | per-event handlers | `Project`, `Meeting`, `FinanceLine` (raw facts) |
| 1 | `reduce` step | `Balance` (recomputed aggregate) |
| 2 | per-event handler | `CycleClose` (the guarded transition) |

A stage's per-event handlers are applied as a **fold** (per event, in stream
order) so two events touching the same row — a corrected `PROGRESS`, say — don't
collide inside one batch; the per-stage `reduce` steps then run once against the
committed projections.

## Determinism and idempotency

- The verdict is a pure function of the projections, which are pure functions of
  the log — so replay is deterministic and the transition self-heals.
- Re-replaying **onto existing state** (no reset) must be a no-op: the per-event
  upserts are idempotent and the aggregate is a recompute. The example asserts
  this directly (a reset between replays would mask a non-replay-safe aggregate).

## What making it first-class requires

The spike carries stages, `reduce` steps, and the guard as example code. Promoting
to core means:

1. A declared **stage** on `@register_handler` (shared with #7 #1).
2. A **`reduce=`** step kind for per-group aggregates (this is #7 #3,
   `reconcile_aggregate`).
3. A guard whose rejection is a first-class, queryable outcome — so a `POM_1` that
   fails preconditions is recorded as REJECTED-with-reasons rather than silently
   dropped.

## Relationship to the other spikes

- **Staged replay** (#7 #1, `partisipa_staged`) supplies the stages + `refs`.
- **Multi-stream merge** (#7 #2) would let the `PROGRESS`/`MEETING`/`FINANCE`
  facts live in separate form streams instead of one.
- **`reconcile_aggregate`** (#7 #3) is the balance rollup, generalized.
- This spike is what ties them together into a subproject *lifecycle*.
