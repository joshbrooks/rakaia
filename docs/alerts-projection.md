# Alerts as a rakaia projection

An **alert** (partisipa's quality `Flag`) is a soft-delete row on an entity:
`(stream_key, alert_type, field_key)` natural key, `severity`/`message`, and a
`resolved_at` that is `NULL` while the alert is open and holds the *resolving
event's timestamp* once retired. Alerts come in three layers, each owned by a
different mechanism. Getting the ownership right is the whole game: a
re-derivation must never clobber a human's judgment, and vice-versa.

| Layer | Owner | rakaia mechanism | Status |
|---|---|---|---|
| **1. Authored / informational** (`alert`, `backfilled`, …) | an actor | appended `alert_raised` / `alert_dismissed` events → natural-key `update_or_create` (+ `external` transition) | **shipped (Phase 1)** |
| **2. Machine-reconciled** (validator violations) | the rules | `reconcile_by_key(..., retire={"resolved_at": ts})`, scoped by `retire_filter` to machine types | **shipped (Phase 2)** |
| **3. Dismissable warnings** (derived ⊕ authored) | rules *and* actor | reconcile **composed with** a standing `alert_dismissed` — authored wins for user-resolvable types | **planned (Phase 3, needs refs / #19)** |

## Phase 1 — authored alerts (shipped, zero core changes)

Authored alerts need no new primitive. Raise = upsert the natural key with
`resolved_at=NULL`; dismiss = upsert the same key with `resolved_at=<event ts>`,
`resolved_by=<actor>`. Both are plain, idempotent `Effect(op="update_or_create")`.
The raise/dismiss event each carries its own `Effect(op="external",
kind="alert_transition")`, so **one event = one transition**, and replay drops
all `external` effects — a rebuild never re-spams. This satisfies the "one
transition per real state change, none on replay" rule *by construction*.

Reference: `tests/test_django_rakaia/test_alerts.py::TestAuthoredAlerts` and the
replay-discipline test in `tests/test_rakaia/test_alerts.py`.

## Phase 2 — machine-reconciled alerts (shipped)

Three additions to core made a scoped, soft-delete, natural-key reconcile
expressible (the gaps the flag spike called G1/G2/G3):

- **`reconcile_by_key`** (`rakaia.projections`) — upsert one row per current
  violation keyed by a *composite* natural key merged into `scope`, plus a
  single retire pass over `scope` narrowed by an independent **`retire_filter`**
  (G1), sparing the composite keys still present (G2).
- **`op="retire"`** with a **`patch`** dict (`rakaia.effects`) — soft-delete by
  UPDATE (`resolved_at = <event ts>`) instead of DELETE (G3). The patch value is
  the triggering event's timestamp, never `timezone.now()`, so replay is
  deterministic. `reconcile_by_key` auto-adds an **open-guard**
  (`<field>__isnull=True` for each patch field) so a re-run neither re-stamps an
  already-resolved row nor re-fires a transition.
- **`spare_keys`** on delete/retire (`rakaia.effects` + `DjangoExecutor`) — the
  executor builds `.exclude(Q(**k0) | Q(**k1) | …)` for composite keys.

The headline property (oracle criterion 2, **zero clobber**): because the retire
is scoped by `retire_filter` to machine `alert_type`s, a re-derivation cannot
touch authored rows —
`tests/test_django_rakaia/test_alerts.py::TestMachineReconciledAlerts::test_zero_clobber_authored_untouched`.

`reconcile_children` is left as the positional special case (`key=(index,),
retire="delete"`); it and `reconcile_by_key` share the executor. Unifying the
two is a safe future cleanup, not required.

### Known limitation carried into Phase 3

`reconcile_by_key` emits **no** `external` transition. One retire Effect covers N
rows, and a pure handler cannot see which rows actually flipped, so it cannot
emit "one transition per real machine resolution" without either (a) an
executor-level diff (return the rows a retire actually updated) or (b) the refs
capability below. Until then, machine resolutions are silent; only authored
transitions notify. **Decision needed in Phase 3.**

## Phase 3 — composition (derived ⊕ authored), needs refs / #19

**Goal.** Open alerts = `reconcile(machine violations)` **⊖** standing
dismissals (user-resolvable only) **⊕** authored raises. A validator warning a
human dismissed must **not** be re-raised while its rule still fails (oracle
criterion 3: 27 re-raises → 0), yet a *machine* (non-user-resolvable) type must
ignore dismissals.

**The blocker.** A handler is a pure function of a **single event**
(`version.fn(event)`); there is no way to read "the latest authored
`alert_dismissed` for this key" while reconciling. rakaia has **no cross-stream
/ refs capability today** — this is issue #19, and alerts is its first real
consumer. Phase 3 cannot land without it.

### Work breakdown

1. **refs primitive (#19) — prerequisite, the bulk of the work.**
   A replay-time, deterministic read of *derived state from another projection
   (or an earlier fold of the same stream)*, exposed to handlers. Minimum viable
   shape for alerts: "latest authored `alert_dismissed` per `(stream_key,
   alert_type, field_key)`, with its event version/seq." Design decisions:
   - **Source**: fold the authored events of the same submission stream into a
     `standing_dismissals` map, vs. read a materialized `Alert`/dismissal table.
     Folding the stream keeps replay self-contained and deterministic; reading a
     table introduces read-ordering hazards during a rebuild. *Recommend the
     stream-fold.*
   - **Handler surface**: extend the handler signature to `fn(event, refs)` (or
     inject a context) without breaking the existing `fn(event)` handlers —
     resolve via `inspect`/opt-in, mirroring how `match_field` was added.
   - **Determinism**: refs must be a function of events at seq ≤ current only;
     never "current DB state." Add a replay test that a mid-stream replay sees
     only dismissals up to that seq.

2. **`MACHINE_ALERT_TYPES` split.** Port partisipa's
   `NON_USER_RESOLVABLE_FLAG_TYPES`; user-resolvable = everything else. Machine
   types ignore dismissals; user-resolvable types honor a standing dismissal at
   `version ≥ the current violation's version`.

3. **Composition in the reconcile.** For a user-resolvable violation whose key
   has a standing dismissal ≥ its version, emit the row **resolved** (or skip the
   upsert and let the retire stand) instead of open. Encode as a filter on
   `items`/`defaults_fn` fed by refs — keep it inside `reconcile_by_key`'s
   contract rather than a new op if possible.

4. **Machine-transition notifications** (resolve the Phase-2 limitation). Pick
   (a) executor returns retired-row keys → orchestrator emits transitions, or (b)
   derive transitions from refs (prev open-set vs new). *Lean (a):* it keeps
   handlers pure and needs no cross-stream read.

### Acceptance — the spike oracle

Run `spike/rebuild_ida_status/flag_reconcile_spike.py` over the same population;
a rakaia projection must hit:

1. **Machine parity** — projected open machine alerts == a fresh `run_validators`
   reconcile (baseline 93.6% in_sync; the delta is point-in-time drift the
   projection *removes*).
2. **Zero clobber** — authored alerts untouched (baseline 2027 at risk → 0).
   *Already provable at Phase 2.*
3. **Dismissals honored** — dismissed user-warnings not re-raised while failing
   (baseline 27 → 0). *Phase 3.*
4. **External discipline** — one `alert_transition` per real state change, none
   on replay. *Authored: Phase 1. Machine: Phase 3 item 4.*

### Open questions

- **Stream-keying (#22):** authored `alert_raised` / `alert_dismissed` on the
  **Submission** stream (recommended — replays in lockstep) vs. a dedicated
  alerts stream.
- **Attachment entity:** rakaia core stays entity-agnostic (`stream_key`); the
  concrete `Alert` model + `MACHINE_ALERT_TYPES` live in the consuming app
  (partisipa) / the example, not core.
