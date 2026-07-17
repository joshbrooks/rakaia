# FormKit submissions — a rakaia adoption spike for `formkit-ninja`

An evaluation prototype answering one question:

> [`formkit-ninja`](https://github.com/catalpainternational/formkit-ninja) turns
> a raw `Submission` (JSON) into typed Django rows via a passive, signal-based
> `SeparatedSubmission.to_model()` write. **If we drove that same population
> from a rakaia event stream instead, would we get identical rows — and what
> would it buy us?**

This spike re-expresses a `formkit-ninja` submission pipeline on rakaia and
proves the answer is **yes, identical** — while adding time-correct history that
plain signals cannot give you.

## The mapping

| `formkit-ninja` concept | rakaia equivalent here |
|---|---|
| `Submission` (raw JSON payload) | an event on the `submissions` stream |
| Stage 1 flatten → `SeparatedSubmission` (root + repeater rows) | `mapping.py` helpers over `fields` / `fields.activities` |
| Stage 3 `to_model()` → `update_or_create(submission_id=…, defaults=…)` | `Effect(op="update_or_create", lookup={"submission_id": …}, defaults=…)` |
| The `submission` OneToOneField anchor | `lookup={"submission_id": …}` on every effect |
| Producer form drift (renamed field) | `upcasters.py` (`pct` → `progress_pct`) |
| Mapping/business-rule change over time | `handlers.py` versioned `visit_summary` (v1/v2) |

The sample form is a **project monitoring visit** (Partisipa-flavoured): a root
node (`project_code`, `suku`, `monitor`, `visit_date`) plus an `activities`
**repeater**. It hydrates two typed models — `MonitoringVisit` (root) and
`ActivityProgress` (one row per repeater child).

## Run

```sh
just formkit-demo          # seed + replay + equivalence proof
just formkit-dev           # http://localhost:8003 — same rows in the browser
```

Or directly:

```sh
cd examples/formkit_submissions
uv run python manage.py migrate
uv run python manage.py demo_submissions --twice
```

Expected output:

```
Seeded 4 submissions (completion policy changes at seq 2).
Dry run: replay would apply 16 effects (no writes yet).
[replay] events=4 effects_applied=16

submission  project  suku            budget  progress       status
------------------------------------------------------------------
11111111    WS-014   Fatuberliu    10000.00    100.00     COMPLETE
22222222    RD-227   Maubara       10000.00     95.00  IN_PROGRESS
33333333    WS-031   Liquica       10000.00     91.60     COMPLETE
44444444    IR-108   Bobonaro      10000.00     88.00  IN_PROGRESS

[1] PARITY: rakaia replay == direct to_model() — identical ✓

[2] VALUE-ADD: a naive to_model() re-run would rewrite 1 historical visit row(s) that versioned replay preserves:
    22222222…  naive=COMPLETE  time-correct=IN_PROGRESS

[3] RECONCILE: IR-108 resubmitted with 1 activity (was 2) -> 1 row(s) — no orphan ✓

Replayed again: 4 -> 4 visit rows — idempotent ✓
```

## What the three assertions mean

**[1] Parity — the migration is safe.** The command runs the same submissions
two ways: through rakaia's `replay()` (stream → versioned handlers →
`DjangoExecutor`) and through `reference.py`, a plain imperative
`to_model()`-style write that mirrors what `formkit-ninja` does today. Both call
the *same* pure `mapping.py` helpers, so the only thing under test is the
plumbing. The projections come out byte-identical. Re-deriving typed rows from
the stream is behaviourally equivalent to the current direct write.

**[2] Value-add — what rakaia buys over signals.** The completion policy changed
at seq 2 (COMPLETE needs 100% before, 90% after). Submission `2222…` was
recorded *before* the change at 95%, so it is correctly `IN_PROGRESS`. A naive
`to_model()` re-run — the equivalent of re-firing today's signal handler over old
`Submission` rows — would silently promote it to `COMPLETE`. Rakaia's versioned
handlers keep it time-correct: **fixing the rule forward never rewrites
history.** This is the concrete answer to `formkit-ninja`'s existing
`reconcile_separated_submissions` reprocessing problem.

**[3] Reconcile — repeaters don't orphan.** The `activity_rows` handler uses
`reconcile_children`, which emits the per-activity upserts *and* a reconcile
delete. When IR-108 is resubmitted with one activity instead of two, replaying
the stream prunes the dropped child row instead of leaving it orphaned — the
trap a naive `update_or_create` fan-out falls into.

## How it fits together

* **`seed.py`** — sample submissions. List position = stream `seq` (selects the
  handler *version*); each event's `schema_version` drives the upcaster.
* **`mapping.py`** — pure field derivations, shared by the handlers and the
  reference so parity measures the plumbing, not coincidence.
* **`upcasters.py` / `handlers.py`** — autodiscovered by `django_rakaia` on app
  `ready()`; the `@register_*` decorators populate the process-wide registries.
* **`reference.py`** — the `formkit-ninja` baseline: direct `update_or_create`,
  no stream. What parity is checked against.
* **`management/commands/demo_submissions.py`** — seeds the in-memory stream,
  previews the effects with a `CollectingExecutor`, normalises the input with
  `upcast()`, replays, and runs the three assertions.
* **`models.py` / `views.py`** — the materialized projection and a read-only view.

### rakaia APIs this exercises

This spike drove six additions to rakaia (see the tracking issue). It now uses
all of them: the **delete `Effect`** op and **`reconcile_children`** (assertion
[3]), the **`CollectingExecutor`** dry-run, and the **`upcast()`** one-liner.
The **durable `DjangoStreamStore`** (`RAKAIA_STORE="durable"`) and **`match_field`**
content routing are available for a next iteration (see caveats).

### Why seed + replay live in one command

By default rakaia's store is **in-memory and process-local** — the event log is
not persisted (only the derived rows are), so a separate `manage.py replay`
invocation would find an empty stream, and the demo seeds and replays in one
process. In a real adoption you would set `RAKAIA_STORE="durable"` and emit from
`Submission.save()`, at which point the stream survives across processes and the
built-in `manage.py replay <stream>` command works unchanged.

## Caveats (this is a spike, not a recommendation)

* Rakaia is early-stage; this validates the *shape*, not production readiness.
* Only one `form_type` and a single repeater level are modelled. Real
  `formkit-ninja` submissions nest repeaters arbitrarily (`repeater_parent`
  chains) — mapping that to stream granularity is the next open question.
* The stream is keyed as one `submissions` stream; per-`form_type` streams
  (`form:{type}:submissions`) with `match_field` routing are the other option
  worth prototyping.
