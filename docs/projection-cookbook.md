# Projection cookbook

The pieces of a multi-form projection are documented one at a time elsewhere —
[versioned handlers](versioned-handlers.md), [staged replay](staged-replay.md),
[dry-run & executors](dry-run-and-executors.md). This page puts them in one place:
the full wiring for a staged projection you can copy, end to end.

```
StreamStore  →  register_simple / register_handler(stage=)
             →  replay(reader=DjangoProjectionReader(), executor=DjangoExecutor())
             →  diff_effects_against_rows        # verify the rebuild
```

The runnable companion is
[`examples/projection_cookbook`](../examples/projection_cookbook) — `just
cookbook-demo` runs everything below with asserted checks.

## The projection

A `PROJECT` form creates a reference entity; a `TASK` form creates a dependent
entity that links to its project by the `code` natural key. The events arrive
**out of order** — a task before the project it belongs to — which is exactly why
stages exist.

```python
# cookbook/models.py
class Project(models.Model):
    code = models.CharField(max_length=32, unique=True)   # natural key
    name = models.CharField(max_length=200, default="")

class Task(models.Model):
    task_id = models.CharField(max_length=32, unique=True)  # natural key
    title = models.CharField(max_length=200, default="")
    project = models.ForeignKey(Project, null=True, on_delete=models.SET_NULL)
```

## 1. Handlers, in stages

A handler is a pure function returning [`Effect`](versioned-handlers.md)
descriptions. Put them in an app's `handlers.py` and `django_rakaia`
autodiscovers them on startup — the decorators register into the default registry
with no explicit wiring.

Stage **0** builds the reference entities; stage **1** links to them. Replay runs
the whole event range through stage 0 before stage 1 begins, so a stage-1
handler's `reader` sees every `Project` — even one whose event arrives *after* the
task that needs it.

```python
# cookbook/handlers.py
from rakaia import Upsert, register_handler, register_simple

@register_simple(name="project", event_match="PROJECT", match_field="form_type")
def project(event):
    # register_simple = the always-on "just project this" case — no
    # effective_from=0 / effective_to=None ceremony. match_field routes on the
    # payload's form_type rather than the stream path.
    return Upsert(
        model_label="cookbook.Project",
        lookup={"code": event["code"]},
        defaults={"name": event["name"]},
    )

@register_handler(name="task", event_match="TASK", effective_from=0,
                  match_field="form_type", stage=1)
def task(event, reader):                      # stage > 0 ⇒ fn(event, reader)
    linked = reader.get("cookbook.Project", code=event["project_code"])
    return Upsert(
        model_label="cookbook.Task",
        lookup={"task_id": event["task_id"]},
        defaults={
            "title": event["title"],
            "project_id": linked.pk if linked is not None else None,
        },
    )
```

!!! tip "One handler for several form types"
    `event_match` also takes a **set** of patterns, so one registration can
    cover several unrelated form types that share no glob — a generic sweep
    instead of a `register(...)` per value:

    ```python
    @register_handler(name="sweep", event_match={"TASK", "NOTE", "MILESTONE"},
                      effective_from=0, match_field="form_type", stage=1)
    def sweep(event, reader): ...
    ```

## 2. Replay

Point `replay()` at the stream with a real executor and reader. It requires
`reader=` whenever any handler declares stage > 0; with only stage 0 handlers it
is a single pass and no reader is needed (backward compatible).

```python
from rakaia import replay
from django_rakaia import DjangoExecutor
from django_rakaia import DjangoProjectionReader

replay(store, "cookbook", DjangoExecutor(), reader=DjangoProjectionReader())
```

That's the whole projection: the two `P-100` tasks link to their project even
though they were appended before it. No reactive re-save signal, no backfill task.

## 3. Verify — does replay reproduce the rows?

The migration question is *"if I replay the log, do I get back exactly the rows I
have?"* Answer it read-only: replay with a
[`CollectingExecutor`](dry-run-and-executors.md) (which writes nothing) and diff
each write effect's `defaults` against the live rows.

```python
from rakaia import CollectingExecutor
from django_rakaia import diff_effects_against_rows

ex = CollectingExecutor()
replay(store, "cookbook", ex, reader=DjangoProjectionReader())

report = diff_effects_against_rows(ex.effects)   # UUID/Decimal normalised by default
assert report.certified, report                  # or: report.raise_if_diff()
```

`diff_effects_against_rows` returns a `DiffReport` (`.verdict` / `.certified` /
`.compared` / `.problems` / `.raise_if_diff()`); its default normalizers handle
the two representation mismatches that produce false diffs — a `UUID` column read
back vs a string in the effect, and a JSON float vs the column's rounded
`Decimal`.

### Don't accept a vacuous green

Note `assert report.certified` above, not `assert report.ok`. `.ok` answers "did
anything disagree?", which is vacuously true when **nothing was compared** — and
the ways a sweep silently compares zero rows are all ordinary: a store on the
wrong backend, a replay over a stream path that was renamed, an `event_match`
filter that stopped matching, a handler registry that failed to autodiscover.
Each yields an empty effect list, and `.ok` reports a clean bill of health with
nothing behind it.

`.verdict` separates the three outcomes that matter:

| Verdict | Meaning |
|---|---|
| `GREEN` | Rows were compared and all of them matched — real evidence. |
| `RED` | Something was compared and disagreed. |
| `VACUOUS` | Nothing was compared. This run certifies nothing. |

Failures are checked **before** vacuity: if anything disagreed then something was
evidently compared, so a real failure is never hidden behind "empty".

`.raise_if_diff()` raises `VerificationError` on `RED` and `VacuousVerification`
on `VACUOUS`. If an empty population is genuinely expected, say so explicitly at
the call site with `raise_if_diff(allow_empty=True)` — which still raises on a
real failure. `.ok` keeps its original meaning, so existing callers are
unaffected; `.certified` is the stricter property a migration proof wants.

### Verifying a large sweep in bulk

The diff does one `reader.get` per effect — one round-trip each. Fine for a
cookbook; on a full reconcile (tens of thousands of effects) it is tens of
thousands of round-trips. Give the *same* batch to a
[`PreloadedProjectionReader`](../src/django_rakaia/verification.py) and it
bulk-fetches every lookup up front, one query per `(model, lookup-shape)` group,
then serves each `get` from an in-memory snapshot:

```python
from django_rakaia import (
    PreloadedProjectionReader,
    diff_effects_against_rows,
)

effects = list(ex.effects)
reader = PreloadedProjectionReader(effects, using="rebuild")   # one bulk fetch per shape
diff_effects_against_rows(effects, reader=reader).raise_if_diff()
```

It is a **point-in-time snapshot** — build it for read-only verification, not for
live staged replay (where rows change as the replay writes them; use a plain
`DjangoProjectionReader` there). A `get` for a lookup outside the batch, or one
that spans a relation (`field__gte`), falls back to a live query and memoises it.

## Run it

```sh
just cookbook-demo
```

```
Seeded 5 events (the P-100 tasks arrive before the P-100 project).

Projects and their tasks:
  P-100  Water supply
    - T-1  Survey the site
    - T-2  Draft the budget
  P-200  Road repair
    - T-3  Order gravel

[1] out-of-order link: T-1 (seeded first) → P-100 ✓
[2] verification: replay reproduces all 5 projected rows ✓
[3] idempotent: a second replay is a no-op ✓
```

## Capstone: binding to a row you just made

Stage 1 above resolves the Project through the reader because stage 0 already
committed it. When two effects are **siblings in one handler's batch** — create a
row *and* bind to its primary key — a symbolic `Ref` removes even that ceremony:
the producing effect names its row, a sibling references it, and the executor
fills in the real pk at apply time.

> Ships with the symbolic-refs change (`produces=` / `Ref`).

```python
from rakaia import Ref, Upsert

return [
    Upsert(model_label="cookbook.Project",
           lookup={"code": event["code"]}, defaults={"name": event["name"]},
           produces="proj"),                                  # names this row
    Upsert(model_label="cookbook.Task",
           lookup={"task_id": event["task_id"]},
           defaults={"project_id": Ref("proj")}),             # → Project.pk
]
```

`Ref("proj")` resolves to the DB-assigned pk (right for an FK column); use
`Ref("proj", "code")` to bind a natural-key field instead.

## Capstone: verifying a from-scratch rebuild

The `CollectingExecutor` check above confirms replay reproduces rows that
*already exist*. To prove a **cold rebuild** — build the whole projection into an
empty schema — replay into a **disposable database** via the `using=` seam, so you
get full ORM fidelity without touching production:

> Uses the `using=` connection seam on `DjangoExecutor` / `DjangoProjectionReader`.

```python
# settings: a throwaway alias (in-memory sqlite for CI, scratch Postgres for cut-over)
DATABASES["rebuild"] = {"ENGINE": "django.db.backends.sqlite3", "NAME": ":memory:"}

replay(store, "cookbook",
       DjangoExecutor(using="rebuild"),
       reader=DjangoProjectionReader(using="rebuild"))
# ...then diff the rebuilt rows against production and discard the scratch DB.
```

Two caveats worth stating: diff on **natural keys**, not pks (a rebuild assigns
fresh pks); and sqlite `:memory:` is full *ORM* fidelity but not full *Postgres*
fidelity — use a scratch Postgres alias for the final cut-over proof. See
[Dry-run & executors](dry-run-and-executors.md) for the full treatment.
