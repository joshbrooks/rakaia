# Projection cookbook — staged replay + reader + executor + diff

The end-to-end recipe for a multi-form projection, in one place:

```
StreamStore  →  register_simple / register_handler(stage=)
             →  replay(reader=DjangoProjectionReader(), executor=DjangoExecutor())
             →  diff_effects_against_rows   (verify the rebuild reproduces the rows)
```

A **`PROJECT`** form creates a reference entity; a **`TASK`** form creates a
dependent entity that links to its project by a natural key (`code`). The catch:
the task events are seeded **before** the project they belong to. Staged replay
links them anyway — stage 0 builds every `Project`, then stage 1 links each
`Task` through a read-only reader — so there is no reactive re-save and no
backfill task.

This is the runnable companion to
[`docs/projection-cookbook.md`](../../docs/projection-cookbook.md).

## Run

```sh
just cookbook-demo
```

Or directly:

```sh
cd examples/projection_cookbook
uv run python manage.py migrate
uv run python manage.py demo_cookbook
```

Expected output:

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

All checks passed.
```

## What each file shows

| File | The piece of the recipe it demonstrates |
|---|---|
| `cookbook/handlers.py` | `register_simple` (stage 0, no `effective_from` ceremony) + a stage-1 `register_handler` called `fn(event, reader)` |
| `cookbook/seed.py` | events **out of order** — the reason stages exist |
| `cookbook/models.py` | the `Project` / `Task` read-model rows a replay materialises |
| `cookbook/management/commands/demo_cookbook.py` | the replay call + `diff_effects_against_rows` verification, as asserted checks |

Handlers are **autodiscovered** — `django_rakaia` imports every installed app's
`handlers.py` on startup, so the `@register_simple` / `@register_handler`
decorators run and populate the default registry with no explicit wiring.

## Going further

The cookbook doc closes with two capstones covered by the library:

- **Symbolic refs** (`produces=` / `Ref`) — bind one effect to a sibling's
  generated key without a staging split.
- **Verifying a from-scratch rebuild** — replay into a *disposable* database via
  the `using=` seam (`DjangoExecutor(using=...)`), so you can prove a cold
  rebuild before cut-over. See
  [`docs/dry-run-and-executors.md`](../../docs/dry-run-and-executors.md).
