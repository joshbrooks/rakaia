# Rebuild and compare

After the seven saves, the tables say Fatuberliu's March is ready to close. The
last step asks whether that is still what the log says. It rebuilds every table
from the log into an empty, throwaway database, then compares that database with
the live one, row by row.

## What the comparison catches

If the two match, the tables can be trusted: they are exactly what the steps
produce from the forms. The demo then changes one live row by hand, the way a
stray script or a hurried fix in the database might, and sets the water
report's total to 9999. It runs the comparison again, which reports:

> the water report's total: expected 2000, got 9999

The comparison names the row and the field, so the change is found and not
silently carried forward.

It also catches the opposite case: a step that was changed and now produces
something different from what is live. In that case the rebuild is right and
live is out of date, and the comparison shows exactly which rows a fix would
change, before anyone runs that fix on live.

## Why it is safe to run

The rebuild never writes to live. It is not allowed to read live while it runs,
either, because a step that peeked at live tables would make the rebuild look
right when it was not. Both rules are enforced while it runs, not just promised.

## What it does not check

The comparison only covers the tables it is told about. Here that is all six
of them. The coordinator's emails are not in any table, so the comparison says
nothing about them. That is why the email step gives its reason on the list
instead.

## Try it

```sh
just pipeline-demo
```

Near the end of the output, `compared 13 rows: green` is the rebuild matching
live. `compared 13 rows: red`, followed by the row that differs, is the planted
change being caught.

## Appendix

From `examples/partisipa_pipeline/pipeline/management/commands/demo_pipeline.py`:

```python
result = rebuild_and_verify(
    STREAM, into="scratch", live_models=TABLES, registry=registry
)
assert result.verdict == GREEN
```

- `into` must name an empty database alias. The example's `scratch` alias is an
  in-memory SQLite database, so it is empty every time the process starts. The
  demo also clears it before each comparison, because `rebuild_and_verify`
  refuses a non-empty one.
- `live_models` is the set of tables compared in both directions. A row only in
  live, a row only in the rebuild, and a field that differs are all reported.
  Database ids and auto-set timestamps are left out of the comparison, which is
  why links store the registration form's key rather than an id.
- Read `verdict` (`green`, `red` or `vacuous`), not `ok`. A rebuild that
  produced nothing compared nothing, and `ok` would be true for it.
- The read and write guards are `deny_database_access` and
  `assert_no_live_writes`.
- More: [Add streams to a Django model](../django-integration.md), and
  [Check a stream still matches its table](../check-stream-coverage.md) for
  the related check that every row has a stream at all.
