# Forms become rows

A form is one document, but people want to see tables: a list of projects, a
list of reports, and the lines of spending on each report. The first step turns
each form into those rows. The totals step then adds up each report's lines.

## Step 1: a form becomes rows

- A **project registration** creates the project: one row with its name, its
  suku, what it builds and its budget, named after the registration form.
- A **progress report** becomes one report row, plus one row for each line of
  spending: pipes 1200, labour 800.

When a report is sent again, its rows are brought into line with the new version.
The first road report had three lines, the third an extra culvert for 600.
The version sent later has two. After that save, the road report has two lines:
the culvert line is **removed**, not left behind as a stray row nobody can
explain.

## Step 4: totals

A report's total is not typed in by the officer. It is added up from the lines
again on every save. The road report totals 2200 with the culvert line
and 1600 without it, and the total changes on the same save that removes the
line.

Totals come after the status and link steps in the list, but they do not depend
on them. They only need the lines, which step 1 has already written.

## What goes wrong without it

When stale lines are left behind, a total adds up spending that was taken out of
the form, and the month shows as over budget for no reason anyone can find. When
a total is typed in by hand, it eventually disagrees with the lines, and then
nobody knows which one is right.

## Try it

```sh
just pipeline-demo --until totals
```

Watch the road report: total 2200 at save 3, total 1600 at save 5.

## Appendix

The report handler, from `examples/partisipa_pipeline/pipeline/steps.py`:

```python
def report_rows(event):
    report = Upsert(
        model_label=REPORT,
        lookup={"key": event["key"]},
        defaults={
            "suku": event["suku"],
            "output": event["output"],
            "period": event["period"],
        },
    )
    lines = reconcile_children(
        LINE,
        {"report_key": event["key"]},
        "line_index",
        event["lines"],
        lambda line: {"item": line["item"], "cost": line["cost"]},
    )
    return [report, *lines]
```

- An [Upsert](../glossary.md#upsert) creates the row the first time and
  updates it after that, so running it twice gives the same row. That is what
  makes a rebuild safe to repeat.
- `reconcile_children` returns one upsert per line, followed by a delete that
  spares only the lines the new version still has. The lines are keyed by their
  position, which suits a list that only grows or is cut from the end. For a
  list people reorder, see [Tree-reconcile](../tree-reconcile.md).
- Totals are a [reducer](../glossary.md#reducer) at stage 1. It runs once per
  pass, after every stage-0 handler, and reads the lines back through the
  reader:

  ```python
  def report_totals(reader):
      return [
          Update(
              model_label=REPORT,
              lookup={"key": report.key},
              defaults={
                  "total": sum(
                      line.cost for line in reader.filter(LINE, report_key=report.key)
                  )
              },
          )
          for report in reader.query(REPORT)
      ]
  ```

  It recomputes every report on every pass, which keeps the example short. A
  real pipeline would declare a second parameter, `touched`, and recompute only
  the reports the pass wrote; see `register_reducer` in the
  [API reference](../api-reference.md).
