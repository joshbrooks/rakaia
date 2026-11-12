# One suku's forms, step by step

The code behind the [explainers](../../docs/explainers/index.md): a small
Partisipa-shaped pipeline that turns one village's month of forms into tables,
one step at a time. It then rebuilds every table from the log and checks that
the rebuild matches what is live.

The other `partisipa_*` examples each prove one idea on their own models. This
one runs the whole journey in order, so each step can be seen in place.

## Run

```sh
just pipeline-demo                         # every step, then rebuild and compare
just pipeline-demo --until project_links   # stop after one step
```

Each claim the demo prints is checked, and a failed one exits non-zero.

## The steps

| # | Step | What it does | On a rebuild |
|---|---|---|---|
| 1 | `form_rows` | A project form becomes a project; a report becomes a report and its lines | Same |
| 2 | `status` | A review decision becomes the report's status | Same |
| 3 | `project_links` | A save assigns each report to the registration for its suku and output, and records why | **Not needed**: a rebuild replays the recorded assignments |
| 4 | `totals` | Each report's lines are added up | Same |
| 5 | `month_ready` | The month is ready when every report is approved, linked and within budget | Same |
| 6 | `notify_coordinator` | An approval emails the coordinator | **Not reproduced**: the email went out already |

The seven forms (`pipeline/forms.py`) arrive in an awkward order on purpose. A
report arrives before its project's registration, a report is resubmitted with a
line removed, and the month only becomes ready with the last approval. A project
is named by the registration form that creates it, never by a project number.

## Where things are

- `pipeline/steps.py` holds the step list and every handler and reducer. The
  list is this example's own code, modelled on partisipa-import's proposal
  (catalpainternational/partisipa-import GitHub #3443). It is not a rakaia API;
  joshbrooks/rakaia GitHub #374 asks for one.
- `pipeline/routes.py` holds the two routes. `save_form` appends one form and
  replays just that event. `rebuild` replays the whole stream.
- `pipeline/management/commands/demo_pipeline.py` is the demo. Its comparison
  goes through `rebuild_and_verify` into the `scratch` database alias, which is
  in memory, so it is empty every run.
- `tests/test_examples/test_partisipa_pipeline.py` runs both routes over
  in-memory tables. It checks that they agree, that the save bringing a late
  registration assigns the waiting report, that a rebuild reproduces every
  assignment without deciding, and that the emailing step is the
  only one whose result a rebuild leaves out.
