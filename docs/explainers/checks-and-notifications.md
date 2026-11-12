# Checks and emails

The last two steps use everything the earlier ones worked out. One decides
whether the month can close. The other tells the coordinator when a report has
been approved, and it is the one step a rebuild must not repeat.

## Step 5: is the month ready?

Fatuberliu's March can close when every report in it is:

- **approved**,
- **linked** to a project, and
- **within** that project's budget.

When something is missing, the month says what it is. After save 3 it reads
*not ready: road report not approved; road report over budget; water report not
approved; water report has no project*. Each save that follows clears one of
those reasons. After save 7 it reads *ready*.

Nobody ticks the month off by hand. It is worked out again on every save from
the other tables, so it cannot fall behind them.

## Step 6: email the coordinator

When a report is approved, the coordinator gets an email. That is different from
every other step, because an email leaves the system. Once it is sent, it cannot
be taken back.

A rebuild runs this step too, over every approval in the log, and the step asks
for the same emails again. Rakaia never sends an email itself: it hands back what
the steps asked for, and whoever ran them decides. A save sends them. A rebuild
throws them away, because otherwise the coordinator would get every email again
each time. The step records that decision on the list, with its reason: *the email
went out when the report was approved; a rebuild must not send it again*.

The tables are different. A rebuild must reproduce every row, or the rebuilt
tables would not match live. So the line is not drawn between important steps
and unimportant ones. It is drawn between **rows**, which a rebuild always
reproduces, and **things that leave the system**, which it never repeats.

## What goes wrong without it

Without the reason written down, what a rebuild does with the emails is a
choice buried in the rebuild's own code. Someone who writes a new rebuild later
has nothing to tell them the emails must be thrown away, and the coordinator gets
a month of duplicates. The list makes the choice visible in one place, next to
the step it belongs to.

## Try it

```sh
just pipeline-demo
```

Saves 6 and 7 each print an `email to coordinator` line. The rebuild near the
end prints `not on a rebuild: notify_coordinator`, with its reason, and sends
nothing.

## Appendix

```python
def approval_email(event, reader):
    if event["status"] != "approved":
        return None
    return ExternalEffect(
        kind="email",
        payload={"to": "coordinator", "subject": f"{event['report']} approved"},
    )
```

- An [external effect](../glossary.md#external-effect) is never applied by rakaia.
  `replay` hands it back in `ReplayResult.external`, and the caller decides what
  to do with it. The save puts it in the outbox; the rebuild drops it. The split
  is by kind of effect, inside the replay: an executor never receives an
  external effect.
- On the step list, the email step carries
  `Skip("not_reproduced", "...")` in `on_rebuild`: the rebuild leaves the
  result out on purpose. The other kind the example uses is `not_needed`, work
  that means nothing on a rebuild, which is how deciding a report's project is
  marked. Both kinds come from partisipa-import's open proposal (GitHub
  #3443), which has a third, `done_elsewhere`.
- Partisipa (read 2026-11-13, in GitHub #3443's ADR-0056) has a third case
  this example does not: checks, such as flags and
  re-checks, that are rows on a save but that a rebuild deliberately does not
  recreate or compare (its proposal records them as `not_reproduced`). The
  month check here is not one of those; a rebuild reproduces it.
- The month check is a stage-2 reducer, so it runs after totals and links.
  Before that, the reports it reads would not yet be complete. The email is a
  stage-3 handler, so it runs last, as the list says. Within one stage a replay
  runs the handlers before the reducers, so putting both at stage 2 would have
  run step 6 before step 5.
- The step list records the decision; it does not enforce it. The rebuild
  receives the emails in `ReplayResult.external` whatever `on_rebuild` says.
  `test_only_a_step_that_reaches_outside_says_what_a_rebuild_does_with_it` is
  what holds the two together: it adds the steps one at a time and checks that
  the step which starts the emails is the one that carries a reason, and no
  other. It can only see emails the seven sample forms actually cause.
