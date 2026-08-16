---
icon: lucide/help-circle
---

# Why Rakaia exists

Most Django projects that need to answer *"who changed this, and when?"* reach for
an audit-log tool. That works, right up until the day you need to do something
with the history other than read it.

Rakaia takes the other route. Instead of your tables being the truth and the
history being a copy of them, **the history is the truth and your tables are
built from it**. Every change is recorded once, in order, and your tables are
produced by reading that record back.

That one inversion buys three things an audit log can't give you.

## 1. You can rebuild a table

If your tables are derived from the log, a bug in how you derived them is not a
data-loss event. Fix the code, replay the log, and the tables come back correct —
including for rows that were written wrong months ago.

With an audit log, the table is the original and the log is the copy. You can
read what a row used to be, but you can't regenerate the table from it.

## 2. You can rehearse the rebuild before you run it

This is the part nothing else does.

Before touching real data, Rakaia can run the entire rebuild and tell you exactly
what it *would* write — every row it would create, update, or delete — while
physically blocking any write from reaching the database. If the code tries to
write anyway, the run fails loudly instead of quietly succeeding.

It also fails when a rebuild would do *nothing at all*. A dry run that produces
no changes is usually a broken rebuild rather than a clean bill of health, so
Rakaia reports that case as a distinct result rather than a pass.

So the question "is this migration safe?" stops being a judgement call.

## 3. Old data keeps working when the code changes

Your rules will change. Rakaia lets you say *"this rule applied between these two
points in the log"*, so a replay applies the rule that was correct at the time
rather than today's rule to all of history. Events written in an old shape are
translated forward on the way in, so old records stay readable without being
rewritten.

## Does it actually reproduce an audit log?

Yes, and this is checked on every commit rather than asserted.

The `partisipa_history` example takes a stream of changes, rebuilds an audit
trail from it, and compares that against a reference `pgh_event` table — the
table layout `django-pghistory` produces. It checks the order, the change type,
the person responsible, the timestamp, and the field values, and requires them to
match exactly. It also checks the recovery case: restoring a truncated record
from its most complete earlier version, which is the job people actually keep an
audit log for.

Run it yourself:

```bash
just partisipa-history-demo
```

It exits non-zero if any of that stops being true, and CI runs it.

!!! warning "The honest limits"

    - The comparison is against a faithful **model** of a `pgh_event` table, not
      a live `django-pghistory` installation. It proves the shape and contents
      match; it is not an integration test against that package.
    - Rakaia records changes when your model is saved. Operations that skip
      `save()` — notably `QuerySet.update()`, which Django documents as not
      emitting `pre_save` or `post_save` signals — are invisible to it.
      `django-pghistory` installs database triggers instead, so it catches those,
      and raw SQL besides. If your project writes in bulk, this difference
      matters and you should know about it before you choose.
    - Rakaia is pre-1.0. See [the public API](public-api.md) for what is and
      isn't promised.

## Is this for you?

**Probably yes, if:** you have a Django project with a load-bearing audit trail,
you have written one-off backfill scripts to repair derived data, and you would
sleep better previewing those before running them.

**Probably not, if:** you want change tracking and nothing more. An audit-log
package is less machinery and will catch bulk writes that Rakaia won't.

## Where next

- [Tutorial: your first rebuilt table](tutorial.md) — the shortest path from
  nothing to a table you can rebuild.
- [Glossary](glossary.md) — the vocabulary, in plain language.
- [Preview a replay with no writes](dry-run-and-executors.md) — how the rehearsal
  in point 2 works.

---

## Appendix — the parity design in detail

The reasoning behind the audit-trail replacement, the envelope fields it depends
on, and the migration path off `django-pghistory` are recorded in
[the pghistory parity notes](pghistory-retirement.md). That page assumes the
vocabulary; this one doesn't.
