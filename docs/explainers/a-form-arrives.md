# A form arrives

When a field officer sends a form, the first thing that happens is that it is
written to the **log**, in full and in the order it arrived. Nothing is worked
out yet, and no table is touched. The log also records who sent the form and
when.

Everything after this is worked out from the log. That is why the log is never
edited: a resubmitted report is a second entry, not a change to the first. Both
versions stay on record, and the later one wins when the tables are worked out.

## What goes wrong without it

If forms went straight into tables, the tables would be the only record. A
mistake in how a form was turned into rows would then be permanent, because the
form as it was sent is gone. With the log kept, a mistake is fixed by correcting
the step and running it over the log again.

## What a save does next

Straight after writing the form to the log, a save runs the steps for that one
form, so the tables are up to date by the time the officer's screen refreshes.
A rebuild skips this first step, because the log already holds every form.

## Try it

```sh
just pipeline-demo --until form_rows
```

Each `[n] Saved:` line in the output is one form written to the log and then run
through the steps that are switched on.

## Appendix

Appending and replaying one event, from `_append_and_replay` in
`examples/partisipa_pipeline/pipeline/routes.py`:

```python
position = len(store.read(stream)[0]) if store.has(stream) else 0
append_event(store, stream, event, label=label, actor=actor)
result = replay(
    store,
    stream,
    executor,
    handler_registry=registry,
    start_seq=position,
    end_seq=position + 1,
    reader=reader,
)
outbox.extend(result.external)
```

- `append_event` writes the event with an envelope: `actor` is recorded under
  `user`, and the label says what kind of write it was (`submit` for a form,
  `assign` for a decision the save recorded). See
  [Event envelope & provenance](../event-envelope.md).
- The save replays only the event it has just written: `start_seq` is that
  event's position, and `end_seq` is one past it. It uses the same handlers a
  rebuild uses, so a save and a rebuild cannot fill a table differently. The
  one thing only a save does is decide a report's project and record it, which
  [Status and project links](status-and-project-links.md) covers. The
  position is counted before the append, because that is the number a rebuild
  will give the event, and a [versioned handler](../versioned-handlers.md) is
  chosen by that number.
- A replay that reads earlier stages needs a `reader`, here the same tables the
  executor writes to.
