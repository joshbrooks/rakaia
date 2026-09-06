---
icon: lucide/arrow-up-circle
---

# Upgrading

Breaking changes, and what to do about each, organised by the release that
carries them. Find the releases you are crossing and apply their changes in
order.

If you pin rakaia to a git revision rather than a version — which every consumer
did before `0.2.0`, since `0.1.0` predates nearly everything here — each section
below also names the revision the change landed in, so you can work out which
ones you are crossing.

---

# Unreleased

## A new table for the outcomes a consumer records

There is a new migration, `django_rakaia` `0010`, adding one table:
`rakaia_consumeroutcome`. Run `manage.py migrate django_rakaia` when you upgrade.

Nothing existing changes and nothing writes to the table on its own. It is where
`django_rakaia.outcomes.DjangoOutcomeStore` keeps a record of an event a consumer
could not apply — the third place outcomes can be kept, after the in-memory
reference and the JSONL files — and only a consumer that asks for that store ever
puts a row in it. A consumer that never fails, or never uses the store, carries an
empty table and nothing else; outcomes record exceptions only, so there is no row
per applied event.

One thing to know before you point a consumer at it. Four of the values are kept
in bounded columns, at the same widths `ConsumerCursor` already uses — the
consumer name at 128 characters, the stream path and the subject at 255, the
offset at 64 — and the store refuses an outcome whose name is longer rather than
shortening it, because a shortened subject is a different subject. Everything
else an outcome carries is unbounded. If your names can run longer than that,
they will not fit, and you will see the refusal when the outcome is recorded
rather than when it is read back.

---

# 0.3.0

## A replay applies a whole pass of effects at once, not one event at a time

`replay()` used to call `executor.apply()` once per event. It now buffers a pass's
effects and hands them over in as few batches as it can, breaking the batch
whenever holding one more back would change the outcome — a write arriving behind
a delete, a second write to the same column of the same row, a repeated
`produces=` id, or a retire that asked for notifications. Passes whose handlers
receive a reader are not batched at all, because a buffered write would be
invisible to them.

**What changes for you.** For most consumers, only the transaction and statement
count: on a nine-event save this went from nine `transaction.atomic()` blocks to
one, and with `DjangoExecutor(batch_updates=True)` from nine `UPDATE`s to one. The
rows written, and the order they are written in, are unchanged — there is a test
that replays the same effects both ways and compares.

Three cases are worth checking against your own code:

- **A custom executor sees fewer, larger batches.** If yours does per-call work —
  opens a connection, logs a line, counts calls in a test — the count drops. The
  flat sequence of effects it receives is identical.
- **A `Ref` to an earlier event's `produces=` row now resolves.** It used to raise
  `UnresolvedRefError`, because refs do not cross an `apply()` call and each event
  was its own call. Within one pass they can now be in the same batch. This is a
  widening: code that worked still works, and code that was relying on the failure
  was relying on a batch boundary that was never part of the contract. The
  executor-level rule is unchanged — refs still do not resolve across two explicit
  `apply()` calls, which `tests/executor_contract.py` pins.
- **A failure part-way through a pass now rolls back more.** `DjangoExecutor`
  wraps each `apply()` in a transaction, so a failing effect discards its whole
  batch rather than just its own event's effects. Events dispatched before the
  batch started are unaffected, and a malformed event still leaves everything
  before it applied. If you were depending on per-event commit granularity within
  a pass, you no longer have it.

**If you never call `replay()` directly**, and reach it only through
`django_rakaia.replay.replay_stream` or a rebuild, this needs no action.

## `poll` refuses a cursor from another store instead of reporting `rewound`

`rakaia.subscription.poll` compares your saved cursor against the stream head to
decide whether the log was rebuilt beneath you. If the cursor came from a
*different* store than the one being read, that comparison had no correct answer
to give and gave a wrong one: an in-memory cursor four events into a stream was
judged to be beyond a durable head at forty-two, because the two formats are
padded to different widths and the comparison fell back to text order.

It now raises `ForeignOffset` (a subclass of the existing `InvalidOffset`, so
`except InvalidOffset` still catches it).

**What changes for you.** Nothing, unless a cursor really is crossing stores —
in which case you previously got `Poll(status="rewound")`, which instructs a
consumer to discard its derived state and re-read the whole stream. The final
state was correct; the stated reason was not. If you have a consumer branching on
`.rewound`, it will now see an exception on that path instead. Clear the saved
cursor when you change stores.

Realistically this arises in a test that builds a cursor with the wrong store, or
from a corrupted saved value — the in-memory store is for tests, demos and the
conformance suite, so no deployment accumulates a cursor under it and then
switches.

**If you supply your own store, nothing changes at all.** The refusal fires only
when rakaia can see that both tokens came from its own two formats and that those
formats differ. A third-party `CursorStore` issuing ULIDs, timestamps or hex
offsets matches neither, and its cursors are compared byte-wise exactly as before
— which is the ordering rule the protocol states for any opaque offset.

---

# 0.2.0

Everything in this file so far. `0.1.0` was the initial groundwork and `0.2.0`
is the first release describing the library, so a consumer moving off a pinned
revision is crossing all of the below at once.

## Start here if you installed `rakaia-streams==0.1.0` from PyPI

The sections below are keyed to the revisions their changes landed in, which is
what a SHA-pinned consumer needs — but the **published** `0.1.0` is not the
oldest of those revisions, so part of the work below is already behind you.

`v0.1.0` is `f676302`, which is `5e4a6e3` plus exactly two commits: `5d576b6`
(the issue #80 Django fixes) and the packaging commit itself. So from published
`0.1.0`:

| Change | From published `0.1.0` |
|---|---|
| The distribution rename to `rakaia-streams` | **Already done** — `f676302` *is* that commit, so you installed under the new name. |
| Migration `0006` | **Already applied** — it shipped in `0.1.0`. |
| `DjangoStreamStore.get()` returns metadata, not the ORM row | **Still to cross.** This is your one code change. |
| Migrations `0007` and `0008` | **New.** Apply both; `0008` drops a table. |

### The one break that matters, and why testing may not show it

`store.get()` no longer returns the `django_rakaia.models.Stream` ORM row, so
anything that hands the result to the ORM breaks:

```python
stream = store.get(path)
entries = StreamEntry.objects.filter(stream=stream)  # <-- TypeError
```

```
TypeError: Field 'id' expected a number but got Stream(path='history/tf',
  content_type=None, messages=[], current_offset='00000000000000014678', ...)
```

The fix is one line, and it is one query instead of two:

```diff
-entries = StreamEntry.objects.filter(stream=store.get(path))
+entries = StreamEntry.objects.filter(stream__stream_id=path)
```

**Test this against a database that already has streams in it.** In the first
production consumer every one of these calls sat behind an `if store.has(path):`
guard, so on an empty database the branch is never taken, the upgrade looks
clean, and the failure only appears on the *second* run once the stream exists.
A consumer who validates the upgrade on a fresh database will conclude it is
clean and ship the break. Grep for `store.get(` and check what each result is
passed to.

### `0007` walks every `Stream` row

`0007` carries a data migration seeding `last_activity_at` from `created_at`,
one row at a time. On a fresh install that costs nothing — a consumer whose
production database had never installed `django_rakaia` measured the entire
`migrate`, `0001` through `0008`, at **8 seconds** against empty tables. On a
database that already holds streams the cost is proportional to how many: that
same deployment reached **95,635 events across 31 streams** once seeded. If your
log is already large, plan a window.

## Upgrading past `5e4a6e3` (`append_many`, 2026-08-08)

Three changes need action. Nothing else in this range requires a code change.

### 1. The distribution is now `rakaia-streams`

`rakaia` was already taken on PyPI by an unrelated placeholder, so the published
distribution is **`rakaia-streams`**. **The import name is unchanged** — it is
still `import rakaia` and `import django_rakaia`. Only the packaging name moved.

This matters even if you install from git: `uv` resolves the requirement name
against the name the source builds, so a source pinned past `f676302` no longer
satisfies a requirement spelled `rakaia`, and `uv sync` fails.

```diff
 dependencies = [
-    "rakaia",
+    "rakaia-streams",
 ]

 [tool.uv.sources]
-rakaia = { git = "https://github.com/joshbrooks/rakaia.git", rev = "<old>" }
+rakaia-streams = { git = "https://github.com/joshbrooks/rakaia.git", rev = "<new>" }
```

From PyPI instead: `pip install rakaia-streams` / `uv add rakaia-streams`.
No import statement changes.

### 2. `DjangoStreamStore.get()` returns metadata, not the ORM row

`get()` used to return the `django_rakaia.models.Stream` **row**. It now returns
a `rakaia.types.Stream` — a plain metadata snapshot, the same type the in-memory
store returns.

The change is deliberate. A protocol server is async, and an ORM row is lazy:
reading `stream.current_offset` off one issues a query at attribute access,
outside the store's `run_sync` bridge, which Django refuses from an async
context. Everything a server reads is now resolved inside the sync call and
handed over inert. That is what lets one protocol server run on either store.

The snapshot carries `path`, `content_type`, `current_offset`, `last_seq`,
`ttl_seconds`, `expires_at`, `created_at`, `last_activity_at`, `closed` and
`closed_by`. Its `messages` list is always empty — read the stream with `read()`.

**If you were using the result as an ORM row**, query the row directly. You do
not need `get()` at all for this:

```diff
-stream = store.get(path)
-entries = StreamEntry.objects.filter(stream=stream)
+entries = StreamEntry.objects.filter(stream__stream_id=path)
```

That is one query instead of two, and it does not go through the store — which
is the point: the ORM row was never part of the store interface, it just happened
to be what came back.

Where you genuinely want the row (admin, a migration, a data fix), reach for it
by name: `Stream.objects.get(stream_id=path)`.

`StreamServerStore.get` is now declared `-> Stream | None` rather than `-> Any`,
and `tests/server_store_contract.py` asserts the type on both backends — so this
particular change cannot happen again unannounced.

### 3. Two migrations to apply

```bash
python manage.py migrate django_rakaia
```

- `0006` — re-declares `StreamEvent.data` and `.metadata` with
  `encoder=DjangoJSONEncoder`, so a payload containing a `UUID`, `datetime` or
  `Decimal` no longer raises `TypeError` at insert time. No data is rewritten.
- `0007` — adds the protocol-lifecycle columns to `Stream` (content type, TTL,
  expiry, closed/closed-by, last-seq, last-activity) and the `StreamProducer`
  table. It carries a **data migration** seeding `last_activity_at` from
  `created_at` for existing rows, so plan for it to touch every `Stream` row.

Both are additive; neither drops a column.

### Not a break, despite looking like one

`read()` now raises `StreamNotFound` where it used to raise a bare `KeyError`,
and `InvalidOffset` for an offset the store did not issue. Every named store
failure subclasses the builtin it replaced (`StreamNotFound` is a `KeyError`,
`StreamConfigConflict` and the rest are `ValueError`s), so existing
`except KeyError` / `except ValueError` code keeps working. Catching the named
types is better, but it is not required to upgrade.

`append()` now returns an `AppendResult` on the durable store rather than the
`StreamEntry` row. If you were discarding the return value — as every known
consumer was — nothing changes. The entry is still reachable via `read()`.

`create()` gained keyword-only options (`content_type`, `ttl_seconds`,
`expires_at`, `initial_data`, `closed`). `create(path)` is unchanged.

`AppendOptions.seq` and `Stream.last_seq` are `str | None`, and the durable
`Stream.last_seq` column is text — migration `0009` widens it. `Stream-Seq` is
an opaque string compared byte-wise, as the protocol requires, so any value is
accepted and none is rejected as malformed.

**If your writer sends unpadded decimals, pad them.** Byte-wise, `"10"` sorts
below `"9"`, so `Stream-Seq: 10` after `9` is a `409 Conflict`. Send `"09"` then
`"10"` — zero-padded to whatever width your writer will reach — or use a ULID.
A writer already padding, or not sending `Stream-Seq` at all, needs no change.

---

## The `Translatable` model has left the library

`django_rakaia` shipped a translations demo — a `Translatable` model and manager,
an admin, an HTMX dashboard, JSON endpoints and a translations SSE feed. It was
demo domain rather than library surface (its `langcode` choices were hard-coded
to `tet`/`pt`/`id`), and because the model was declared in `0001_initial`,
**every** consumer got a `django_rakaia_translatable` table whether they used it
or not. It has moved to `examples/polyglot`, the only thing that ever used it.

**Migration `0008` drops the table.** If you have rows in it, copy them out
first:

```bash
python manage.py dumpdata django_rakaia.Translatable > translatable.json
python manage.py migrate django_rakaia
```

Nothing else in rakaia referenced it, so if you never used it there is nothing
to do — the table simply goes away.

**If you were using it**, the model is small and self-contained; declare it in
your own app rather than depending on the library for it:

```python
from django.db import models
from django.utils import timezone


class Translatable(models.Model):
    msgid = models.CharField(max_length=2048)
    msgstr = models.CharField(max_length=2048, null=True, blank=True)
    domain = models.CharField(max_length=2048, null=True, blank=True)
    msgctxt = models.CharField(max_length=2048, null=True, blank=True)
    langcode = models.CharField(max_length=3, default="en")
    deleted = models.DateTimeField(null=True, blank=True)

    class Meta:
        unique_together = [["msgid", "msgctxt", "langcode"]]
        indexes = [models.Index(fields=["msgid", "msgctxt", "langcode"])]
```

The gettext-mirroring manager (`gettext`, `ngettext`, `pgettext`, `npgettext`)
is in [`examples/polyglot/polyglot/models.py`](https://github.com/joshbrooks/rakaia/blob/main/examples/polyglot/polyglot/models.py)
— copy it if you want it. Note polyglot's table is `polyglot_translatable`, a
*different* table, so loading a dump into it needs the app label changed.

**Also removed** with it: the `/streams/translations/` page, the
`/streams/api/translations/…` endpoints (JSON, HTMX and SSE), the
`TranslatableAdmin`, and the `post_save` receiver that broadcast events carrying
a `translatable_id` to a `"translations"` channel group. The stream dashboard,
the stream SSE endpoint and everything else under `/streams/` are unaffected.

One incidental routing change: a stream literally named `translations` now
resolves to the stream detail page, because `/streams/translations/` is no
longer claimed by the dashboard.

---

## `Effect` is now four types, one per operation

This is the largest break in `0.2.0`. `Effect` shipped in `0.1.0` and was
exported, so every handler you have written constructs one.

The single `Effect` dataclass carried thirteen fields and an `op=` string
selecting which of them were meaningful. Ten of the thirteen were meaningless on
any given `op`, which is why the class needed five runtime `ValueError` checks to
police combinations that should never have been writable in the first place. It
is now four dataclasses, one per operation, each carrying only its own fields:

```python
from rakaia import Upsert, Update, Delete, Retire, ExternalEffect
```

`Effect` still exists and is still importable — it is now the union
`Upsert | Update | Delete | Retire`, so a `-> list[Effect]` annotation on your
handler needs no change. `RowEffect` is the shared base carrying `model_label`
and `lookup`; both are now **required**, where they were `None`-defaulted before.
`EffectOp` is gone.

### The five ops, before and after

```diff
-Effect(op="update_or_create", model_label="app.Room", lookup={"id": 5},
-       defaults={"name": "general"}, produces="room")
+Upsert(model_label="app.Room", lookup={"id": 5},
+       defaults={"name": "general"}, produces="room")

-Effect(op="update", model_label="app.Order", lookup={"ref": "A"},
-       defaults={"bonus": 50})
+Update(model_label="app.Order", lookup={"ref": "A"}, defaults={"bonus": 50})

-Effect(op="delete", model_label="app.Child", lookup={"parent_id": 7},
-       exclude={"idx__in": [0, 1]})
+Delete(model_label="app.Child", lookup={"parent_id": 7},
+       spare=Exclude({"idx__in": [0, 1]}))

-Effect(op="delete", model_label="app.Alert", lookup={"stream_key": "s"},
-       spare_keys=[{"alert_type": "ff4"}])
+Delete(model_label="app.Alert", lookup={"stream_key": "s"},
+       spare=SpareKeys([{"alert_type": "ff4"}]))

-Effect(op="retire", model_label="app.Alert", lookup={"stream_key": "s"},
-       patch={"resolved_at": ts}, spare_keys=keys,
-       transition_kind="alert_resolved",
-       transition_key_fields=("stream_key", "alert_type"))
+Retire(model_label="app.Alert", lookup={"stream_key": "s"},
+       patch={"resolved_at": ts}, spare=SpareKeys(keys),
+       transition=Transition(kind="alert_resolved",
+                             key_fields=("stream_key", "alert_type")))

-Effect(op="external", kind="email", payload={"to": "x@y.z"})
+ExternalEffect(kind="email", payload={"to": "x@y.z"})
```

**If you use only `update_or_create`, `update` and `delete`** — which is the
shape of the one adopter we know of — the migration is purely mechanical: drop
the `op=`, rename the constructor, and wrap a delete's `exclude=` in
`spare=Exclude(...)`. Nothing else changes: the field names, the values, and the
resulting rows are identical. `Exclude` and `SpareKeys` are the two shapes a
delete's single `spare` field can take; they were always alternatives, and a rule
forbidding both at once is now simply unwritable.

`Transition` is the only place a runtime check survives: it rejects empty
`key_fields`. Everything the other four checks used to catch is now a type error,
so run `pyright` (or your checker of choice) over your handlers once after
migrating and it will find every site.

If you construct effects through `reconcile_children`, `reconcile_by_key`,
`reconcile_tree`, `reconcile_aggregate`, `project_latest` or `history_effects`,
those helpers are unchanged — they now return the new types, and you only need to
adjust code that *inspects* what they returned.

### `external` effects are no longer effects at all

An `ExternalEffect` shares none of the row fields, no executor ever applied one,
and replay filtered them out before the executor anyway. It has left the family:

- `ReplayResult.external_effects_skipped` (a count) is replaced by
  **`ReplayResult.external: list[ExternalEffect]`** — the effects themselves, in
  order, including the ones a notifying `Retire` produced.
- **`include_external=` is gone** from `replay()`, `merge_replay()` and
  `django_rakaia.replay.replay_stream`, and `--include-external` is gone from
  `manage.py replay`. It no longer means anything: no executor receives an
  external effect under any setting.

```diff
 result = replay(store, "orders", executor)
-if result.external_effects_skipped:
-    ...
+for effect in result.external:
+    send(effect.kind, effect.payload)
```

Replay still never delivers anything by itself, so a rebuild cannot re-send last
year's receipts — but the caller now gets enough to deliver them deliberately,
which a bare count never was. **If your executor has an `op == "external"`
branch, delete it**; it is unreachable.

### `diff_effects_against_rows(ops=...)` is now `kinds=`

The parameter selected which ops to verify by string. It now takes effect
*classes* and defaults to `(Upsert, Update)` — the two that carry `defaults`:

```diff
-diff_effects_against_rows(effects, ops=("update_or_create",))
+diff_effects_against_rows(effects, kinds=(Upsert,))
```

Callers using the default — which is all known callers — need no change.

---

## `rakaia.types.Stream` no longer carries `messages`

`Stream` is the metadata a store's `get()` returns. It had a `messages` list,
which the durable store documented as permanently empty — so the field was
already a lie on one of the two backends. Where the messages actually live is now
the store's own business.

```diff
-messages = store.get(path).messages
+messages, _ = store.read(path)
```

Nothing outside the in-memory store's own internals read it, so most consumers
have nothing to do.

---

## Behaviour worth re-checking after upgrading

These are not API breaks — nothing to edit — but each changes what your code
*does*, and each is easy to have been relying on:

- **`RAKAIA_STORE` no longer falls back to memory.** An unrecognised backend
  raises `ImproperlyConfigured` instead of silently selecting the in-memory
  store. If a typo has been hiding in your settings, this is where you find out.
  `manage.py check` reports it as `rakaia.E001`.
- **`diff_effects_against_rows(...).raise_if_diff()` now refuses an empty
  population**, raising `VacuousVerification`. A verification sweep that compared
  nothing used to report itself clean. If a proof of yours is expected to run
  against zero effects, say so with `raise_if_diff(allow_empty=True)`.
- **`@stream_model` now records ambient provenance and `event_ts`.** Events it
  writes carry `metadata` and a logical timestamp where they previously carried
  `{}` and `NULL`. If you were compensating for the empty actor field — for
  instance by reading the owner FK yourself — that workaround is now redundant
  and may disagree with the recorded actor.
- **`append()` honours producer options on the durable store.** It previously
  ignored `producer_id`/`producer_epoch`/`producer_seq` entirely. Fenced writes
  through `append` are now refused with a producer result where they used to be
  accepted unconditionally.
- **Bulk appends now reach SSE subscribers.** `append_many` was invisible to the
  channel layer. If you worked around that by also sending your own frame, you
  will now get two.
- **A refused append to a closed stream now always carries a producer result.**
  `AppendResult.producer_result` was `None` for anything but a retry of the
  closing tuple; it is now `ProducerStreamClosed`. The HTTP response is
  unchanged — the server already synthesised that result — so this only affects
  code calling `store.append()` directly and testing the field for `None`.
- **The dashboard views refuse a stream position they did not issue.** The JSON
  events endpoint and the SSE `Last-Event-ID` header now answer `400` for an
  offset in the other store's format, where they used to resolve it to an
  unrelated position and answer `200`. An absent `Last-Event-ID` still means a
  fresh connection. If a client of yours was passing a compound
  `{seq}_{byte}` offset to the durable dashboard, it was already getting the
  wrong window and will now be told so.
- **Timestamps now compare equal to the column they were encoded from.**
  `DEFAULT_NORMALIZERS` gained `normalize_temporal`, so a `DateTimeField`,
  `DateField` or `TimeField` no longer reports a difference on every replay.
  Two consequences worth checking: a replay with
  `DjangoExecutor(skip_unchanged=True)` stops re-`UPDATE`ing rows it used to
  rewrite every time — so `auto_now` columns, `post_save` receivers and
  replication go quiet for those rows — and a difference smaller than a
  millisecond now reads as unchanged, because that is all the event log can
  carry. If you pass your own `normalizers=` set, add `normalize_temporal` to
  it; the default set is only used when you pass nothing.
