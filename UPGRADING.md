---
icon: lucide/arrow-up-circle
---

# Upgrading

Breaking changes, and what to do about each. Rakaia is pre-1.0 and the version
number has not moved yet, so this file is organised by the change rather than by
release — find the ones you are crossing and apply them in order.

If you pin rakaia to a git revision (which every current consumer does, since
`0.1.0` predates most of what follows), the relevant question is *which
revision*, not which version.

---

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

`StreamMessage.seq` and `Stream.last_seq` are now `int | None` where they were
`str | None`. The header is parsed strictly, which is what fixed `Stream-Seq: 10`
being rejected after `9` because `"10" < "9"` as text.

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
