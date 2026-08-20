# Polyglot — live-editable translations over SSE

A marketing landing page where every visible string is a `Translatable`
row. The right pane is a translation editor; saving a string broadcasts
a StreamEvent over `translations:{langcode}` and every browser pinned to
that language updates in place.

## Run

```sh
just polyglot-dev          # http://localhost:8001
```

Open two tabs at `/?lang=tet`, edit a string in the right pane and
click **Save** (or press Enter), then watch the left pane of the other
tab update without a refresh. The Save button is disabled until you
change the value, so the translation stream only carries deliberate
updates — no autosave noise. Switch languages with the dropdown in the
editor header.

## Stress it

One human editing one string shows the wiring is connected. To see what
happens when events arrive faster than a browser can paint:

```sh
uv run python manage.py stress_translations           # 1000 scrambles, flat out
uv run python manage.py stress_translations --delay 0.25 --lang pt
```

Every save scrambles the letters of a random string, fires a `post_save`,
and pushes an event down every open EventSource. Leave a tab open at
`/?lang=tet` and watch the left pane thrash. Originals are restored when
the run ends (or on Ctrl-C) unless you pass `--no-restore`.

The saves are POSTed to the running server, the same request the Save
button makes — deliberately, because live delivery goes through the
channel layer and `just polyglot-dev` uses `InMemoryChannelLayer`, which
is in-memory to *one process*. `--direct` writes via the ORM with no
server running; the events are still durable and replay on the next
connect, but nothing arrives live under an in-memory layer.

### What limits it

Numbers below are 400 `--direct` saves on one laptop — indicative, not a
benchmark. Each save is ~10 queries: the row `UPDATE`, then an event, an
offset allocation and a stream entry.

| | 1 writer | 12 writers, 1 stream | 12 writers, 3 streams |
|---|---|---|---|
| SQLite, stock pragmas | 41/s | *"database is locked"* | *"database is locked"* |
| SQLite, WAL (what this app now uses) | 374/s | 220/s | 241/s |
| Postgres 16 (container, loopback) | 85/s | 176/s | 363/s |

Three things that turned out to matter, in order:

1. **SQLite's default `journal_mode=delete` + `synchronous=FULL`.** Two
   fsyncs per save — the row `UPDATE` autocommits, then the event append
   opens its own transaction — for ~24ms of the ~26ms. WAL plus
   `synchronous=NORMAL` is a 9x win and is now set in `settings.py`.
2. **Concurrent writers used to fail outright**, instantly, not after
   `timeout`: a deferred transaction that reads before it writes cannot
   upgrade to a write lock, and SQLite does not retry the upgrade. Fixed
   with `transaction_mode: "IMMEDIATE"`.
3. **Appends to one stream path serialize on that path's offset
   high-water**, by design — offsets per path are a gapless ordered
   sequence, so allocating one takes a row lock. On Postgres that is the
   *only* thing serializing, which is why spreading the same 12 writers
   across three langcodes doubles throughput. On SQLite it is moot: one
   writer at a time for the whole file, so splitting streams buys nothing.

So Postgres is worse at one writer (85/s — ten network round trips beat
ten in-process ones) and better the moment there is more than one, with
the ceiling set by how many distinct stream paths you are writing to.
To try it:

```sh
just pg-up
export POLYGLOT_DB=postgres PGHOST=127.0.0.1 PGPORT=55432 \
       PGUSER=postgres PGPASSWORD=postgres PGDATABASE=postgres
uv run python manage.py migrate
uv run python manage.py stress_translations --direct -c 8
```

The HTTP path (the default, no `--direct`) tops out around 137/s and is
flat in `--concurrency`: `urllib` opens a fresh connection per request,
so that number is the harness, not the server. For the demo it is
irrelevant — a browser cannot paint 137 updates/s anyway, which is
rather the point of running the stress command with a tab open.

## How it works

* `polyglot/strings.py` — the catalog of msgids the page renders, plus
  default translations per language. The view seeds rows on first hit.
* `polyglot/signals.py` — `post_save` / `post_delete` on `Translatable`
  call `create_stream_event(stream_paths=[f"translations:{langcode}"], …)`.
  No decorator on the library model — the demo wires its own signal so
  the library stays unopinionated.
* `polyglot/templates/polyglot/landing.html` — split layout, EventSource
  subscribed to the per-langcode stream, DOM updates by `data-msgid`.

## Production-style run

```sh
just polyglot-serve workers=4
```

Uses `polyglot_project.settings_prod` (DEBUG=False, channels-redis,
WhiteNoise CompressedManifest). Redis is started via podman by the
`redis-up` recipe.
