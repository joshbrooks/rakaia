# Polyglot — live-editable translations over SSE

A marketing landing page where every visible string is a `Translatable`
row. The right pane is a translation editor; saving a string appends a
JSON record to `/translations/{langcode}` and every browser pinned to
that language updates in place.

The log is not in the database. It is a folder of ordinary JSON-lines
files under `examples/polyglot/streams/`, which you can `tail -f` while
the demo runs.

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

The saves are POSTed to the running server by default, the same request
the Save button makes. `--direct` writes via the ORM instead, from a
process of its own — and open browsers still update, because readers tail
the log through the filesystem rather than waiting on a channel layer.
That used to be the one thing `--direct` could not do.

### What limits it

Numbers below are 400 `--direct` saves on one laptop — indicative, not a
benchmark. Each save is now two writes to two places: the `Translatable`
row `UPDATE` in SQLite, and one line appended to a file.

| | 1 writer | 12 writers, 1 stream |
|---|---|---|
| log on disk, fsync on (the default) | 73/s | 84/s |
| log on disk, fsync off | 195/s | — |
| log on `/dev/shm`, fsync off | 242/s | 232/s |

Two things about that shape:

1. **Concurrency buys almost nothing.** Writers to one stream take an
   exclusive `flock` on that stream's directory for the whole
   check-then-write, so appends to a path serialize — the same property
   the database-backed store gets from `select_for_update()` on the
   stream row. Spreading writers across langcodes would help; twelve
   writers on one will not.
2. **The default is slower than the database-backed store was**, which
   managed 374/s here. That is not a like-for-like comparison: SQLite in
   WAL with `synchronous=NORMAL` does not fsync on commit, so those
   events survived a crashed process but not a power cut. This store
   fsyncs every append by default and so survives both. Turn it off with
   `POLYGLOT_STREAM_FSYNC=0` and the gap mostly closes.

To move the log somewhere else, or into memory:

```sh
POLYGLOT_STREAM_ROOT=/dev/shm/polyglot POLYGLOT_STREAM_FSYNC=0 just polyglot-dev
```

`POLYGLOT_DB=postgres` still switches the database (see `settings.py`),
but it now only moves the `Translatable` row — the log stays in files
wherever `POLYGLOT_STREAM_ROOT` points.

The HTTP path (the default, no `--direct`) tops out around 137/s and is
flat in `--concurrency`: `urllib` opens a fresh connection per request,
so that number is the harness, not the server. For the demo it is
irrelevant — a browser cannot paint 137 updates/s anyway, which is
rather the point of running the stress command with a tab open.

## How it works

* `polyglot/strings.py` — the catalog of msgids the page renders, plus
  default translations per language. The view seeds rows on first hit.
* `polyglot/signals.py` — `post_save` / `post_delete` on `Translatable`
  append a JSON record to `/translations/{langcode}` through
  `get_store()`, which `RAKAIA_STORE = "jsonl"` resolves to the
  file-backed store. Not `create_stream_event`: that writes `StreamEvent`
  rows directly and never consults the setting. No decorator on the
  library model either — the demo wires its own signal so the library
  stays unopinionated.
* `polyglot_project/asgi.py` — Django, plus rakaia's protocol server
  mounted on `/protocol/` over the same folder. This is what serves SSE.
  Django's own SSE endpoint replays from `StreamEntry` rows and waits on
  the channel layer, and neither exists here; the protocol server reads
  through the store interface and blocks in `wait_for_messages`, which
  the file-backed store answers by watching the directory.
* `polyglot/templates/polyglot/landing.html` — split layout, an
  `EventSource` on `/protocol/translations/{lang}?live=sse&offset=0`,
  DOM updates by `data-msgid`. The frames are the protocol server's
  named `data` / `control` pair, not the single unnamed frame Django's
  endpoint sends.

## Production-style run

```sh
just polyglot-serve workers=4
```

Uses `polyglot_project.settings_prod` (DEBUG=False, WhiteNoise
CompressedManifest). Four workers all serve live updates from one log
with no message broker between them: writers coordinate with `flock` on
the stream directory and readers tail the files. Nothing here needs
Redis, and the recipe no longer starts one.

That works because every worker is on one machine. `flock` is not
dependable over NFS or another network filesystem, so this arrangement
does not stretch across hosts — that is what the database-backed store
is for.
