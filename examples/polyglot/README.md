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
