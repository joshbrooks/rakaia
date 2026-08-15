---
icon: lucide/languages
---

# Translations (example)

!!! note "This moved out of the library"

    `django_rakaia` used to ship a `Translatable` model, admin, HTMX dashboard
    and SSE endpoint. It was demo domain rather than library surface — its
    language choices were even hard-coded to `tet`/`pt`/`id` — and because the
    model sat in `0001_initial`, **every** consumer got a translations table in
    their database whether or not they ever used it.

    It now lives in [`examples/polyglot`](https://github.com/joshbrooks/rakaia/tree/main/examples/polyglot),
    which was the only thing that ever used it. If you were relying on the
    library model, see [`UPGRADING.md`](https://github.com/joshbrooks/rakaia/blob/main/UPGRADING.md)
    — it carries the model definition to paste into your own app, and the
    migration note.

## What the example shows

`polyglot` is a live-editable translations UI, and it is a good small
demonstration of the part rakaia actually provides: **one row changes, every
connected browser sees it**, without polling.

- A `post_save` receiver calls `create_stream_event` to fan the change out to a
  per-language stream (`translations:tet`, `translations:pt`, …), so a client
  subscribes to just the language it is showing.
- The SSE endpoint delivers those events to the browser.
- The model itself mirrors the `gettext` family (`gettext`, `ngettext`,
  `pgettext`, `npgettext`) with a database-backed manager, so a translation can
  be edited without a deploy.

Run it:

```bash
just polyglot-dev   # http://localhost:8001
```

The rakaia concepts it exercises are covered in
[Django integration](django-integration.md) and
[the event envelope](event-envelope.md). Nothing in the example needs anything
translations-specific from the library.
