# Translations

`django_rakaia` ships with an optional `Translatable` model providing a
database-backed alternative to gettext. It's useful when translations need
to be edited live in production (e.g. by non-engineer translators) instead
of being committed to `.po` files.

## Model

```python
from django_rakaia.models import Translatable
```

Fields:

| Field      | Description                                          |
|------------|------------------------------------------------------|
| `msgid`    | Original message identifier.                         |
| `msgstr`   | Translated message (nullable).                       |
| `langcode` | Destination language code (e.g. `tet`, `pt`, `id`).  |
| `domain`   | Optional domain.                                     |
| `msgctxt`  | Optional context (for `pgettext`-style lookups).     |
| `deleted`  | Soft-delete timestamp; populated by `soft_delete()`. |

## Lookup API

`Translatable.objects` mirrors the gettext API:

```python
Translatable.objects.gettext("Hello", langcode="tet")
Translatable.objects.ngettext("apple", "apples", number=5, langcode="tet")
Translatable.objects.pgettext("button", "Save", langcode="tet")
Translatable.objects.npgettext("food", "apple", "apples", number=5, langcode="tet")
```

If no record matches, the lookup methods emit a `warnings.warn(...)` and
return `None`/the original string, depending on the variant.

## Soft delete

```python
t = Translatable.objects.get(pk=1)
t.soft_delete()  # sets t.deleted = timezone.now()
t.restore()      # clears t.deleted
```

## Emitting stream events from translations

Translatable does not automatically emit events out of the box. If you want
each save/delete to broadcast to a translation stream, decorate it (or wire
up a signal) with `@stream_model`:

```python
from dataclasses import dataclass
from django_rakaia.decorators import create_stream_event
from django_rakaia.models import Translatable
from django.db.models.signals import post_save
from django.dispatch import receiver


@dataclass
class TranslatableData:
    id: int
    msgid: str
    msgstr: str | None
    langcode: str


@receiver(post_save, sender=Translatable)
def broadcast_translation(sender, instance, created, **kwargs):
    create_stream_event(
        stream_paths=[
            f"translations:lang:{instance.langcode}",
            "translations:all",
        ],
        to_dataclass=lambda obj: TranslatableData(
            id=obj.id,
            msgid=obj.msgid,
            msgstr=obj.msgstr,
            langcode=obj.langcode,
        ),
        instance=instance,
        action="create" if created else "update",
    )
```

The bundled SSE endpoint `/streams/api/translations/sse/` subscribes to a
Channels group named `translations`. The signal handler in
`django_rakaia.channels_signals` already broadcasts to that group whenever a
`StreamEvent` is saved with a `translatable_id` key in `data`.

## Translations dashboard UI

The dashboard at `/streams/translations/` is a server-rendered Django
template using [HTMX](https://htmx.org/) — no bundler, no `node_modules`,
no JavaScript framework. The page loads two `<script>` tags from a CDN
(`htmx.org` and the `htmx-ext-sse` extension) and uses HTMX attributes for
all interactivity.

### How the pieces fit together

| Concern | Implementation |
|---------|----------------|
| Filtered table | `<tbody>` with `hx-get` to `translations_table_htmx`, debounced via `hx-trigger="input changed delay:300ms"` |
| Create / update | `<form>` with `hx-post` to `translation_create_htmx`, swaps the refreshed `<tbody>` back in |
| Live activity feed | `<div hx-ext="sse" sse-connect="…" sse-swap="activity">` — **zero JavaScript** |
| CSRF | `hx-headers='{"X-CSRFToken": "{{ csrf_token }}"}'` on the wrapping container |

### Server endpoints

| URL | View | Returns |
|-----|------|---------|
| `/streams/api/translations/htmx/table/` | `translations_table_htmx` | `<tr>` rows for the table body |
| `/streams/api/translations/htmx/create/` | `translation_create_htmx` | refreshed `<tbody>` after save |
| `/streams/api/translations/htmx/sse/` | `translation_activity_sse_html` | SSE stream of `<div class="activity-item">` fragments framed as `event: activity` |

The HTML SSE endpoint subscribes to the `translations` channel layer group
and renders each broadcast through the `_activity_item.html` partial. The
`translation_create_htmx` view writes a `StreamEvent` after saving so the
existing `channels_signals` handler broadcasts to the group.

If you also need a JSON SSE feed for non-browser clients, the legacy
endpoint at `/streams/api/translations/sse/` (`translation_streams_sse`) is
still wired up.

### Customizing the look

The template inlines a small `<style>` block. Override it by extending
`django_rakaia/translations_index.html` in your project's templates
directory or by adding a custom stylesheet through the parent `base.html`.
