# Django Integration

`django_rakaia` is a Django app that:

1. Stores stream events in your database via three normalized models
   (`Stream`, `StreamEvent`, `StreamEntry`).
2. Lets your own Django models emit stream events automatically with the
   `@stream_model` decorator.
3. Broadcasts changes to connected SSE clients through Django Channels.
4. Provides a Django admin interface for browsing streams and events.

## Setup

```python
# settings.py
INSTALLED_APPS = [
    "daphne",            # ASGI server (must come before django.contrib.staticfiles)
    "channels",
    "django_rakaia",
    # ...
]

ASGI_APPLICATION = "myproject.asgi.application"

CHANNEL_LAYERS = {
    "default": {
        "BACKEND": "channels.layers.InMemoryChannelLayer",
    },
}
```

```python
# urls.py
from django.urls import include, path

urlpatterns = [
    path("streams/", include("django_rakaia.urls", namespace="django_rakaia")),
    path("protocol/", include("django_rakaia.protocol_views", namespace="protocol")),
]
```

Run migrations:

```bash
python manage.py migrate django_rakaia
```

## Data model

Three normalized tables work together:

| Model         | Purpose                                                  | Table                |
|---------------|----------------------------------------------------------|----------------------|
| `Stream`      | A logical stream identified by `stream_id` (string).     | `rakaia_stream`      |
| `StreamEvent` | One piece of event data (JSON), reusable across streams. | `rakaia_streamevent` |
| `StreamEntry` | Junction row giving an event a monotonic offset within a single stream. | `rakaia_streamentry` |

A single `StreamEvent` can appear in many streams via multiple `StreamEntry`
rows — useful when one logical change is interesting to several subscribers
(e.g. a project event belongs to both `user:42:projects` and
`area:7:projects`).

## The `@stream_model` decorator

The decorator is the easiest way to make your models emit events. Wrap any
Django model and supply two callables:

- `stream_paths` — a string, list of strings, or callable returning either,
  identifying which streams the event belongs to.
- `to_dataclass` — a callable converting an instance into a dataclass; the
  dataclass is serialized into the event payload.

```python
from dataclasses import dataclass
from django.db import models
from django_rakaia.decorators import stream_model


@dataclass
class ProjectData:
    id: int
    name: str
    area_id: int
    created_by_id: int


@stream_model(
    stream_paths=lambda obj: [
        f"user:{obj.created_by_id}:projects",
        f"area:{obj.area_id}:projects",
    ],
    to_dataclass=lambda obj: ProjectData(
        id=obj.id,
        name=obj.name,
        area_id=obj.area_id,
        created_by_id=obj.created_by_id,
    ),
)
class Project(models.Model):
    name = models.CharField(max_length=100)
    area = models.ForeignKey("Area", on_delete=models.CASCADE)
    created_by = models.ForeignKey("auth.User", on_delete=models.CASCADE)
```

Every `Project.save()` emits a single `StreamEvent` plus one `StreamEntry`
per stream returned by `stream_paths`. Deletes emit an event with
`event_type="delete"`.

## Manual event creation

If you can't decorate the model (e.g. it's `auth.User`), call
`create_stream_event` from a signal handler:

```python
from dataclasses import dataclass
from django.contrib.auth import get_user_model
from django.db.models.signals import post_save
from django.dispatch import receiver
from django_rakaia.decorators import create_stream_event


@dataclass
class UserData:
    id: int
    username: str


User = get_user_model()


@receiver(post_save, sender=User)
def emit_user_event(sender, instance, created, **kwargs):
    create_stream_event(
        stream_paths=f"user:{instance.id}:activity",
        to_dataclass=lambda obj: UserData(id=obj.id, username=obj.username),
        instance=instance,
        action="create" if created else "update",
    )
```

## Real-time SSE

When a `StreamEntry` is saved, `django_rakaia.channels_signals` broadcasts
the event to a Django Channels group named after the stream. Two SSE views
subscribe to those groups:

| URL                                            | Purpose                              |
|------------------------------------------------|--------------------------------------|
| `/streams/api/streams/<stream_id>/sse/`        | Live events for one stream.          |
| `/streams/api/translations/sse/`               | Live translation events (see [translations.md](translations.md)). |

Consume them from JavaScript:

```js
const es = new EventSource("/streams/api/streams/room:42:messages/sse/");
es.onmessage = (msg) => {
    const event = JSON.parse(msg.data);
    console.log("got event", event);
};
```

The SSE handler first sends the existing entries (catch-up), then streams
new events as they arrive via the channel layer.

## Protocol HTTP API

`django_rakaia.protocol_views` exposes the full Durable Streams HTTP API
backed by the database models. Mount it under any prefix:

```python
path("protocol/", include("django_rakaia.protocol_views", namespace="protocol")),
```

| Method | URL                                                  |
|--------|------------------------------------------------------|
| PUT    | `/protocol/streams/<stream_path>/create`             |
| POST   | `/protocol/streams/<stream_path>/append`             |
| GET    | `/protocol/streams/<stream_path>/read?offset=...`    |
| GET    | `/protocol/streams/<stream_path>/sse?cursor=...`     |
| HEAD   | `/protocol/streams/<stream_path>/metadata`           |

The semantics follow the [protocol specification](protocol.md).

## Admin

`django_rakaia.admin` registers `Stream`, `StreamEvent`, `StreamEntry`, and
`Translatable` with the Django admin so you can browse stored streams and
inspect event payloads. To register your own concrete `StreamEvent` subclass
in the admin, call:

```python
from django_rakaia.admin import register_stream_event_admin
from myapp.models import MyStreamEvent

register_stream_event_admin(MyStreamEvent)
```
