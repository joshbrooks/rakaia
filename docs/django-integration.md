# Django Integration

`django_rakaia` is a Django app that:

1. Stores stream events in your database via three normalized models
   (`Stream`, `StreamEvent`, `StreamEntry`).
2. Lets your own Django models emit stream events automatically with the
   `@stream_model` decorator.
3. Broadcasts changes to connected SSE clients through Django Channels.
4. Provides a Django admin interface for browsing streams and events.

!!! tip "Which parts actually need Django (or Channels)?"
    Everything in the `rakaia` package is stdlib-only, and within `django_rakaia`
    only the SSE broadcast needs `channels`. See
    [Framework vs. protocol server](framework-vs-protocol-server.md) for the full
    "what needs Django / what is pure" matrix and the `RAKAIA_ENABLE_SSE` gate.

## Setup

```python
# settings.py
INSTALLED_APPS = [
    "daphne",  # ASGI server (must come before django.contrib.staticfiles)
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

## Adopting the durable store

Rakaia's default store is the in-memory `StreamStore` — fast and dependency-free,
but **process-local**: the event log lives in memory and vanishes on restart.
That's fine for the protocol server and for demos, but event sourcing needs the
log to survive. If you emit an event from a web request and want to *replay* that
stream later — in a worker, a management command, another process — the events
have to be persisted.

`DjangoStreamStore` is that durable backend. It implements the same read/emit
surface on top of the `Stream` / `StreamEvent` / `StreamEntry` models above, so
everything downstream (replay, handlers, `manage.py replay`) works identically —
only now the stream is in your database.

### Selecting the store

The store is chosen by the `RAKAIA_STORE` Django setting via
`django_rakaia.store.get_store()`:

```python
# settings.py
RAKAIA_STORE = "durable"  # DjangoStreamStore (persisted in the DB)
# RAKAIA_STORE = "memory"  # StreamStore (default — in-memory, process-local)
```

Both `manage.py replay` and the mounted protocol app resolve their store through
`get_store()`, so flipping this one setting switches the whole integration over.
The chosen store is cached one-per-backend for the process.

Only those two values are accepted. Anything else — a typo like `"durrable"`, a
stray space, a plausible guess like `"postgres"` — raises `ImproperlyConfigured`
rather than falling back. It used to fall back to the in-memory store, which made
a one-character mistake indistinguishable from a working durable deployment:
appends succeeded, nothing complained, and the whole log vanished on the next
restart. `manage.py check` reports the same problem at startup as `rakaia.E001`,
so a misconfigured deployment fails before it serves a request. Running
`"memory"` with `DEBUG = False` is allowed but warns (`rakaia.W001`) — silence it
via `SILENCED_SYSTEM_CHECKS` if that is deliberate.

| | `"memory"` (default) | `"durable"` |
|---|---|---|
| Backend | `rakaia.StreamStore` | `DjangoStreamStore` |
| Persistence | In memory, lost on restart | In your database |
| Survives across processes | No | Yes |
| `manage.py replay <stream>` | Only within the emitting process | Yes, any process |
| Live-protocol extras (long-poll, producer epochs, stream close) | Yes | Yes — the full `StreamServerStore` surface |

### Migration path (in-memory → durable)

1. **Set `RAKAIA_STORE = "durable"`** and run `python manage.py migrate
   django_rakaia` so the stream tables exist.
2. **Emit into the stream** from your model writes — e.g. from
   `Submission.save()` — so the event log accumulates durably rather than only
   in the process that produced it. The [`@stream_model` decorator](#the-stream_model-decorator)
   and [`create_stream_event`](#manual-event-creation) both write through the
   configured store.
3. **Verify before cutting over.** Replay the durable stream with a
   `CollectingExecutor` and diff the recorded effects against your existing rows
   to confirm they reproduce current state — see [Dry-run & executors](dry-run-and-executors.md).
4. **Replay for real** with `python manage.py replay <stream>`, now that the log
   persists across processes.

The [`formkit_submissions`](../examples/formkit_submissions/) example walks this
exact adoption story — driving a `formkit-ninja` pipeline from a durable stream
and proving the projected rows are identical to the current direct write.

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
from django_rakaia import stream_model


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

Saves made with `raw=True` — `manage.py loaddata`, test databases restored via
`serialized_rollback=True` — are **ignored**. Fixture rows are replayed history,
not new facts; appending them would inflate the stream on every restore, and a
raw instance can reference foreign-key rows that have not been loaded yet.

### Payload types

`StreamEvent.data` is encoded with `DjangoJSONEncoder`, so a payload may carry
`UUID`, `datetime`/`date`, and `Decimal` values straight off the model — they
land as strings. No pre-stringifying in the transformer; a `TypeError` at insert
time would otherwise be raised from inside `post_save` and take down the save
being audited.

Datetimes are the one lossy case. `DjangoJSONEncoder` truncates — does not round
— to millisecond precision, and emits no fractional part at all when
`microsecond` is 0:

| model value | payload string |
| --- | --- |
| `…12:30:00.123456+00:00` | `"2026-08-09T12:30:00.123Z"` |
| `…12:30:00.123999+00:00` | `"2026-08-09T12:30:00.123Z"` |
| `…12:30:00.000000+00:00` | `"2026-08-09T12:30:00Z"` |

Django stores microseconds, so a payload timestamp will **not** compare equal to
the column it came from. That matters in two places: comparing a payload field
against a source row, and `merge_replay(order_key=...)` pointed at a payload
datetime, where two events inside the same millisecond now tie and fall through
to the `(stream_path, offset)` tiebreak. Prefer the `ENVELOPE_TS` order key,
which reads the `event_ts` column instead. If you need full precision in the
payload, format the value explicitly in `to_dataclass`.

### Soft-delete models

`pgtrigger.SoftDelete` rewrites a `DELETE` into `UPDATE is_active=false`, so the
row survives — but Django still fires `post_delete`. The default behaviour would
record a hard delete that never happened, snapshotting the stale pre-delete
state. Emit the update that actually occurred, with a payload describing the
state the row ended up in rather than the one Django hands `post_delete`:

```python
@stream_model(
    stream_paths=...,
    to_dataclass=to_node_data,
    on_delete="update",
    delete_to_dataclass=lambda obj: NodeData(id=obj.id, is_active=False),
)
class Node(models.Model): ...
```

> **Do not reach for `on_delete=None` here.** The `is_active` flip is performed
> by a `BEFORE DELETE` trigger *inside the database* — the trigger returns
> `NULL`, cancelling the row delete. Django only ever issued a `DELETE`, so it
> fires `post_delete` and **never** `post_save`. Suppressing the delete receiver
> therefore drops the soft delete from the stream entirely, and a projection
> replayed from that stream shows the node active forever — strictly worse than
> the phantom hard delete.

`on_delete=None` is the right choice for a different shape: a model that
soft-deletes in *Python* (a `delete()` override that calls `save()`, so the flip
already arrives as an ordinary `post_save` update), or one whose deletes are
simply not worth streaming.

`on_delete` accepts `"delete"` (the default), `"update"`, or `None` (register no
`post_delete` receiver at all). `delete_to_dataclass` replaces `to_dataclass`
for the delete signal only, and requires `on_delete` to be set.

## Manual event creation

If you can't decorate the model (e.g. it's `auth.User`), call
`create_stream_event` from a signal handler:

```python
from dataclasses import dataclass
from django.contrib.auth import get_user_model
from django.db.models.signals import post_save
from django.dispatch import receiver
from django_rakaia import create_stream_event


@dataclass
class UserData:
    id: int
    username: str


User = get_user_model()


@receiver(post_save, sender=User)
def emit_user_event(sender, instance, created, **kwargs):
    if kwargs.get("raw"):  # fixture load — replayed history, not a new fact
        return
    create_stream_event(
        stream_paths=f"user:{instance.id}:activity",
        to_dataclass=lambda obj: UserData(id=obj.id, username=obj.username),
        instance=instance,
        action="create" if created else "update",
    )
```

The `raw` guard is **yours to write here.** `@stream_model` applies it for you;
a hand-wired receiver does not get it, and `create_stream_event` never sees the
signal kwargs. Without the guard this handler appends a phantom event per
fixture row on every `loaddata` — and `auth.User`, the model that drives you to
the manual path in the first place, is the one most likely to be in a dump.

## Real-time SSE

When a `StreamEntry` is saved, `django_rakaia.channels_signals` broadcasts
the event to a Django Channels group named after the stream. Two SSE views
subscribe to those groups:

| URL                                            | Purpose                              |
|------------------------------------------------|--------------------------------------|
| `/streams/api/streams/<stream_id>/sse/`        | Live events for one stream. Needs a signed-in user, like every dashboard page. |

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

Serve the Durable Streams protocol off the database by mounting rakaia's ASGI
application over the durable store. It is an ASGI app, not a Django view, so it
is mounted in `asgi.py` rather than in `urls.py`:

```python
# asgi.py
from django.core.asgi import get_asgi_application
from django_rakaia import get_asgi_app

django_app = get_asgi_application()
protocol_app = get_asgi_app()  # create_app() over the configured store


async def application(scope, receive, send):
    if scope["type"] == "http" and scope["path"].startswith("/protocol/"):
        scope = {**scope, "path": scope["path"][len("/protocol") :]}
        return await protocol_app(scope, receive, send)
    return await django_app(scope, receive, send)
```

Set `RAKAIA_STORE = "durable"` so `get_asgi_app()` picks up `DjangoStreamStore`.

Routing is by **HTTP method on the stream path**, as the protocol specifies:

| Method | URL                              | Meaning                     |
|--------|----------------------------------|-----------------------------|
| PUT    | `/protocol/<stream_path>`        | create                      |
| POST   | `/protocol/<stream_path>`        | append (or close)           |
| GET    | `/protocol/<stream_path>?offset=`| read, long-poll, or SSE     |
| HEAD   | `/protocol/<stream_path>`        | metadata                    |
| DELETE | `/protocol/<stream_path>`        | delete ([refusable](#permanent-streams)) |

This is the same implementation the standalone server runs, so producer
epoch/seq fencing, close, TTL, long-poll, ETag/304 and CORS all behave
identically on either store.

A catch-up read returns at most a page of messages — a thousand unless
`RAKAIA_READ_PAGE_SIZE` says otherwise, and `None` turns paging off. A page that
stops short leaves out `Stream-Up-To-Date` and sets `Stream-Next-Offset` to its
last message, so a client that follows the protocol reads on from there without
knowing a page size exists. The setting is read when `get_asgi_app()` is called
without `options`; pass `ServerOptions(read_page_size=...)` to set it yourself.

### Protect the protocol app

> **Warning.** The mount above bypasses Django's middleware, so sessions,
> `request.user` and `login_required` do not apply to it. Out of the box anyone who
> can reach the URL can create, append to, read and delete any stream, from any
> website. Before you expose it, set `RAKAIA_AUTHORIZE` and `RAKAIA_ALLOWED_ORIGINS`.

`RAKAIA_AUTHORIZE` is a function (or its dotted path) called with the ASGI scope
and the action — `"create"`, `"append"`, `"read"` or `"delete"` — before the
request does anything. Return `None` to allow it, or a status such as `401` or
`403` to refuse it; `(status, b"message")` sets the body too. It may be `async`.
A browser's CORS preflight is never passed to it.

```python
# settings.py
RAKAIA_AUTHORIZE = "myproject.access.authorize"
RAKAIA_ALLOWED_ORIGINS = ["https://app.example.org"]  # default is ["*"]

# myproject/access.py
from django.conf import settings


def authorize(scope, action):
    # A token: scope["headers"] is a list of (bytes, bytes) pairs.
    token = dict(scope["headers"]).get(b"authorization")
    if token == f"Bearer {settings.PROTOCOL_TOKEN}".encode():
        return None
    # Or, for a browser session, wrap the app in Channels' AuthMiddlewareStack
    # in asgi.py (below) and read the user it puts in the scope.
    user = scope.get("user")
    if user is not None and user.is_authenticated:
        return None if action == "read" or user.is_staff else 403
    return 401
```

```python
# asgi.py — only needed for the session version
from channels.auth import AuthMiddlewareStack

protocol_app = AuthMiddlewareStack(get_asgi_app())
```

Other limits, all with defaults: a body over `RAKAIA_MAX_BODY_BYTES` (8 MiB;
`None` for no limit) is refused with `413`, and a live SSE connection is closed
after `RAKAIA_SSE_MAX_SECONDS` (60; `None` for never) so the client reconnects from
the last offset it saw. A closed connection also stops the server polling the
store for it. As with the page size, these settings are read when `get_asgi_app()`
is called without `options`; `ServerOptions(authorize=..., allowed_origins=...,
max_body_bytes=..., sse_max_seconds=...)` sets them directly.

> **Changed.** This used to be `django_rakaia.protocol_views`, a separate
> implementation with verb-in-path URLs (`/streams/x/append`), newline-delimited
> read responses, and no fencing, close, TTL or long-poll. It has been removed;
> see the changelog for the migration.

The semantics follow the [protocol specification](protocol.md).

## Permanent streams

By default a stream can be given an expiry, is removed once it runs out, and can
be deleted by any client with a protocol `DELETE`. If your streams are the
record of what happened, that is history disappearing without anyone deciding it
should. Turn it off with one setting:

```python
# settings.py
RAKAIA_PERMANENT_STREAMS = True  # off unless you set it
```

With it on, every stream is permanent, whichever store `RAKAIA_STORE` picks:

- **A create that asks for an expiry is refused.** Passing `ttl_seconds` or
  `expires_at` to `create()` raises `ExpiryNotAllowed`, and a protocol `PUT`
  carrying `Stream-TTL` or `Stream-Expires-At` gets `400 Bad Request`.
- **A protocol `DELETE` is refused** with `405 Method Not Allowed`. The stream
  and its events are untouched.
- **Nothing is removed for having expired.** A stream that was given an expiry
  before you turned the switch on is read, written and listed as if it had none.

**Deleting a stream from Python has to say so.** `store.delete(path)` is refused
with `DeleteNotAllowed` too, because a management command run by hand is a Python
call and the likeliest way to lose a stream. `store.delete(path, force=True)` is
carried out; `force` does nothing while the switch is off, so a command that
rebuilds a stream by deleting and re-appending can pass it unconditionally.

Every store honours the switch the same way since `0.9.0`; before that only the
durable store did. Outside Django, pass it to the store yourself:
`StreamStore(permanent=True)` or `JsonlStreamStore(root, permanent=True)`. The
durable store reads the setting on every call, so `override_settings` in a test
takes effect immediately; the in-memory and file stores take it when
`get_store()` first builds them.

### Deleting a stream deletes its events

Whether the switch is on or off, `delete()` removes the stream, its entries, and
every event that no stream refers to any more. An event that also appears in
another stream is kept, because that stream still needs it. Nothing keeps a copy
of what goes: if the payloads matter, export the stream first, and treat your
database backups as the way back.

Events left behind by deletes from before this behaviour, or by anything else
that removed entries, can be cleared with a command:

```bash
python manage.py prune_orphan_events --dry-run   # count only
python manage.py prune_orphan_events             # delete them
```

It takes `--database` for a named alias and `--batch-size` (default 1000), as
`prune_outcomes` does. See [Deployment](deployment.md#retention-pruning-orphaned-events).

## Admin

`django_rakaia.admin` registers `Stream`, `StreamEvent`, and `StreamEntry` with
the Django admin so you can browse stored streams and inspect event payloads. To register your own concrete `StreamEvent` subclass
in the admin, call:

```python
from django_rakaia import register_stream_event_admin
from myapp.models import MyStreamEvent

register_stream_event_admin(MyStreamEvent)
```

## Appendix

**Permanent streams, precisely.** The rules live in `rakaia.stream_rules`, which
every store asks. The switch makes a store's expiry check report every stream as
live, which is the one question each read and write path
asks before reaping, so none of them reaps. A create is refused before anything
is written, including an idempotent re-create of a stream that already has the
same expiry. The protocol server refuses `DELETE` by calling the store's
`check_protocol_delete(path)` before `delete`. A `DELETE` of a stream that does not exist is still `404`, and the
`405` carries an `Allow` header without `DELETE` in it, whether the refusal came
from that check or from `delete()` itself. A stream that was given an expiry
earlier reports none (`get()` returns no TTL or expiry), so `HEAD` does not send
`Stream-TTL` or `Stream-Expires-At` for it. The two refusals are
`rakaia.ExpiryNotAllowed` (a `ValueError`, mapped to `400`) and
`rakaia.DeleteNotAllowed` (mapped to `405`), both in `STORE_FAILURE_STATUS`.
Copying a log onto a permanent store with `migrate_stream` fails on a source
stream that has an expiry, since the copy recreates it with that expiry.

**The orphan delete** runs in one transaction on the store's alias. It walks
the stream's entries 500 at a time, reading only ids: it deletes each batch of
entries, then the events from that batch that no entry points at any more, and
deletes the stream row (and its producer rows) last. An event fanned into
another stream still has an entry there and is kept. If any step fails, the
whole delete is rolled back. The offset watermark survives as before. If your
app subclasses `StreamEvent` (the `register_stream_event_admin(MyStreamEvent)`
pattern), deleting a stream costs one query per event instead: Django reads
each subclass row, payload included, before deleting it.

**A stream given an expiry before the switch** is treated as having none by
`create()` too, so re-creating it without an expiry returns it (a protocol `PUT`
answers `200`) instead of a `409` configuration conflict.
