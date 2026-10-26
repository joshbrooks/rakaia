# Rakaia chat sample

A minimal Django app demonstrating `django_rakaia`. It shows:

- The `@stream_model` decorator emitting events on save.
- Multi-stream events: each `Message` lands in both
  `room:{room_id}:messages` and `user:{author_id}:activity`.
- Live updates in the browser via Server-Sent Events.

## Run it

```bash
# From the repo root
uv sync --extra dev --extra django

cd examples/chat
uv run python manage.py migrate
uv run python manage.py runserver
```

Then visit http://localhost:8000/, create a room, and open it in two
browser tabs. Posting in one tab should make the message pop up in the
other tab without a refresh.

## No login, on purpose

The dashboard's live feed needs a signed-in user as of 0.10.0, and this sample has
no login page. [`chat/middleware.py`](chat/middleware.py) therefore signs every
visitor in as one demo user. A real project would sign people in properly and
drop it.

## What to look at

- [`chat/models.py`](chat/models.py) — `Message` decorated with
  `@stream_model` that returns *two* stream paths.
- [`chat/templates/chat/room.html`](chat/templates/chat/room.html) — Vanilla
  JavaScript `EventSource` consuming `/streams/api/streams/<stream_id>/sse/`.
- [`chat_project/settings.py`](chat_project/settings.py) — minimal settings
  with `daphne`, `channels`, and `django_rakaia` installed and an in-memory
  channel layer.
