"""Append a JSON record to the per-langcode stream when a Translatable changes.

We don't decorate the library's `Translatable` model itself — that would push
demo concerns into the library. Instead the polyglot app subscribes its own
post_save / post_delete handlers and appends to:

    /translations/{langcode}

so a browser pinned to one language only receives updates for that language.

**Why this writes to the store rather than calling `create_stream_event`.**
`create_stream_event` is the ORM door: it writes `StreamEvent` and `StreamEntry`
rows directly and never consults `RAKAIA_STORE`, so with the file-backed store
selected it would keep filling the database while the log this app now serves
from stayed empty. Appending through `get_store()` is what makes the setting
load-bearing. The cost is that this demo gives up the envelope the ORM door
builds — provenance, and the fan-out of one event into several streams — for a
plain JSON record, which is all the page renders anyway.

The path is a protocol path (`/translations/tet`), not the old
`translations:tet`, because the protocol server routes on the URL path verbatim:
the stream id and the URL that reads it are the same string.
"""

from __future__ import annotations

import contextlib
import json
from dataclasses import asdict, dataclass
from typing import cast

from django.db import models
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from django_rakaia.store import get_store
from polyglot.models import Translatable
from rakaia import AppendOptions, StreamConfigConflict

#: JSON so the protocol server sends the record as text over SSE rather than
#: base64, and so an append that is not valid JSON is refused at the door.
CONTENT_TYPE = "application/json"


def stream_path(langcode: str) -> str:
    return f"/translations/{langcode}"


@dataclass
class TranslationPayload:
    id: int
    msgid: str
    msgstr: str
    langcode: str
    msgctxt: str | None
    action: str


def _to_payload(obj: models.Model, action: str) -> TranslationPayload:
    t = cast(Translatable, obj)
    return TranslationPayload(
        id=t.pk,
        msgid=t.msgid,
        msgstr=t.msgstr or "",
        langcode=t.langcode,
        msgctxt=t.msgctxt,
        action=action,
    )


def ensure_stream(path: str) -> None:
    """Create `path` if it isn't there yet, tolerating a concurrent creator.

    `create` is idempotent when the configuration matches, so the ordinary
    second call is a cheap read of `meta.json`. A `StreamConfigConflict` here
    can only mean a stream of this name already exists with different settings —
    left over from an earlier run of the demo — which is not worth failing a
    save over.
    """
    with contextlib.suppress(StreamConfigConflict):
        get_store().create(path, content_type=CONTENT_TYPE)


def _append(instance: Translatable, action: str) -> None:
    path = stream_path(instance.langcode)
    ensure_stream(path)
    record = json.dumps(asdict(_to_payload(instance, action))).encode()
    get_store().append(path, record, AppendOptions(label=action))


@receiver(post_save, sender=Translatable)
def _translatable_saved(
    # Django mandates the receiver signature; `sender` is unused here.
    sender,  # noqa: ARG001
    instance: Translatable,
    created: bool,
    **kwargs,
) -> None:
    # Hand-wired receivers do not get `@stream_model`'s `raw` guard — without
    # this, every `loaddata` row appends a phantom event (issue #80).
    if kwargs.get("raw"):
        return
    _append(instance, "create" if created else "update")


@receiver(post_delete, sender=Translatable)
def _translatable_deleted(
    sender,  # noqa: ARG001 - Django mandates the receiver signature
    instance: Translatable,
    **kwargs,  # noqa: ARG001
) -> None:
    _append(instance, "delete")
