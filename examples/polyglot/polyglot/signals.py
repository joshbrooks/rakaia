"""Emit per-langcode SSE events when Translatable rows change.

We don't decorate the library's `Translatable` model itself — that would
push demo concerns into the library. Instead the polyglot app subscribes
its own post_save / post_delete handlers and broadcasts to:

    translations:{langcode}

so a browser pinned to one language only receives updates for that
language.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import cast

from django.db import models
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from django_rakaia.decorators import create_stream_event
from django_rakaia.models import Translatable


@dataclass
class TranslationPayload:
    id: int
    msgid: str
    msgstr: str
    langcode: str
    msgctxt: str | None


def _to_payload(obj: models.Model) -> TranslationPayload:
    t = cast(Translatable, obj)
    return TranslationPayload(
        id=t.pk,
        msgid=t.msgid,
        msgstr=t.msgstr or "",
        langcode=t.langcode,
        msgctxt=t.msgctxt,
    )


@receiver(post_save, sender=Translatable)
def _translatable_saved(
    sender, instance: Translatable, created: bool, **kwargs
) -> None:
    # Hand-wired receivers do not get `@stream_model`'s `raw` guard — without
    # this, every `loaddata` row appends a phantom event (issue #80).
    if kwargs.get("raw"):
        return
    create_stream_event(
        stream_paths=[f"translations:{instance.langcode}"],
        to_dataclass=_to_payload,
        instance=instance,
        action="create" if created else "update",
    )


@receiver(post_delete, sender=Translatable)
def _translatable_deleted(sender, instance: Translatable, **kwargs) -> None:
    create_stream_event(
        stream_paths=[f"translations:{instance.langcode}"],
        to_dataclass=_to_payload,
        instance=instance,
        action="delete",
    )
