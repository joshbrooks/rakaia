"""Polyglot landing page: live-editable translations via SSE.

Visiting `/?lang=<code>` renders the marketing page using Translatable rows.
The right-hand pane is an editor — saving a row triggers a post_save
signal (see signals.py) that appends a JSON record to
`/translations/{langcode}`. Browsers tuned to that langcode update in
place, over SSE served by the protocol server mounted in asgi.py.

First request seeds the catalog so the page is never blank.
"""

from __future__ import annotations

from django.conf import settings
from django.http import HttpRequest, HttpResponse
from django.shortcuts import redirect, render
from django.urls import reverse
from django.views.decorators.http import require_http_methods

from polyglot.models import Translatable

from .signals import ensure_stream, stream_path
from .strings import CATALOG, DEFAULT_LANG, LANGUAGES


def _seed_catalog() -> None:
    """Idempotently ensure a Translatable row exists for every msgid x lang."""
    for msgid, defaults_by_lang in CATALOG:
        for langcode, msgstr in defaults_by_lang.items():
            Translatable.objects.get_or_create(
                msgid=msgid,
                langcode=langcode,
                msgctxt=None,
                defaults={"msgstr": msgstr},
            )


def _strings_for(langcode: str) -> dict[str, str]:
    rows = Translatable.objects.filter(langcode=langcode, deleted__isnull=True)
    return {r.msgid: (r.msgstr or "") for r in rows}


def _ids_for(langcode: str) -> dict[str, int]:
    rows = Translatable.objects.filter(langcode=langcode, deleted__isnull=True)
    return {r.msgid: r.pk for r in rows}


def landing(request: HttpRequest) -> HttpResponse:
    _seed_catalog()

    langcode = request.GET.get("lang", DEFAULT_LANG)
    if langcode not in {code for code, _ in LANGUAGES}:
        langcode = DEFAULT_LANG

    # The seeding above appends, which creates the stream on a fresh checkout.
    # This covers the other order: rows already in the database and the stream
    # directory deleted, where the page would otherwise render against an
    # endpoint that 404s until someone saves.
    ensure_stream(stream_path(langcode))

    strings = _strings_for(langcode)
    ids = _ids_for(langcode)
    entries = [
        {"msgid": msgid, "msgstr": strings.get(msgid, ""), "id": ids.get(msgid)}
        for msgid, _ in CATALOG
    ]
    by_msgid = {e["msgid"]: e for e in entries}

    return render(
        request,
        "polyglot/landing.html",
        {
            "langcode": langcode,
            "languages": LANGUAGES,
            "entries": entries,
            "t": by_msgid,
            "stream_id": stream_path(langcode),
            "protocol_prefix": settings.POLYGLOT_PROTOCOL_PREFIX,
        },
    )


@require_http_methods(["POST"])
def update_translation(request: HttpRequest, pk: int) -> HttpResponse:
    msgstr = request.POST.get("msgstr", "")
    obj = Translatable.objects.filter(pk=pk).first()
    if obj is not None:
        obj.msgstr = msgstr
        obj.save()  # post_save → append to the log → SSE update event
    if request.headers.get("X-Requested-With") == "XMLHttpRequest":
        return HttpResponse(status=204)
    return redirect(
        reverse("polyglot:landing") + f"?lang={obj.langcode if obj else DEFAULT_LANG}"
    )
