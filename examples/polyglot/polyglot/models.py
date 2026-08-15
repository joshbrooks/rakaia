"""The `Translatable` model this example is built around.

It used to live in `django_rakaia` itself, which meant every consumer of the
library got a translations table in their database whether or not they wanted
one — and its `langcode` choices are Timor-Leste specific (`tet`/`pt`/`id`),
which is a strong hint it was never a general-purpose library feature. It is
demo domain, so it lives with the demo.

This example's own `signals.py` already made the argument, before the model
moved:

    "We don't decorate the library's `Translatable` model itself — that would
    push demo concerns into the library."

What rakaia provides here is the streaming: `create_stream_event` fans a change
out to a per-langcode stream, and the SSE endpoint delivers it. The model is
just the thing being translated.
"""

from __future__ import annotations

import enum
import warnings

from django.db import models
from django.utils import timezone

DEFAULT_LANG = "en"


class MSG_IDX(enum.Enum):
    SINGULAR = 0
    PLURAL = 1


class TranslatableManager(models.Manager["Translatable"]):
    @staticmethod
    def plural_formula(langcode: str, number: int):  # noqa: ARG004
        """
        Different languages have different "plural" forms
        This can be specified here - although most languages have
        "one thing" , "many thingS"
        Unless we translate Arabic or Polish we should be good!
        """
        if number == 1:
            return 0  # "an apple", "one apple"
        return 1  # "no apples", "some apples", "five apples"

    def gettext(self, msgid: str, langcode: str = DEFAULT_LANG):
        try:
            return (
                self.get_queryset()
                .filter(langcode=langcode, msgid=msgid)
                .first()
                .msgstr
            )
        except (Translatable.DoesNotExist, IndexError, AttributeError):
            pass
        warnings.warn(
            f"No translated content found: langcode='{langcode}', msgid='{msgid}'",
            stacklevel=2,
        )

    def ngettext(
        self, singular: str, plural: str, number: int, langcode: str = DEFAULT_LANG
    ):
        msg_idx = TranslatableManager.plural_formula(langcode, number)
        try:
            return (
                self.get_queryset()
                .filter(langcode=langcode, msgid=singular)
                .first()
                .msgstr[msg_idx]
            )
        except (Translatable.DoesNotExist, IndexError, AttributeError):
            pass
        warnings.warn(
            f"No translated content found: langcode='{langcode}', singular='{singular}', plural='{plural}'",
            stacklevel=2,
        )
        return singular if msg_idx == MSG_IDX.SINGULAR.value else plural

    def pgettext(self, context: str, msgid: str, langcode: str = DEFAULT_LANG):
        try:
            return (
                self.get_queryset()
                .filter(msgctxt=context, langcode=langcode, msgid=msgid)
                .first()
                .msgstr
            )
        except (Translatable.DoesNotExist, IndexError, AttributeError):
            pass
        warnings.warn(
            f"No translated content found: langcode='{langcode}', context='{context}', msgid='{msgid}'",
            stacklevel=2,
        )
        return msgid

    def npgettext(
        self,
        context: str | None,
        singular: str,
        plural: str,
        number: int,
        langcode: str = DEFAULT_LANG,
    ):
        msg_idx = TranslatableManager.plural_formula(langcode, number)
        try:
            return (
                self.get_queryset()
                .filter(msgctxt=context, langcode=langcode, msgid=singular)
                .first()
                .msgstr[msg_idx]
            )
        except (Translatable.DoesNotExist, IndexError, AttributeError):
            pass
        warnings.warn(
            f"No translated content found: langcode='{langcode}', context='{context}', singular='{singular}'",
            stacklevel=2,
        )
        return singular if msg_idx == MSG_IDX.SINGULAR.value else plural


class Translatable(models.Model):
    """
    This represents a database-side interpretation of the `gettext`
    funtions
    """

    msgid = models.CharField(
        max_length=2048, help_text="The original message to be translated"
    )
    msgstr = models.CharField(
        max_length=2048, null=True, blank=True, help_text="Translated message"
    )
    domain = models.CharField(
        max_length=2048, null=True, blank=True, help_text="Message domain"
    )
    msgctxt = models.CharField(
        max_length=2048, null=True, blank=True, help_text="Message context"
    )
    langcode = models.CharField(
        max_length=3,
        help_text="The destination language code",
        default=DEFAULT_LANG,
        choices=[("tet", "tet"), ("pt", "pt"), ("id", "id")],
    )
    deleted = models.DateTimeField(null=True, blank=True)

    objects = TranslatableManager()

    class Meta:
        unique_together = [["msgid", "msgctxt", "langcode"]]
        indexes = [
            models.Index(fields=["msgid", "msgctxt", "langcode"]),
        ]

    def __str__(self):
        return self.msgid

    def soft_delete(self):
        """Soft delete by marking as deleted instead of actually deleting."""
        self.deleted = timezone.now()
        self.save()

    def restore(self):
        """Restore a soft-deleted translation."""
        self.deleted = None
        self.save()
