"""
Django models for the Data Streams dashboard.

Provides a normalized model structure with Stream, StreamEvent, and StreamEntry
for efficient querying and real-time updates via Unix sockets.
"""

import enum
import warnings

from django.db import models
from django.db.models import Max
from django.utils import timezone

DEFAULT_LANG = "en"


class MSG_IDX(enum.Enum):
    SINGULAR = 0
    PLURAL = 1


class Stream(models.Model):
    """
    A logical stream identified by a unique stream_id.

    Examples:
        - "user:1:projects" - Projects for user 1
        - "area:5:projects" - Projects in area 5
        - "global:updates" - Global update feed

    Attributes:
        stream_id: Unique identifier for the stream
        created_at: When the stream was first created
    """

    stream_id = models.CharField(max_length=255, unique=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "rakaia_stream"
        ordering = ["stream_id"]

    def __str__(self) -> str:
        return self.stream_id

    def get_next_offset(self) -> int:
        """Calculate the next monotonic offset for this stream."""

        agg = self.entries.aggregate(max_offset=Max("offset"))
        max_offset = agg["max_offset"]
        if max_offset is None:
            return 1
        return max_offset + 1


class StreamEvent(models.Model):
    """
    An event containing data that can appear in one or more streams.

    This is the core event data, separate from stream assignments.
    Multiple StreamEntry records can reference the same StreamEvent,
    allowing the event to appear in multiple streams.

    Attributes:
        data: JSON payload containing the event data
        event_type: Type of event ("create", "update", "delete")
        created_at: When the event was created
    """

    data = models.JSONField()  # type: ignore[assignment]
    event_type = models.CharField(max_length=50, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)

    class Meta:
        db_table = "rakaia_streamevent"
        ordering = ["-created_at"]

    def __str__(self) -> str:
        return f"Event #{self.id} ({self.event_type})"

    def get_streams(self) -> list[str]:
        """Get all stream IDs this event appears in."""
        return list(
            self.entries.select_related("stream")
            .values_list("stream__stream_id", flat=True)
            .order_by("stream__stream_id")
        )


class StreamEntry(models.Model):
    """
    Junction table linking events to streams with stream-specific offsets.

    Each entry represents one event appearing in one stream at a specific
    offset. The offset is monotonic and unique within each stream.

    Attributes:
        stream: The stream this entry belongs to
        event: The event data
        offset: Monotonically increasing offset within the stream
        created_at: When the entry was created
    """

    stream = models.ForeignKey(
        Stream,
        on_delete=models.CASCADE,
        related_name="entries",
    )
    event = models.ForeignKey(
        StreamEvent,
        on_delete=models.CASCADE,
        related_name="entries",
    )
    offset = models.BigIntegerField()
    created_at = models.DateTimeField(auto_now_add=True)

    class Meta:
        db_table = "rakaia_streamentry"
        ordering = ["stream", "offset"]
        unique_together = ["stream", "offset"]  # Enforce monotonic offset per stream
        indexes = [
            models.Index(fields=["stream", "offset"]),  # Fast range queries
            models.Index(fields=["stream", "-offset"]),  # Fast reverse queries
        ]

    def __str__(self) -> str:
        return f"{self.stream.stream_id}#{self.offset}"


# Keep AbstractStreamEvent for backward compatibility
# New code should use the normalized models above
class AbstractStreamEvent(models.Model):
    """
    Legacy abstract model for backward compatibility.

    DEPRECATED: Use the normalized Stream/StreamEvent/StreamEntry models instead.

    This model is kept to avoid breaking existing code but should not be used
    for new development.
    """

    stream_id = models.CharField(max_length=255, db_index=True)
    data = models.JSONField()
    event_type = models.CharField(max_length=50)
    created_at = models.DateTimeField(auto_now_add=True)
    offset = models.BigIntegerField()

    class Meta:
        abstract = True
        ordering = ["stream_id", "offset"]
        unique_together = ["stream_id", "offset"]
        indexes = [
            models.Index(fields=["stream_id", "offset"]),
            models.Index(fields=["stream_id", "-offset"]),
        ]

    def __str__(self) -> str:
        return f"{self.stream_id}#{self.offset} ({self.event_type})"


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
