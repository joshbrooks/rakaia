"""
Django models for Durable Streams integration.

This module provides the normalized model structure for streams:

    Stream (stream_id) ──< StreamEntry (offset) >── StreamEvent (data)

This allows a single event to appear in multiple streams with independent,
monotonic offsets per stream.
"""

from django.db import models


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
        from django.db.models import Max

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

    data = models.JSONField()
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
