"""
Django models for the Data Streams dashboard.

Provides a normalized model structure with Stream, StreamEvent, and StreamEntry
for efficient querying and real-time updates via Unix sockets.
"""

import enum
import warnings

from django.core.serializers.json import DjangoJSONEncoder
from django.db import models
from django.db.models import Max
from django.utils import timezone

from rakaia.types import ClosedBy

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

    The lifecycle columns below (content type, TTL/expiry, closed, last_seq)
    exist so this model can back a Durable Streams **protocol server**, not
    only the event-sourcing read/emit path. They mirror the in-memory
    `rakaia.types.Stream` field-for-field so one server implementation can run
    on either store. Streams created purely for event-sourcing leave them at
    their defaults and behave exactly as before.
    """

    stream_id = models.CharField(max_length=255, unique=True, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True)

    # --- protocol lifecycle ---------------------------------------------
    content_type = models.CharField(max_length=255, null=True, blank=True)
    """Declared content type. A JSON content type puts the stream in JSON mode."""

    ttl_seconds = models.IntegerField(null=True, blank=True)
    """Sliding TTL window, anchored on `last_activity_at`."""

    expires_at = models.CharField(max_length=64, null=True, blank=True)
    """Absolute RFC 3339 expiry. Unlike `ttl_seconds`, does not slide."""

    last_activity_at = models.FloatField(default=0.0)
    """Unix time of the last TTL-extending activity (read/write/close)."""

    last_seq = models.IntegerField(null=True, blank=True)
    """Last `Stream-Seq` accepted, for writer coordination."""

    closed = models.BooleanField(default=False)
    """Whether the stream is closed to further appends."""

    closed_at = models.FloatField(null=True, blank=True)

    closed_by_producer_id = models.CharField(max_length=255, null=True, blank=True)
    closed_by_epoch = models.IntegerField(null=True, blank=True)
    closed_by_seq = models.IntegerField(null=True, blank=True)
    """The producer tuple that closed the stream, so a retried close is
    recognised as the same one and reported as idempotent rather than refused."""

    class Meta:
        db_table = "rakaia_stream"
        ordering = ["stream_id"]

    def __str__(self) -> str:
        return self.stream_id

    @property
    def current_offset(self) -> str:
        """The stream head, as the protocol's opaque offset string.

        A property rather than a column: the high-water is already tracked
        authoritatively by `StreamOffsetWatermark` (and survives delete +
        recreate), so storing it again here would be a second source of truth
        to keep in step. Named to match the in-memory `Stream.current_offset`,
        which is what lets one protocol server read either.
        """
        from .django_store import format_offset

        entries_max = self.entries.aggregate(max_offset=Max("offset"))["max_offset"]
        watermark_high = (
            StreamOffsetWatermark.objects.filter(stream_path=self.stream_id)
            .values_list("high", flat=True)
            .first()
        )
        return format_offset(max(entries_max or 0, watermark_high or 0))

    @property
    def closed_by(self) -> ClosedBy | None:
        """The closing producer tuple, in the shape the protocol server expects."""
        if self.closed_by_producer_id is None:
            return None
        return ClosedBy(
            producer_id=self.closed_by_producer_id,
            epoch=self.closed_by_epoch or 0,
            seq=self.closed_by_seq or 0,
        )

    def get_next_offset(self) -> int:
        """Calculate the next globally-monotonic offset for this stream.

        Thin wrapper over :meth:`get_next_offset_block` for the single-event
        write path; see that method for the locking and monotonicity contract.
        """
        return self.get_next_offset_block(1)

    def get_next_offset_block(self, count: int) -> int:
        """Atomically reserve ``count`` contiguous offsets, returning the first.

        The reserved block is ``start .. start + count - 1``. ``count`` must be
        ``>= 1``. This is the single offset-allocation path shared by every
        write surface (durable store single + bulk append, ``@stream_model``
        signals, protocol views).

        Must be called inside a transaction: locks the per-path high-water row
        (``StreamOffsetWatermark``) with select_for_update() so concurrent
        writers serialize on offset allocation instead of colliding on
        unique_together(stream, offset). A bulk writer holds the lock once and
        advances the high-water by the whole block, so interleaved single and
        bulk appends never overlap offsets.

        Offsets are **globally monotonic across delete+recreate** (#34, Defect
        #2): the high-water row is keyed by ``stream_id`` and is never
        cascade-deleted with the ``Stream``, so a stream recreated at the same
        path resumes numbering *above* the retired high mark rather than
        restarting at 1. That closes the silent-data-loss window where a stale
        subscriber cursor collides with the recreated stream's head. This is the
        durable analogue of the in-memory store's in-process ``_retired_seq``.
        """
        if count < 1:
            raise ValueError(f"count must be >= 1, got {count}")
        # Lock the per-path high-water for the duration of the enclosing
        # transaction. get_or_create tolerates the first-writer race (it retries
        # on the unique-key IntegrityError); the subsequent locked read then
        # serializes the read-modify-write.
        StreamOffsetWatermark.objects.get_or_create(stream_path=self.stream_id)
        watermark = StreamOffsetWatermark.objects.select_for_update().get(
            stream_path=self.stream_id
        )
        # `max(entries, watermark)` is belt-and-suspenders: the watermark alone
        # is authoritative for live streams, but taking the max also seeds it
        # correctly from any pre-existing entries (e.g. rows written before this
        # high-water table existed) on the first allocation after migration.
        entries_max = self.entries.aggregate(max_offset=Max("offset"))["max_offset"]
        start = max(entries_max or 0, watermark.high) + 1
        watermark.high = start + count - 1
        watermark.save(update_fields=["high"])
        return start


class StreamOffsetWatermark(models.Model):
    """The highest offset ever issued for a stream path, surviving deletion.

    Keyed by ``stream_path`` (the ``Stream.stream_id`` string) with **no FK to
    ``Stream``**, so it is *not* cascade-deleted when a stream is deleted. That
    persistence is the whole point: it lets a recreated stream resume offset
    numbering above the retired high mark, keeping durable offsets globally
    monotonic across delete+recreate (#34, Defect #2). See
    ``Stream.get_next_offset``.
    """

    stream_path = models.CharField(max_length=255, primary_key=True)
    high = models.BigIntegerField(default=0)

    class Meta:
        db_table = "rakaia_streamoffsetwatermark"

    def __str__(self) -> str:
        return f"{self.stream_path}={self.high}"


class StreamProducer(models.Model):
    """Per-producer epoch and sequence state, for producer fencing.

    The durable equivalent of the in-memory `Stream.producers` dict. A producer
    identifies itself with `(Producer-Id, Producer-Epoch, Producer-Seq)`; the
    server uses this row to reject a stale epoch, spot a sequence gap, and
    recognise a retry as a duplicate rather than writing it twice.

    Rows are expired lazily on read (see `PRODUCER_STATE_TTL_SECONDS`), matching
    the in-memory store's cleanup, so an abandoned producer id does not pin
    state forever.
    """

    stream = models.ForeignKey(
        Stream, on_delete=models.CASCADE, related_name="producers"
    )
    producer_id = models.CharField(max_length=255)
    epoch = models.IntegerField()
    last_seq = models.IntegerField()
    last_updated = models.FloatField()

    class Meta:
        db_table = "rakaia_streamproducer"
        unique_together = [("stream", "producer_id")]

    def __str__(self) -> str:
        return f"{self.producer_id}@{self.epoch}:{self.last_seq}"


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

    data = models.JSONField(encoder=DjangoJSONEncoder)  # type: ignore[assignment]
    """The event payload. Encoded with ``DjangoJSONEncoder`` so the types Django
    models actually hold — ``UUID``, ``datetime``/``date``, ``Decimal`` — are
    serialized (to strings) instead of raising ``TypeError`` at insert time from
    inside the consumer's ``post_save``, i.e. crashing the very save being
    audited. Consumers do not need to pre-stringify payloads (issue #80). The
    encoder is Python-side only — no schema change.

    ``datetime``/``time`` values are truncated (not rounded) to **millisecond**
    precision, because that is what ``DjangoJSONEncoder`` emits; a
    ``microsecond`` of exactly 0 emits no fractional part at all. The models the
    payload is lifted from store microseconds, so a payload timestamp will not
    compare equal to its source column. Format the value yourself in
    ``to_dataclass`` if you need the full precision or a stable string shape."""
    event_type = models.CharField(max_length=50, db_index=True)
    created_at = models.DateTimeField(auto_now_add=True, db_index=True)
    metadata = models.JSONField(default=dict, blank=True, encoder=DjangoJSONEncoder)
    """Event-sourcing envelope metadata (actor, url, causation, …). Empty for
    raw/pure-protocol appends. `event_type` doubles as the envelope label."""
    event_ts = models.FloatField(null=True, blank=True, db_index=True)
    """Event-sourcing envelope timestamp: the event's logical time as set by the
    producer (`AppendOptions.event_ts`). `null` when the producer didn't set one,
    in which case the store surfaces the append time (`created_at`) instead. This
    is the deterministic merge key, kept distinct from transport time."""

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


class ConsumerCursor(models.Model):
    """A durable per-consumer watermark over a stream.

    One row per ``(consumer_id, stream_path)`` holding the last offset that
    consumer applied. It is the streams-native replacement for a hand-rolled
    ``last_change_id`` sync table: a consumer resumes exactly where it left off
    across restarts. See ``rakaia.subscription.poll`` for the read semantics and
    ``django_rakaia.subscription`` for the load/commit helpers.
    """

    consumer_id = models.CharField(max_length=128)
    stream_path = models.CharField(max_length=255)
    offset = models.CharField(max_length=64)
    updated_at = models.DateTimeField(auto_now=True)

    class Meta:
        db_table = "rakaia_consumercursor"
        unique_together = [["consumer_id", "stream_path"]]

    def __str__(self) -> str:
        return f"{self.consumer_id}@{self.stream_path}={self.offset}"
