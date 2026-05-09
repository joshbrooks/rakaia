"""
Django decorators for streaming model changes to stream events.

Supports multi-stream events where a single event can appear in multiple
streams with independent, monotonic offsets per stream.
"""

import dataclasses
from collections.abc import Callable
from typing import Any

from django.db import models, transaction
from django.db.models import Max
from django.db.models.signals import post_delete, post_save
from django.dispatch import receiver

from django_rakaia.models import Stream, StreamEntry, StreamEvent

StreamPathResolver = (
    str
    | Callable[[models.Model], str]
    | list[str]
    | Callable[[models.Model], list[str]]
)
DataclassTransformer = Callable[[models.Model], Any]


def _get_or_create_stream(stream_id: str) -> Stream:
    """Get or create a Stream record."""
    stream, _ = Stream.objects.get_or_create(stream_id=stream_id)
    return stream


def _get_next_offset(stream: Stream) -> int:
    """
    Calculate the next monotonic offset for a stream.

    Uses atomic MAX(offset)+1 calculation.
    """
    agg = stream.entries.aggregate(max_offset=Max("offset"))
    max_offset = agg["max_offset"]
    if max_offset is None:
        return 1
    return max_offset + 1


def create_stream_event(
    stream_paths: str | list[str] | Callable[[models.Model], str | list[str]],
    to_dataclass: DataclassTransformer,
    instance: models.Model,
    action: str,
) -> StreamEvent:
    """
    Create a stream event for the given model instance.

    Supports single or multiple streams. The event data is stored once,
    with separate StreamEntry records for each stream.

    Args:
        stream_paths: Single stream ID, list of stream IDs, or callable
            that returns either. The callable receives the model instance.
        to_dataclass: Callable converting model instance to dataclass.
        instance: The model instance that changed.
        action: Event type ("create", "update", or "delete").

    Returns:
        The created StreamEvent instance.

    Example (single stream):
        create_stream_event(
            stream_paths=f"user:{instance.id}:activity",
            to_dataclass=to_user_data,
            instance=instance,
            action="create",
        )

    Example (multiple streams):
        create_stream_event(
            stream_paths=[
                f"user:{instance.created_by_id}:projects",
                f"area:{instance.area_id}:projects",
            ],
            to_dataclass=to_project_data,
            instance=instance,
            action="create",
        )
    """
    # Resolve stream paths
    if callable(stream_paths):
        resolved = stream_paths(instance)
    else:
        resolved = stream_paths

    # Normalize to list
    if isinstance(resolved, str):
        stream_ids = [resolved]
    else:
        stream_ids = list(resolved)

    # Convert to dataclass and then dict
    dc_instance = to_dataclass(instance)

    if not dataclasses.is_dataclass(dc_instance):
        raise TypeError(
            f"to_dataclass must return a dataclass, got {type(dc_instance)}"
        )

    payload = dataclasses.asdict(dc_instance)

    # Create event and entries atomically
    with transaction.atomic():
        # Create the event (data stored once)
        event = StreamEvent.objects.create(
            data=payload,
            event_type=action,
        )

        # Create an entry for each stream
        for stream_id in stream_ids:
            stream = _get_or_create_stream(stream_id)
            next_offset = _get_next_offset(stream)
            StreamEntry.objects.create(
                stream=stream,
                event=event,
                offset=next_offset,
            )

    return event


def stream_model(
    stream_paths: StreamPathResolver,
    to_dataclass: DataclassTransformer,
) -> Callable[[type[models.Model]], type[models.Model]]:
    """
    Decorator to automatically stream Django model changes to stream events.

    Supports single or multiple streams per event.

    Args:
        stream_paths: Stream ID(s) or callable(s) that return stream ID(s).
            Can be:
            - A single stream ID string
            - A list of stream ID strings
            - A callable that takes the instance and returns a string or list
        to_dataclass: Callable converting model instance to dataclass.

    Example (single stream):
        @stream_model(
            stream_paths=lambda obj: f"user:{obj.id}:updates",
            to_dataclass=lambda obj: UserData(id=obj.id, name=obj.name),
        )
        class User(models.Model):
            name = models.CharField(max_length=100)

    Example (multiple streams):
        @stream_model(
            stream_paths=lambda obj: [
                f"user:{obj.created_by_id}:projects",
                f"area:{obj.area_id}:projects",
            ],
            to_dataclass=lambda obj: ProjectData(id=obj.id, name=obj.name),
        )
        class Project(models.Model):
            name = models.CharField(max_length=100)
            area = models.ForeignKey(Area, on_delete=models.CASCADE)
            created_by = models.ForeignKey(User, on_delete=models.CASCADE)
    """

    def decorator(model_cls: type[models.Model]) -> type[models.Model]:
        @receiver(post_save, sender=model_cls, weak=False)
        def handle_post_save(
            sender: type[models.Model],  # noqa: ARG001
            instance: models.Model,
            created: bool,
            signal: Any,  # noqa: ARG001
            **kwargs: Any,  # noqa: ARG001
        ) -> None:
            action = "create" if created else "update"
            create_stream_event(stream_paths, to_dataclass, instance, action)

        @receiver(post_delete, sender=model_cls, weak=False)
        def handle_post_delete(
            sender: type[models.Model],  # noqa: ARG001
            instance: models.Model,
            signal: Any,  # noqa: ARG001
            **kwargs: Any,  # noqa: ARG001
        ) -> None:
            create_stream_event(stream_paths, to_dataclass, instance, "delete")

        return model_cls

    return decorator
