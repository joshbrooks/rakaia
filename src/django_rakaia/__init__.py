"""
Django Integration for Rakaia Durable Streams.

This module provides Django-specific integration for the Durable Streams protocol,
allowing model changes (save/delete) to automatically emit stream events.

Usage:
    from django_rakaia.decorators import stream_model, create_stream_event

    @stream_model(
        stream_paths=lambda obj: f"user:{obj.id}:updates",
        to_dataclass=lambda obj: MyData(id=obj.id, name=obj.name),
    )
    class MyModel(models.Model):
        name = models.CharField(max_length=100)

    # Or use create_stream_event for built-in models
    @receiver(post_save, sender=User)
    def handle_user_save(sender, instance, created, **kwargs):
        create_stream_event(
            stream_paths=f"user:{instance.id}:activity",
            to_dataclass=to_user_data,
            instance=instance,
            action="create" if created else "update",
        )

Public API:
    - stream_model: Decorator for automatically streaming model changes
    - create_stream_event: Helper function for manually creating stream events
    - register_stream_event_admin: Register stream event model with Django admin
"""

# We intentionally do not eager import models here
# to avoid AppRegistryNotReady errors during Django's initialization.
# Models should be imported directly from django_rakaia.models after Django is setup.

default_app_config = "django_rakaia.apps.DjangoRakaiaConfig"
