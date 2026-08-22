"""The two handlers that make up the staged projection.

Autodiscovered by ``django_rakaia`` on app-ready (it imports every installed
app's ``handlers.py``), so importing this module registers both handlers into
the default registry — no wiring in the demo command.

The whole recipe hinges on **stages**: stage 0 builds the reference entities,
stage 1 links to them. Replay runs the entire event range through stage 0 before
stage 1 begins, so a stage-1 handler's ``reader`` sees every Project — even one
whose event arrives *after* the task that needs it.
"""

from __future__ import annotations

from rakaia import Effect, Upsert, register_handler, register_simple


@register_simple(name="project", event_match="PROJECT", match_field="form_type")
def project(event: dict) -> Effect:
    """Stage 0 (the default): upsert the reference Project.

    ``register_simple`` is the always-on "just project this" case — no
    ``effective_from=0`` / ``effective_to=None`` ceremony. ``match_field`` routes
    on the payload's ``form_type`` rather than the stream path.
    """
    return Upsert(
        model_label="cookbook.Project",
        lookup={"code": event["code"]},
        defaults={"name": event["name"]},
    )


@register_handler(
    name="task",
    event_match="TASK",
    effective_from=0,
    match_field="form_type",
    stage=1,
)
def task(event: dict, reader) -> Effect:
    """Stage 1: upsert the Task and link it to its Project by ``code``.

    A stage > 0 handler is called ``fn(event, reader)``. Because stage 0 has
    already committed every Project, the lookup resolves regardless of the order
    the events arrived in.
    """
    linked = reader.get("cookbook.Project", code=event["project_code"])
    return Upsert(
        model_label="cookbook.Task",
        lookup={"task_id": event["task_id"]},
        defaults={
            "title": event["title"],
            "project_id": linked.pk if linked is not None else None,
        },
    )
