"""Where the logic lives. This module registers nothing."""

from __future__ import annotations

from typing import Any


def project_room(event: dict[str, Any], *, prefix: str = "") -> list:
    """A handler body with a dependency to bind — the `functools.partial` case
    `docs/versioned-handlers.md` recommends over a closure."""
    _ = f"{prefix}{event.get('room_id', '')}"
    return []


def upcast_room(event: dict[str, Any]) -> dict[str, Any]:
    return {**event, "currency": "USD"}


def reduce_rooms(reader: Any) -> list:  # noqa: ARG001
    return []


class Projector:
    """A handler defined as a method: its qualname is
    ``…logic.Projector.project``, so splitting one segment off the dotted path
    yields the *class*, which is not importable."""

    def project(self, event: dict[str, Any]) -> list:  # noqa: ARG002
        return []
