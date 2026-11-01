"""`manage.py worker_pass` — one worker's single pass, in its own process.

This is what a second worker *is*: a separate process, with a separate database
connection, running `django_consumer(...).run(...)` under the same consumer
name. Two of these started together are the situation the cursor row lock
exists for.

The worker writes what it did to a small JSON file the demo reads back:
``applied`` (the keys it projected, in order), the pass's ``status``, and the
cursor as it stands after the pass. A pass has one commit at the end, so the
cursor after it is the whole story of where this worker left the stream.

``--hold-marker`` / ``--release-marker`` are the demo's handshake, not
production code: on its first apply the worker writes the marker and then holds
the pass open until the release file appears. The pass is one transaction on the
cursor alias, so holding here holds the cursor row lock — which is how the demo
catches a second worker waiting at it rather than guessing from elapsed time.
"""

from __future__ import annotations

import json
import time
from pathlib import Path
from typing import Any

from django.core.management.base import BaseCommand, CommandError

from django_rakaia import django_consumer
from django_rakaia.store import get_store
from django_rakaia.subscription import load_cursor
from workers.consume import apply_event
from workers.seed import CONSUMER, STREAM

#: How long a held worker waits for the demo's release file before giving up.
HOLD_TIMEOUT_SECONDS = 120.0


class _Hold:
    """Touch a marker on the first apply, then wait for the release file."""

    def __init__(self, marker: str | None, release: str | None) -> None:
        self._marker = Path(marker) if marker else None
        self._release = Path(release) if release else None
        self._done = False

    def wait_once(self) -> None:
        if self._done or self._marker is None:
            return
        self._done = True
        self._marker.touch()
        if self._release is None:
            return
        deadline = time.monotonic() + HOLD_TIMEOUT_SECONDS
        while not self._release.exists():
            if time.monotonic() > deadline:
                raise CommandError("the demo never released the held worker")
            time.sleep(0.02)


class Command(BaseCommand):
    help = "Run one consumer pass as its own process (the two-workers demo)."

    def add_arguments(self, parser: Any) -> None:
        parser.add_argument("--role", required=True, help="which worker this is")
        parser.add_argument("--result", required=True, help="where to write the report")
        parser.add_argument("--hold-marker", default=None)
        parser.add_argument("--release-marker", default=None)

    def handle(self, *args: Any, **opts: Any) -> None:  # noqa: ARG002
        role: str = opts["role"]
        hold = _Hold(opts["hold_marker"], opts["release_marker"])
        applied: list[str] = []

        def apply(message: Any) -> None:
            hold.wait_once()
            apply_event(role, message)
            applied.append(json.loads(message.data)["key"])

        result = django_consumer(get_store(), STREAM, CONSUMER).run(
            apply, on_error="halt"
        )
        report = {
            "role": role,
            "applied": applied,
            "status": result.status,
            "halted": result.halted,
            "applied_count": result.applied,
            "cursor_after": load_cursor(CONSUMER, STREAM),
        }
        Path(opts["result"]).write_text(json.dumps(report))
        self.stdout.write(
            f"worker {role}: {len(applied)} applied, status {result.status}, "
            f"cursor {report['cursor_after']}"
        )
