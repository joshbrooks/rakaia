"""Property for GitHub #341: a model save is an append like any other.

For any run of saves and closes, the configured store holds exactly the events
saved while the stream was open, in order, and a save to a closed stream is
refused. Run against both the database store and the in-memory one, because the
bug was that the model-save path knew about only the database.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass

import pytest
from django.test import override_settings
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from django_rakaia.decorators import create_stream_event
from django_rakaia.store import get_store, reset_store_cache
from rakaia.types import StreamError

from .models import FinanceLine

pytestmark = pytest.mark.django_db


@dataclass
class _Payload:
    n: int


@pytest.mark.parametrize("backend", ["durable", "memory"])
@settings(
    max_examples=25,
    deadline=None,
    suppress_health_check=[HealthCheck.function_scoped_fixture],
)
@given(ops=st.lists(st.sampled_from(["save", "close"]), max_size=8))
def test_store_holds_exactly_the_saves_made_while_open(backend, ops):
    path = f"prop:{uuid.uuid4().hex}"
    line = FinanceLine(submission_id="a", suku="s", delta=1)
    with override_settings(RAKAIA_STORE=backend):
        reset_store_cache()
        try:
            store = get_store()
            store.create(path, content_type="application/json")
            kept: list[int] = []
            closed = False
            for n, op in enumerate(ops):
                if op == "close":
                    store.close_stream(path)
                    closed = True
                elif closed:
                    with pytest.raises(StreamError):
                        create_stream_event(
                            path, lambda _o, n=n: _Payload(n), line, "update"
                        )
                else:
                    create_stream_event(
                        path, lambda _o, n=n: _Payload(n), line, "update"
                    )
                    kept.append(n)
            messages, _ = store.read(path)
            assert [json.loads(m.data)["n"] for m in messages] == kept
        finally:
            reset_store_cache()
