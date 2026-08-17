"""Executable proof for the core-package findings of the 2026-08-17 architecture review.

Each test here **fails today** and is marked `xfail(strict=True)`, so:

* the suite stays green while the finding is open;
* the day someone fixes the underlying defect, the test reports XPASS(strict) —
  a failure that says "your fix worked, now delete this marker";
* nobody has to take the review's word for anything. The claim is the test.

These are not tests of desired new interfaces. Each asserts a property the code
**already claims** — in a docstring, in the glossary, or in a sibling
implementation — and does not currently hold.

Epic: #152.
"""

from __future__ import annotations

import pytest

from rakaia.effects import Upsert
from rakaia.executors import CollectingExecutor
from rakaia.registry import HandlerRegistry
from rakaia.replay import replay
from rakaia.store import StreamStore


def _handler(event: dict) -> Upsert:
    """A trivial handler defined in a real file, so its source can be hashed."""
    return Upsert(model_label="m", lookup={"k": event.get("a", 0)}, defaults={})


def _registry() -> HandlerRegistry:
    reg = HandlerRegistry()
    reg.register("h", "s", _handler, 0, None)
    return reg


# ---------------------------------------------------------------------------
# Finding 3 — wire framing is stored in the event
# ---------------------------------------------------------------------------


def test_a_json_mode_stream_can_be_replayed() -> None:
    """A stream is a stream. Its content type is a transport fact, not an event fact.

    `replay()` reads through `ReadableStore`, which says nothing about content
    types -- so a JSON-mode stream must replay exactly like any other. Today the
    first event fails to decode, because the bytes carry a trailing comma that
    only `format_response` knows how to strip.
    """
    store = StreamStore()
    store.create("s", content_type="application/json")
    store.append("s", b'{"a": 1}')

    result = replay(
        store=store,
        stream_path="s",
        executor=CollectingExecutor(),
        handler_registry=_registry(),
    )

    assert result.events_processed == 1


def test_a_json_array_append_stores_one_message_per_element() -> None:
    """`server_store_contract.py` names this behaviour in a test title.

    That contract test asserts through `format_response`, which re-wraps the
    blob -- so it passes while the property it names does not hold. Asserting on
    the stored messages instead is what makes the claim observable.
    """
    store = StreamStore()
    store.create("s", content_type="application/json")
    store.append("s", b'[{"id": 1}, {"id": 2}]')

    messages = store.read("s")[0]

    assert len(messages) == 2
    # Stored in the canonical compact form the response concatenates, which is
    # what the append path has always produced -- the change is that the payload
    # is now a complete JSON value rather than one with a comma stuck to it.
    assert messages[0].data == b'{"id":1}'
    assert messages[1].data == b'{"id":2}'


# ---------------------------------------------------------------------------
# Finding 4 — drift is hashed per event, not per registration
# ---------------------------------------------------------------------------


def test_a_handler_source_is_hashed_once_per_replay(monkeypatch) -> None:
    """Drift is a property of a registration, so the cost must not scale with events.

    Measured on this checkout: 2000 events spend ~86% of total replay time
    re-hashing one unchanged function. This pins the shape rather than the
    timing -- one resolved handler means one hash, whatever the stream length.
    """
    import sys

    replay_module = sys.modules["rakaia.replay"]
    calls: list[object] = []
    original = replay_module.hash_function_source

    def counting(fn: object) -> str:
        calls.append(fn)
        return original(fn)

    monkeypatch.setattr(replay_module, "hash_function_source", counting)

    store = StreamStore()
    store.create("s")
    for i in range(50):
        store.append("s", b'{"a": %d}' % i)

    replay(
        store=store,
        stream_path="s",
        executor=CollectingExecutor(),
        handler_registry=_registry(),
    )

    assert len(calls) == 1, (
        f"one registered handler, one replay, but the source was hashed "
        f"{len(calls)} times -- once per event"
    )


# ---------------------------------------------------------------------------
# Finding "also surfaced" — the function shadows the module
# ---------------------------------------------------------------------------


@pytest.mark.xfail(
    strict=True,
    reason=(
        "FINDING 9 (#161): `from .replay import replay` in rakaia/__init__.py "
        "rebinds the `replay` attribute on the package from the submodule to "
        "the function, so `import rakaia.replay as rp; rp.anything` fails. "
        "Low value, but it silently misdirects monkeypatches."
    ),
)
def test_importing_the_replay_submodule_yields_the_module() -> None:
    """`import x.y as z` should bind the submodule, as it does for every other one."""
    import rakaia.replay as replay_module

    assert hasattr(replay_module, "build_pipeline")
