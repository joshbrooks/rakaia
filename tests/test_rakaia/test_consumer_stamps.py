"""Stream stamps and the consumer's stop messages (GitHub #339, #350).

Each test here pins something the suite let through when the line that does it
was changed: a cursor already at the head adopting a stamp, the error a stopped
consumer logs, and a file-backed stream written before stamps existed.
"""

from __future__ import annotations

import json
import logging

import pytest

from rakaia.jsonl_store import JsonlStreamStore
from rakaia.store import StreamStore
from rakaia.subscription import consume


def _one_event_store() -> StreamStore:
    store = StreamStore()
    store.create("s")
    store.append("s", b"a")
    return store


def test_a_caught_up_cursor_with_no_stamp_adopts_the_streams() -> None:
    store = _one_event_store()
    head = store.get_current_offset("s")
    stamps: list[str] = []

    result = consume(
        store,
        "s",
        apply=lambda _m: pytest.fail("nothing is new"),
        consumer="c",
        on_error="halt",
        cursor=head,
        generation=None,
        commit_generation=stamps.append,
    )

    assert result.status == "caught_up"
    assert stamps == [store.get_generation("s")]


def test_a_replaced_stream_is_logged_as_an_error(caplog) -> None:
    store = _one_event_store()
    stamp = store.get_generation("s")
    cursor = store.get_current_offset("s")
    store.delete("s")
    store.create("s")
    store.append("s", b"new")

    with caplog.at_level(logging.ERROR, logger="rakaia.subscription"):
        consume(
            store,
            "s",
            apply=lambda _m: pytest.fail("applied"),
            consumer="c",
            on_error="skip",
            cursor=cursor,
            generation=stamp,
        )

    errors = [r for r in caplog.records if r.levelno == logging.ERROR]
    assert errors and "deleted and recreated" in errors[0].getMessage()


def test_a_file_stream_from_before_stamps_gets_one_so_a_recreate_is_noticed(
    tmp_path,
) -> None:
    store = JsonlStreamStore(tmp_path)
    store.create("s", content_type="application/json")
    store.append("s", b"1")
    meta_file = next(tmp_path.rglob("meta.json"))
    raw = json.loads(meta_file.read_text())
    raw.pop("generation")
    meta_file.write_text(json.dumps(raw))

    cursors: list[str] = []
    stamps: list[str] = []
    consume(
        store,
        "s",
        apply=lambda _m: None,
        consumer="c",
        on_error="halt",
        commit=cursors.append,
        commit_generation=stamps.append,
    )
    assert len(stamps) == 1

    store.delete("s")
    store.create("s", content_type="application/json")
    store.append("s", b"2")
    store.append("s", b"3")

    result = consume(
        store,
        "s",
        apply=lambda _m: pytest.fail("applied"),
        consumer="c",
        on_error="halt",
        cursor=cursors[-1],
        generation=stamps[0],
    )
    assert result.status == "replaced"
