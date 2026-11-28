"""What a reader is sent for a page of messages, decided in one place (#396).

The rules for turning stored events into a response body or a page of live
(SSE) frames are tested here directly, without a server or a connection.
"""

from __future__ import annotations

import json
from typing import Any

from rakaia.read_body import catch_up_body, sse_encoding, sse_frames
from rakaia.types import StreamMessage

JSON = "application/json"


def _msgs(*data: bytes) -> list[StreamMessage]:
    return [
        StreamMessage(data=d, offset=f"{n + 1:04d}", timestamp=0.0)
        for n, d in enumerate(data)
    ]


def _events(body: bytes) -> list[tuple[str, Any]]:
    """`(event, data)` for each frame; control data decoded from JSON."""
    out: list[tuple[str, Any]] = []
    for frame in body.decode().split("\n\n"):
        if not frame:
            continue
        lines = frame.split("\n")
        kind = lines[0].removeprefix("event: ")
        data = "\n".join(line.removeprefix("data:") for line in lines[1:])
        out.append((kind, json.loads(data) if kind == "control" else data))
    return out


def _drop_n(event: Any) -> Any:
    return None if event.get("skip") else {"v": event["n"]}


# -- catch-up ---------------------------------------------------------------


def test_a_json_stream_is_sent_as_one_array() -> None:
    body, headers = catch_up_body(JSON, _msgs(b'{"n":1}', b'{"n":2}'), None)
    assert json.loads(body) == [{"n": 1}, {"n": 2}]
    assert headers == {}


def test_a_text_stream_is_sent_joined() -> None:
    body, _ = catch_up_body("text/plain", _msgs(b"a", b"b"), None)
    assert body == b"ab"


def test_a_reshaped_stream_leaves_skipped_events_out_and_is_not_cached() -> None:
    messages = _msgs(b'{"n":1}', b'{"n":2,"skip":true}', b'{"n":3}')
    body, headers = catch_up_body(JSON, messages, _drop_n)
    assert json.loads(body) == [{"v": 1}, {"v": 3}]
    assert headers == {"cache-control": "no-store"}


def test_a_stream_that_is_not_json_is_never_reshaped() -> None:
    body, headers = catch_up_body("text/plain", _msgs(b"a"), _drop_n)
    assert (body, headers) == (b"a", {})


# -- live frames ------------------------------------------------------------


def _frames(messages: list[StreamMessage], **kw: Any) -> list[tuple[str, Any]]:
    args: dict[str, Any] = {
        "content_type": JSON,
        "tail_offset": messages[-1].offset if messages else "0000",
        "closed": False,
        "up_to_date": True,
        "response_cursor": "c1",
        "reshape": None,
    }
    args.update(kw)
    return _events(sse_frames(messages, **args))


def test_every_data_frame_is_followed_by_its_control_frame() -> None:
    frames = _frames(_msgs(b'{"n":1}', b'{"n":2}'))
    assert [kind for kind, _ in frames] == ["data", "control", "data", "control"]
    assert frames[0][1] == '[{"n":1}]'
    assert frames[1][1] == {"streamNextOffset": "0001", "streamCursor": "c1"}
    assert frames[3][1] == {
        "streamNextOffset": "0002",
        "streamCursor": "c1",
        "upToDate": True,
    }


def test_a_page_with_nothing_new_is_one_control_frame_at_the_tail() -> None:
    frames = _frames([], tail_offset="0007")
    assert frames == [
        (
            "control",
            {"streamNextOffset": "0007", "streamCursor": "c1", "upToDate": True},
        )
    ]


def test_a_closed_stream_at_its_tail_says_so_instead_of_a_cursor() -> None:
    frames = _frames(_msgs(b'{"n":1}'), closed=True)
    assert frames[-1] == ("control", {"streamNextOffset": "0001", "streamClosed": True})


def test_a_skipped_event_sends_nothing_unless_it_is_the_last() -> None:
    messages = _msgs(b'{"n":1,"skip":true}', b'{"n":2}', b'{"n":3,"skip":true}')
    frames = _frames(messages, reshape=_drop_n)
    assert frames == [
        ("data", '[{"v":2}]'),
        ("control", {"streamNextOffset": "0002", "streamCursor": "c1"}),
        (
            "control",
            {"streamNextOffset": "0003", "streamCursor": "c1", "upToDate": True},
        ),
    ]


def test_a_binary_stream_is_sent_as_base64() -> None:
    frames = _frames(_msgs(b"\x00\xff"), content_type="application/octet-stream")
    assert frames[0] == ("data", "AP8=")
    assert sse_encoding("application/octet-stream") == "base64"
    assert sse_encoding(JSON) is None
    assert sse_encoding("text/plain; charset=utf-8") is None


def test_a_line_break_in_a_message_cannot_end_its_frame_early() -> None:
    body = sse_frames(
        _msgs(b"one\r\nevent: control\rtwo"),
        content_type="text/plain",
        tail_offset="0001",
        closed=False,
        up_to_date=True,
        response_cursor="c1",
        reshape=None,
    )
    assert body.startswith(b"event: data\ndata:one\ndata:event: control\ndata:two\n\n")
