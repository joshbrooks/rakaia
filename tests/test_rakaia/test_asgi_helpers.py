"""Tests for rakaia._asgi helpers."""

from __future__ import annotations

from rakaia._asgi import (
    get_all_query_params,
    get_header,
    get_method,
    get_path,
    get_query_param,
    get_query_string,
    make_headers,
    parse_query_params,
    read_body,
    send_body_chunk,
    send_response,
    send_sse_event,
    start_streaming_response,
)


class _MockReceive:
    def __init__(self, chunks: list[dict]):
        self._chunks = list(chunks)

    async def __call__(self) -> dict:
        return self._chunks.pop(0)


class _MockSend:
    def __init__(self):
        self.messages: list[dict] = []

    async def __call__(self, message: dict) -> None:
        self.messages.append(message)


class TestReadBody:
    async def test_single_chunk(self):
        receive = _MockReceive([{"body": b"hello", "more_body": False}])
        body = await read_body(receive)
        assert body == b"hello"

    async def test_multiple_chunks(self):
        receive = _MockReceive(
            [
                {"body": b"foo", "more_body": True},
                {"body": b"bar", "more_body": True},
                {"body": b"baz", "more_body": False},
            ]
        )
        body = await read_body(receive)
        assert body == b"foobarbaz"

    async def test_empty_body(self):
        receive = _MockReceive([{"body": b"", "more_body": False}])
        body = await read_body(receive)
        assert body == b""


class TestScopeAccessors:
    def test_get_method_uppercase(self):
        assert get_method({"method": "get"}) == "GET"
        assert get_method({"method": "Post"}) == "POST"

    def test_get_method_default(self):
        assert get_method({}) == "GET"

    def test_get_path(self):
        assert get_path({"path": "/foo"}) == "/foo"

    def test_get_path_default(self):
        assert get_path({}) == "/"

    def test_get_query_string(self):
        assert get_query_string({"query_string": b"a=1&b=2"}) == "a=1&b=2"

    def test_get_query_string_default(self):
        assert get_query_string({}) == ""


class TestQueryParams:
    def test_parse_query_params(self):
        result = parse_query_params("a=1&b=2&a=3")
        assert result == {"a": ["1", "3"], "b": ["2"]}

    def test_get_query_param_single(self):
        assert get_query_param("a=1&b=2", "a") == "1"

    def test_get_query_param_first_of_multiple(self):
        assert get_query_param("a=1&a=2", "a") == "1"

    def test_get_query_param_missing(self):
        assert get_query_param("a=1", "b") is None

    def test_get_query_param_blank_value(self):
        assert get_query_param("flag=", "flag") == ""

    def test_get_all_query_params(self):
        assert get_all_query_params("a=1&a=2&a=3", "a") == ["1", "2", "3"]

    def test_get_all_query_params_missing(self):
        assert get_all_query_params("a=1", "b") == []


class TestGetHeader:
    def test_case_insensitive_lookup(self):
        scope = {"headers": [(b"content-type", b"application/json")]}
        assert get_header(scope, "Content-Type") == "application/json"
        assert get_header(scope, "content-type") == "application/json"
        assert get_header(scope, "CONTENT-TYPE") == "application/json"

    def test_missing_header(self):
        scope = {"headers": [(b"content-type", b"application/json")]}
        assert get_header(scope, "X-Custom") is None

    def test_empty_headers(self):
        assert get_header({}, "Content-Type") is None


class TestMakeHeaders:
    def test_simple_dict(self):
        result = make_headers({"Content-Type": "application/json"})
        assert result == [(b"content-type", b"application/json")]

    def test_empty(self):
        assert make_headers({}) == []


class TestSendResponse:
    async def test_sends_start_and_body(self):
        send = _MockSend()
        await send_response(send, 200, {"X-Test": "yes"}, b"hello")

        assert len(send.messages) == 2
        assert send.messages[0]["type"] == "http.response.start"
        assert send.messages[0]["status"] == 200
        assert (b"x-test", b"yes") in send.messages[0]["headers"]
        assert send.messages[1]["type"] == "http.response.body"
        assert send.messages[1]["body"] == b"hello"

    async def test_no_headers_no_body(self):
        send = _MockSend()
        await send_response(send, 204)
        assert send.messages[0]["status"] == 204
        assert send.messages[1]["body"] == b""


class TestStreamingResponse:
    async def test_start_streaming(self):
        send = _MockSend()
        await start_streaming_response(send, 200, {"Content-Type": "text/event-stream"})
        assert len(send.messages) == 1
        assert send.messages[0]["type"] == "http.response.start"

    async def test_send_body_chunk(self):
        send = _MockSend()
        await send_body_chunk(send, b"chunk1", more_body=True)
        assert send.messages[0]["body"] == b"chunk1"
        assert send.messages[0]["more_body"] is True


class TestSendSseEvent:
    async def test_simple_event(self):
        send = _MockSend()
        await send_sse_event(send, "message", "hello")

        # SSE format
        body = send.messages[0]["body"].decode("utf-8")
        assert "event: message" in body
        assert "data:hello" in body
        assert body.endswith("\n\n")

    async def test_multiline_data(self):
        send = _MockSend()
        await send_sse_event(send, "msg", "line1\nline2\nline3")

        body = send.messages[0]["body"].decode("utf-8")
        # Each line gets its own data: prefix
        assert body.count("data:") == 3
        assert "data:line1" in body
        assert "data:line2" in body
        assert "data:line3" in body
