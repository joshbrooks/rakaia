"""A server with an open and then abandoned SSE connection stops on SIGTERM.

Before the handler read `receive`, uvicorn waited forever for the orphaned
connection's task and needed `kill -9`.
"""

from __future__ import annotations

import signal
import socket
import subprocess
import sys
import time

import pytest


def _free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _request(port: int, request: bytes, wait_for: bytes) -> socket.socket:
    sock = socket.create_connection(("127.0.0.1", port), timeout=5)
    sock.sendall(request)
    seen = b""
    while wait_for not in seen:
        seen += sock.recv(4096)
    return sock


def test_sigterm_stops_the_server_after_an_sse_client_drops():
    pytest.importorskip("uvicorn")
    port = _free_port()
    server = subprocess.Popen(
        [
            sys.executable, "-m", "uvicorn", "rakaia:app",
            "--port", str(port), "--log-level", "warning",
        ],
        stderr=subprocess.PIPE,
    )  # fmt: skip
    try:
        deadline = time.monotonic() + 15
        while True:
            try:
                put = _request(
                    port,
                    b"PUT /s HTTP/1.1\r\nHost: x\r\nContent-Type: text/plain\r\n"
                    b"Content-Length: 0\r\nConnection: close\r\n\r\n",
                    b"201",
                )
                put.close()
                break
            except OSError:
                assert time.monotonic() < deadline, "server did not start"
                time.sleep(0.1)

        sse = _request(
            port,
            b"GET /s?offset=-1&live=sse HTTP/1.1\r\nHost: x\r\n\r\n",
            b"event: control",
        )
        sse.close()  # the client goes away mid-stream
        time.sleep(0.3)

        server.send_signal(signal.SIGTERM)
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            pytest.fail("server ignored SIGTERM with an abandoned SSE connection")
    finally:
        if server.poll() is None:
            server.kill()
            server.wait()
