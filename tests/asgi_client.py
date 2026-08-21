"""One way to drive `create_app` over HTTP, for the tests that still need to.

The three-line `httpx.ASGITransport` incantation was written out separately in
`test_rakaia/test_handler.py`, `test_rakaia/test_store_failures.py` and
`test_django_rakaia/test_protocol_server.py`, once per fixture and again inline
for the tests that wanted a shorter long-poll window. It lives here now, next to
the store contracts, since it is the same seam: the protocol served over an
arbitrary store.

Standing up an app is no longer how a response *rule* gets checked — those are
plain function calls in `test_read_decision.py` and `test_producer_response.py`.
What is left here is wiring coverage, which genuinely does need the round trip.
"""

from __future__ import annotations

import httpx

from rakaia import create_app
from rakaia.handler import ServerOptions
from rakaia.protocols import StreamServerStore


def asgi_client(
    store: StreamServerStore,
    *,
    long_poll_timeout: float | None = None,
    enable_fault_injection: bool | None = None,
) -> httpx.AsyncClient:
    """An `httpx` client driving `create_app` over the given store in-process.

    `long_poll_timeout` shortens the server's wait window; the tests that block
    on a long poll or an SSE keep-alive would otherwise pay the default. Both
    options are `None` unless a test says otherwise, so an omitted one keeps
    whatever `ServerOptions` derives — `enable_fault_injection` reads the
    environment, and forcing it here would hide that.
    """
    options = ServerOptions()
    if long_poll_timeout is not None:
        options.long_poll_timeout = long_poll_timeout
    if enable_fault_injection is not None:
        options.enable_fault_injection = enable_fault_injection
    app = create_app(store=store, options=options)
    return httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    )
