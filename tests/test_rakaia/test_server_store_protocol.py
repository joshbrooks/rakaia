"""`StreamServerStore` must describe the calls a server actually makes.

`isinstance(store, StreamServerStore)` is not enough on its own: a
`runtime_checkable` Protocol only checks that the *names* exist, so a declared
signature can disagree with every implementation and with the server calling
them, and nothing fails. That is not hypothetical — this protocol first landed
declaring `close_stream_with_producer(path, options=None)` while both stores
took `(path, producer_id, producer_epoch, producer_seq)` and `handler.py`
passed exactly those three positionally.
"""

from __future__ import annotations

import inspect

import pytest

from rakaia.protocols import StreamServerStore
from rakaia.store import StreamStore

_SERVER_STORE_METHODS = [
    "get",
    "touch",
    "delete",
    "format_response",
    "append_with_producer",
    "close_stream",
    "close_stream_with_producer",
    "wait_for_messages",
]


def _parameters(obj: object, name: str) -> list[str]:
    """The method's parameter names, minus `self`."""
    return [p for p in inspect.signature(getattr(obj, name)).parameters if p != "self"]


class TestTheDeclaredSurfaceMatchesTheImplementation:
    @pytest.mark.parametrize("method", _SERVER_STORE_METHODS)
    def test_the_in_memory_store_takes_what_the_protocol_declares(
        self, method: str
    ) -> None:
        declared = _parameters(StreamServerStore, method)
        actual = _parameters(StreamStore(), method)
        assert declared == actual, (
            f"StreamServerStore.{method}{tuple(declared)} does not match "
            f"StreamStore.{method}{tuple(actual)}"
        )

    def test_the_in_memory_store_satisfies_the_protocol(self) -> None:
        assert isinstance(StreamStore(), StreamServerStore)
