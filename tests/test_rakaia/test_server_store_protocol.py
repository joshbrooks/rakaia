"""`StreamServerStore` must describe the calls a server actually makes.

`isinstance(store, StreamServerStore)` is not enough on its own: a
`runtime_checkable` Protocol only checks that the *names* exist, so a declared
signature can disagree with every implementation and with the server calling
them, and nothing fails. That is not hypothetical — this protocol first landed
declaring `close_stream_with_producer(path, options=None)` while both stores
took `(path, producer_id, producer_epoch, producer_seq)` and `handler.py`
passed exactly those three positionally.

The check is `inspect.Signature.bind`, not name-list equality: every call the
declared signature permits must bind on the implementation. That covers the
inherited methods too (name-list equality skipped them), and it allows an
implementation to accept a *superset* — extra defaulted parameters of its own
— which is a valid way to satisfy a Protocol.
"""

from __future__ import annotations

import inspect
from typing import Any

import pytest

from rakaia.protocols import StreamServerStore
from rakaia.store import StreamStore


def _protocol_methods() -> list[str]:
    """Every method the protocol declares, inherited ones included."""
    return sorted(
        name
        for name in dir(StreamServerStore)
        if not name.startswith("_")
        and inspect.isfunction(getattr(StreamServerStore, name))
    )


def _declared(method: str) -> inspect.Signature:
    return inspect.signature(getattr(StreamServerStore, method))


def _calls_the_declaration_permits(
    sig: inspect.Signature,
) -> list[tuple[list[Any], dict[str, Any]]]:
    """The extreme calls a declared signature allows: required-only and all-in.

    Dummy values stand in for real arguments — `bind` checks shape, not types.
    If both extremes bind on an implementation, everything between them does
    too (parameters are independent once positional order is fixed).
    """
    minimal_args: list[Any] = []
    minimal_kwargs: dict[str, Any] = {}
    maximal_args: list[Any] = []
    maximal_kwargs: dict[str, Any] = {}
    for p in sig.parameters.values():
        if p.name == "self":
            continue
        if p.kind in (p.POSITIONAL_ONLY, p.POSITIONAL_OR_KEYWORD):
            maximal_args.append(p.name)
            if p.default is p.empty:
                minimal_args.append(p.name)
        elif p.kind is p.KEYWORD_ONLY:
            maximal_kwargs[p.name] = p.name
            if p.default is p.empty:
                minimal_kwargs[p.name] = p.name
    return [(minimal_args, minimal_kwargs), (maximal_args, maximal_kwargs)]


class TestTheDeclaredSurfaceMatchesTheImplementation:
    @pytest.mark.parametrize("method", _protocol_methods())
    def test_every_declared_call_binds_on_the_in_memory_store(
        self, method: str
    ) -> None:
        impl = inspect.signature(getattr(StreamStore(), method))
        for args, kwargs in _calls_the_declaration_permits(_declared(method)):
            try:
                impl.bind(*args, **kwargs)
            except TypeError as e:
                pytest.fail(
                    f"StreamServerStore.{method} permits a call StreamStore "
                    f"cannot take: {method}(*{args}, **{kwargs}) — {e}"
                )

    def test_the_surface_includes_the_inherited_methods(self) -> None:
        """The dynamic enumeration must not quietly shrink.

        Everything `handler.py` calls on its store has to appear here; if a
        method leaves this list the bind check above stops policing it.
        """
        assert set(_protocol_methods()) >= {
            "read",
            "has",
            "create",
            "append",
            "append_many",
            "get",
            "touch",
            "delete",
            "format_response",
            "append_with_producer",
            "close_stream",
            "close_stream_with_producer",
            "wait_for_messages",
        }

    def test_the_in_memory_store_satisfies_the_protocol(self) -> None:
        assert isinstance(StreamStore(), StreamServerStore)
