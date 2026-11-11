"""Small stores named by dotted path in the `RAKAIA_STORE` tests."""

from __future__ import annotations

from rakaia import StreamStore


class CustomStore(StreamStore):
    """A store of our own: the in-memory one, with a name to find it by."""


def make_custom_store() -> CustomStore:
    return CustomStore()


def make_something_else() -> object:
    return object()


class NotAStore:
    pass


not_callable = 42
