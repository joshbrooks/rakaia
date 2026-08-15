"""The meta-stream that remembers what has been registered.

A registry persists each registration to an append-only meta-stream, so a fresh
process can re-import the modules holding those registrations rather than relying
on import side effects having happened in the right order.

That mechanism was written three times — once for handlers, once for reducers,
once for upcasters — as a trio of `_ensure_stream` / `_load_persisted_ids` /
`_persist_if_new` methods plus a hand-mirrored *pair* of module-level identity
functions per kind: one building a tuple from the object, one rebuilding the same
tuple from stored JSON. Six functions that had to agree, with nothing checking
that they did.

They were also read back **positionally**. `rehydrate()` did `ident[4]` for a
handler's dotted path and `ident[2]` for a reducer's, and two of the builders
carried comments telling future editors to append new fields at the end so those
indices stayed valid. Adding one field to a registration meant editing four
functions and re-checking two index comments in a third.

Here the mechanism exists once and each record kind owns its own identity and
payload (`RegistrationRecord` below), so the round trip is a property of the type
rather than an agreement between two functions — and the dotted path is read by
name, so field order stops being load-bearing.
"""

from __future__ import annotations

import json
from typing import Any, Protocol, runtime_checkable

from .protocols import WritableStore


@runtime_checkable
class RegistrationRecord(Protocol):
    """A registration that knows how to identify and serialize itself.

    Three types satisfy it — `HandlerVersion`, `ReducerVersion`,
    `UpcasterVersion`. Keeping all three parts on the type is the point: an
    identity built in one place and rebuilt in another is exactly the pair that
    used to drift. All three now *derive* the three methods from one declared
    field list (`rakaia.registry._MetaStreamRecord`), so the round trip is a
    property of the declaration rather than of three methods agreeing.

    `to_payload` must round-trip through **JSON**, not just through a dict: a
    `frozenset` event match has to serialize as a sorted list (sorted, so the
    meta-stream doesn't change with set iteration order and re-append on every
    restart), and `identity_from_payload` has to rebuild the frozenset from it.
    """

    @property
    def identity(self) -> tuple:
        """The dedup key. Two registrations with the same identity are the same
        registration."""
        ...

    def to_payload(self) -> dict[str, Any]:
        """The JSON-serializable form written to the meta-stream.

        Must include ``registered_in`` — the module that made the registration,
        which is what `RegistrationLog.modules` re-imports — and
        ``dotted_path``, which says where the logic lives, for drift reporting.
        They are different modules whenever a function is wired up somewhere
        other than where it is defined.
        """
        ...

    @classmethod
    def identity_from_payload(cls, payload: dict[str, Any]) -> tuple:
        """Rebuild `identity` from a payload this type wrote.

        Must tolerate payloads written by older versions that predate a field —
        meta-streams already in the wild still have to load.
        """
        ...


class RegistrationLog:
    """Append-only record of registrations of one kind, on one stream.

    Construct one per (store, stream, record type), call `load()` once, then
    `record()` per registration. A `store` of `None` makes every operation a
    no-op, so a registry without persistence needs no special-casing at its call
    sites.
    """

    def __init__(
        self,
        store: WritableStore | None,
        stream_path: str,
        record_type: type[RegistrationRecord] | Any,
    ) -> None:
        self._store = store
        self._stream_path = stream_path
        self._record_type = record_type
        self._known: set[tuple] = set()
        self._payloads: list[dict[str, Any]] = []

    def load(self) -> None:
        """Create the stream if absent and read back what is already recorded.

        The meta-stream declares no content type, so each append is a standalone
        blob (one JSON object per message) rather than being treated as one
        flattened JSON array — which is what JSON mode would do.

        `create()` is called unconditionally: creation is idempotent by contract
        and cannot truncate a populated stream.
        """
        if self._store is None:
            return
        self._store.create(self._stream_path)
        messages, _ = self._store.read(self._stream_path)
        for msg in messages:
            try:
                payload = json.loads(msg.data)
            except (ValueError, UnicodeDecodeError):
                # A message this log did not write, or wrote in a format it no
                # longer understands. Skipping beats refusing to start.
                continue
            self._known.add(self._record_type.identity_from_payload(payload))
            self._payloads.append(payload)

    def record(self, item: RegistrationRecord) -> bool:
        """Append `item` unless an identical registration is already recorded.

        Returns whether it was appended.
        """
        if self._store is None:
            return False
        identity = item.identity
        if identity in self._known:
            return False
        payload = item.to_payload()
        self._store.append(self._stream_path, json.dumps(payload).encode("utf-8"))
        self._known.add(identity)
        self._payloads.append(payload)
        return True

    def known(self) -> set[tuple]:
        """Every recorded identity."""
        return set(self._known)

    def modules(self) -> set[str]:
        """The module to re-import for every recorded registration.

        ``registered_in`` — *where the decorator ran*, which is the only thing
        importing can re-run. This used to be derived from ``dotted_path`` by
        chopping the last segment off, which quietly assumed the function was
        defined in the module that registered it and had exactly one qualname
        segment. Neither holds for a `functools.partial` wired up in an app's
        ``handlers.py``, nor for a handler defined as a method, and the failure
        was a registration that simply did not come back.

        Payloads written before ``registered_in`` existed fall back to the old
        derivation, which is correct for exactly the cases that used to work.
        """
        return {
            module
            for module in (self._module_of(payload) for payload in self._payloads)
            if module
        }

    @staticmethod
    def _module_of(payload: dict[str, Any]) -> str | None:
        site = payload.get("registered_in")
        if site:
            return str(site)
        dotted_path = payload.get("dotted_path")
        if not dotted_path:
            return None
        return str(dotted_path).rsplit(".", 1)[0]

    def reset(self) -> None:
        """Forget what has been recorded, without touching the stream.

        For test isolation: a registry reset drops its in-memory dedup state, and
        a subsequent `load()` rebuilds it from the stream.
        """
        self._known.clear()
        self._payloads.clear()
