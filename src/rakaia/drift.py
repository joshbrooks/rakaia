"""
One place that answers: has the code behind a stored rule changed since it was
registered?

A handler, a reducer and an upcaster are all *rules* recorded with the hash of
the source they were registered with. If that source has since been edited, a
replay produces something different from what was recorded, so we say so. The
three checks were written out three times — in `replay()` for handlers, again for
reducers, and again as a closure handed to `UpcasterRegistry.apply_chain` — and
differed only in the word naming the kind. Reading the warning meant reading all
three (#187).

Worse, the entry point into the third one took **two** options that had to
agree: a `drift_callback` (the report) and a `hasher` (the memo). Pass the
callback and forget the hasher and every event re-read the source; pass the
hasher and forget the callback and the check was skipped in silence. The correct
pairing existed in exactly one place, deep inside `replay()`.

`DriftLedger` is that pairing, made into a thing. One object owns all three
questions — *has this rule's code drifted*, *have I already said so*, and *what
have I hashed already* — so there is one check, and one option to pass instead of
two that can disagree.

Scope: a ledger is made per replay, not shared process-wide. A long-lived process
that reloads code between replays must still be able to detect the drift that
reload introduced, so the memo must not outlive the run that took it.
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any, Literal

from .source_hash import hash_function_source

# Deliberately the logger `replay()` has always warned through: `RAKAIA_DRIFT`
# lines keep arriving on the name operators already filter on.
_log = logging.getLogger("rakaia.replay")

OnDriftPolicy = Literal["warn", "raise"]
"""What to do when a rule's source no longer matches its registered hash."""


class HandlerDriftError(Exception):
    """A handler/upcaster's source body differs from its registered hash."""


@dataclass
class DriftLedger:
    """The drift check, its warn-once memory, and its hash memo, in one object.

    `check()` is the only entry point. It is deliberately the *same* call for a
    handler, a reducer and an upcaster — `kind` is a word in the message, not a
    branch — so the three cannot drift apart from each other.
    """

    on_drift: OnDriftPolicy = "warn"

    warnings: list[str] = field(default_factory=list)
    """The `RAKAIA_DRIFT` lines emitted, one per drifted registration."""

    drifted: list[str] = field(default_factory=list)
    """Names of the drifted rules, deduplicated, in the order first seen."""

    # Source hashes computed under this ledger, keyed by the callable.
    #
    # Drift is a property of a *registration*, not of an event: a rule's source
    # cannot change while its own replay is running. Checking it per event meant
    # `inspect.getsource` + SHA-256 for every (event x version) — measured at
    # ~86% of total replay time on a 2000-event stream (#156).
    _hashes: dict[Any, str] = field(default_factory=dict, repr=False, init=False)

    # Registrations already reported, so the warning is emitted once rather than
    # once per event. `drifted` already dedupes by name; `warnings` and the log
    # did not.
    #
    # Keyed by `(kind, name, stored_hash)` — a *registration*, not a name. A
    # handler name is deliberately stable across its versions, so keying by name
    # alone would report the first drifted version of a name and silently drop
    # every later one. The stored hash is what distinguishes two registrations
    # that share a name.
    _reported: set[tuple[str, str, str]] = field(
        default_factory=set, repr=False, init=False
    )

    def live_hash(self, fn: Any) -> str:
        """The source hash of `fn`, computed at most once per ledger.

        The expensive part of a drift check — `inspect.getsource` and a SHA-256 —
        depends only on the callable, so it is memoised. The *comparison* stays
        per registration, because two registrations may legitimately share a
        function while storing different hashes.
        """
        cached = self._hashes.get(fn)
        if cached is None:
            cached = hash_function_source(fn)
            self._hashes[fn] = cached
        return cached

    def check(self, *, kind: str, name: str, stored_hash: str, fn: Any) -> bool:
        """Compare `fn`'s live source hash against the hash stored for it.

        Returns whether this rule has drifted (whether or not it was reported —
        the second sighting of the same registration is silent). Raises
        `HandlerDriftError` instead of warning when `on_drift == "raise"`, which
        is what `--strict-drift` asks for: a first sighting is enough to fail.
        """
        if self.live_hash(fn) == stored_hash:
            return False
        key = (kind, name, stored_hash)
        if key in self._reported:
            return True
        self._reported.add(key)
        message = (
            f"RAKAIA_DRIFT {kind}={name!r} stored={stored_hash[:12]} "
            f"current={self.live_hash(fn)[:12]}"
        )
        if self.on_drift == "raise":
            raise HandlerDriftError(message)
        if name not in self.drifted:
            self.drifted.append(name)
        self.warnings.append(message)
        _log.warning(message)
        return True
