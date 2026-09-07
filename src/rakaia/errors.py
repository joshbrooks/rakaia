"""The base type for what the apply path raises, and the codes it promises.

Everything rakaia raises from an apply inherits `RakaiaError` and carries a
`code` — a short, stable string an operator reads on a failure record. The codes
are a closed, published set (`REASON_CODES`) and are changed with the care any
other public name gets: they are the vocabulary a failure screen counts and
filters on, so moving one silently rewrites history someone is already reading.

The code is **written out**, never derived from the class name. A derivation
would tie an operator's vocabulary to an internal rename, which is the defect
this module exists to remove; `tests/test_rakaia/test_error_codes.py` renames
every class and asserts the code stays put. ADR 0007 Decision 6 is unchanged by
any of this: reason codes a *consumer* records remain the consumer's and stay
opaque to rakaia. This only covers the case where rakaia itself is what failed.
"""

from __future__ import annotations

from typing import ClassVar, Final

UNHANDLED: Final = "unhandled"
"""Recorded for anything that is not one of rakaia's own.

A consumer's own exception is not rakaia's to name, so the loop records this one
code and puts the exception's type name in `params` under `EXCEPTION_TYPE_KEY`.
That keeps the vocabulary countable — one code rather than an open set of class
names — while leaving enough to tell two unanticipated bugs apart.
"""

EXCEPTION_TYPE_KEY: Final = "exception_type"
"""The `params` key holding the type name of an `unhandled` exception.

A documented key, because an operator filtering on it needs it to be stable. The
type *name* only: an exception's message is where field values leak, and this
library is used on submissions carrying personal and financial data.
"""


class RakaiaError(Exception):
    """Base for every failure rakaia raises from the apply path.

    Catch this to catch anything the library itself can raise from an apply,
    instead of enumerating the concrete classes across three modules. Each
    subclass sets `code` to its own promised member of `REASON_CODES`; the code
    lives on the class rather than the instance because it is a property of the
    failure kind, so a reader can look it up without constructing one.

    The base defaults to `UNHANDLED` so an exception from outside this closed
    set can never break the recording path, and a test walks the subclasses to
    ensure no rakaia error is left sitting on that default.
    """

    code: ClassVar[str] = UNHANDLED


REASON_CODES: Final = frozenset(
    {
        "handler_gap",
        "upcaster_chain",
        "effect_collision",
        "unresolved_ref",
        "duplicate_produces",
        "handler_drift",
        "missing_reader",
        "undecodable_event",
        "merge_key",
        UNHANDLED,
    }
)
"""Every reason code rakaia records for a failure of its own, plus `unhandled`.

Closed and published. Adding one is a public API change; removing or renaming
one breaks whatever an operator has been counting.
"""
