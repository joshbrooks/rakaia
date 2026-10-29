"""
Fingerprints for drift detection on versioned handlers and upcasters.

A fingerprint covers the function's own source *and* what it reaches: the plain
functions it calls, and the constants it reads (module globals, closure cells,
default arguments). It is captured at registration; later re-resolution compares
it against the live function's fingerprint and warns (or raises) on mismatch.

A handler that reaches something we cannot fingerprint (a mutable global, a
module outside the standard library and installed packages, a class that is not
a dataclass, an `import` or `eval` in the body) is *impure*: an edit to that
thing would go unnoticed. Registration warns, naming each such thing, or raises
under `RAKAIA_STRICT_HANDLER_FINGERPRINTS`.
"""

from __future__ import annotations

import datetime
import decimal
import dis
import enum
import fractions
import functools
import hashlib
import inspect
import logging
import os
import re
import sys
import textwrap
import types
import uuid
from dataclasses import dataclass, is_dataclass
from typing import Any

FINGERPRINT_PREFIX = "v2:"
"""Fingerprints before 0.10 were a bare SHA-256 of the handler's own source."""

STRICT_ENV_VAR = "RAKAIA_STRICT_HANDLER_FINGERPRINTS"


def unwrap_handler(fn: Any) -> Any:
    """The function a callable's identity and source should be taken from.

    A `functools.partial` is the supported way to bind a dependency into a
    handler — a stage-0 handler is called `fn(event)`, so there is nowhere else
    to put one. Unwrapping it means `dotted_path` and `source_hash` describe the
    *wrapped* function, which is where the logic lives:

    * the recorded path stays importable, so `rehydrate()` can resolve it;
    * the hash tracks edits to the real body, so drift detection still works;
    * rebinding a different dependency is correctly *not* a logic change.

    Nested partials unwrap all the way down. Anything else is returned as-is.
    """
    while isinstance(fn, functools.partial):
        fn = fn.func
    return fn


def is_importable(fn: Any) -> bool:
    """Whether this callable's qualified name could be imported back.

    False for a closure or a lambda defined inside a function — its qualname
    carries a `<locals>` segment, and `rehydrate()` imports the module part of
    the recorded `dotted_path`, which for such a name is not a module.
    """
    qualname = getattr(unwrap_handler(fn), "__qualname__", "")
    return "<locals>" not in qualname


class ImpureHandlerError(ValueError):
    """A handler cannot be fingerprinted and strict fingerprints are on."""


class ImpureHandlerWarning(UserWarning):
    """A handler cannot be fully fingerprinted: edits to part of it go unnoticed."""


@dataclass(frozen=True)
class Fingerprint:
    digest: str
    """`"v2:"` plus a SHA-256 hex digest."""

    impure: tuple[str, ...] = ()
    """What the handler reaches that could not be fingerprinted, one line each."""


_strict_override: bool | None = None


def set_strict_handler_fingerprints(value: bool | None) -> None:
    """Set the process-wide default. Django's `ready()` calls this from settings.

    `None` goes back to the environment variable.
    """
    global _strict_override
    _strict_override = value


def strict_handler_fingerprints() -> bool:
    """Whether an impure handler is an error rather than a warning.

    Core has no settings object, so like `RAKAIA_ENABLE_FAULT_INJECTION` this is
    an environment variable; the Django app copies the setting of the same name
    over it.
    """
    if _strict_override is not None:
        return _strict_override
    return os.environ.get(STRICT_ENV_VAR, "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }


_LEGACY = re.compile(r"[0-9a-f]{64}")


def is_legacy_hash(stored: str) -> bool:
    """A fingerprint recorded before the `v2:` format: unknown, not drifted."""
    return _LEGACY.fullmatch(stored) is not None


# Library value types that are immutable: a constant like `Decimal("0.15")`.
_VALUE_TYPES = (
    decimal.Decimal,
    fractions.Fraction,
    uuid.UUID,
    datetime.date,
    datetime.datetime,
    datetime.time,
    datetime.timedelta,
)

_UNSAFE_NAMES = frozenset({"eval", "exec", "compile", "globals", "__import__"})
_LIBRARY_ROOTS = frozenset(sys.stdlib_module_names) | {"rakaia", "django_rakaia"}
_NAME_OPS = frozenset({"LOAD_GLOBAL", "LOAD_NAME"})


def _is_library(module: str | None, filename: str | None) -> bool:
    """Code whose version is pinned by the environment, not edited with the handler.

    The standard library, rakaia itself and installed packages: their changes
    arrive with an upgrade, which the lockfile records.
    """
    if module and module.partition(".")[0] in _LIBRARY_ROOTS:
        return True
    return bool(filename) and any(
        part in (filename or "") for part in ("site-packages", "dist-packages")
    )


def _immutable_text(value: Any) -> str | None:
    if value is None or type(value) in (bool, int, float, complex, str, bytes):
        return repr(value)
    if isinstance(value, _VALUE_TYPES):
        return f"{type(value).__qualname__}({value!r})"
    if isinstance(value, enum.Enum):
        inner = _immutable_text(value.value)
        return f"{type(value).__qualname__}.{value.name}" + (
            f"={inner}" if inner is not None else ""
        )
    if isinstance(value, (tuple, frozenset)) and type(value) in (tuple, frozenset):
        parts = [_immutable_text(v) for v in value]
        if any(p is None for p in parts):
            return None
        texts = [p for p in parts if p is not None]
        if isinstance(value, frozenset):
            texts.sort()
        return f"{type(value).__name__}({', '.join(texts)})"
    return None


def _source_of(obj: Any) -> str | None:
    try:
        return textwrap.dedent(inspect.getsource(obj))
    except (OSError, TypeError):
        return None


class _Reach:
    """Everything one handler reaches, collected for hashing."""

    def __init__(self) -> None:
        self.parts: list[tuple[str, str]] = []
        self.impure: list[str] = []
        self._seen: set[int] = set()

    def function(self, label: str, fn: Any) -> None:
        if isinstance(fn, (staticmethod, classmethod)):
            fn = fn.__func__
        fn = getattr(fn, "__func__", fn)  # bound method
        if id(fn) in self._seen:
            return
        self._seen.add(id(fn))
        source = _source_of(fn)
        if source is None:
            self.impure.append(f"{label}: no source available")
            return
        self.parts.append((label, source))
        code = getattr(fn, "__code__", None)
        if code is None:
            return
        self._code(label, code, fn.__globals__)
        for name, cell in zip(code.co_freevars, fn.__closure__ or (), strict=True):
            try:
                self.value(name, cell.cell_contents)
            except ValueError:  # empty cell
                continue
        for i, default in enumerate(fn.__defaults__ or ()):
            self.value(f"{label} default {i}", default)
        for key, default in sorted((fn.__kwdefaults__ or {}).items()):
            self.value(f"{label} default {key}", default)

    def _code(self, label: str, code: types.CodeType, globs: dict[str, Any]) -> None:
        names: set[str] = set()
        stack = [code]
        while stack:
            current = stack.pop()
            for ins in dis.get_instructions(current):
                if ins.opname == "IMPORT_NAME":
                    self.impure.append(
                        f"{label}: imports {ins.argval!r} inside the body"
                    )
                elif ins.opname in _NAME_OPS and isinstance(ins.argval, str):
                    names.add(ins.argval)
            stack.extend(c for c in current.co_consts if isinstance(c, types.CodeType))
        for name in sorted(names):
            if name in _UNSAFE_NAMES:
                self.impure.append(f"{label}: calls {name}()")
            elif name in globs:
                self.value(name, globs[name])

    def value(self, label: str, value: Any) -> None:
        text = _immutable_text(value)
        if text is not None:
            self.parts.append((label, text))
        elif isinstance(value, (types.FunctionType, staticmethod)):
            fn = value.__func__ if isinstance(value, staticmethod) else value
            if not _is_library(
                getattr(fn, "__module__", None), fn.__code__.co_filename
            ):
                self.function(label, fn)
        elif isinstance(value, types.BuiltinFunctionType):
            return
        elif isinstance(value, types.ModuleType):
            if not _is_library(value.__name__, getattr(value, "__file__", None)):
                self.impure.append(f"{label}: module {value.__name__!r}")
        elif isinstance(value, type):
            self._class(label, value)
        elif isinstance(value, logging.Logger):
            return
        else:
            self.impure.append(f"{label}: {type(value).__name__} instance")

    def _class(self, label: str, cls: type) -> None:
        """A class is fine when only its definition matters.

        Handlers construct event and effect types, so flagging every class would
        flag every handler. A class from the standard library, rakaia or an
        installed package is pinned by the environment. A dataclass, an enum or a
        named tuple defined in user code is plain data: its source is its whole
        behaviour, so it goes into the fingerprint. Any other class (a Django
        model, a service with methods that read state) can behave differently
        without its source being the thing that changed, so it is impure.
        """
        if _is_library(cls.__module__, None):
            return
        if (
            is_dataclass(cls)
            or issubclass(cls, enum.Enum)
            or (issubclass(cls, tuple) and hasattr(cls, "_fields"))
        ):
            if id(cls) in self._seen:
                return
            self._seen.add(id(cls))
            source = _source_of(cls)
            if source is not None:
                self.parts.append((label, source))
                return
        self.impure.append(f"{label}: class {cls.__qualname__}")


def fingerprint_handler(fn: Any) -> Fingerprint:
    """The fingerprint of `fn` and whatever it reaches, plus what it could not cover.

    The digest hashes the dedented source of `fn` and of every plain function it
    reaches, and the value of every constant it reads, ordered by the name each
    was reached by (not the module it lives in, so re-importing under another name
    changes nothing). Source text rather than bytecode keeps the digest stable
    across Python versions; the cost is that a comment-only edit still counts as a
    change, as it always has.

    A `functools.partial` is unwrapped first (see `unwrap_handler`).
    """
    fn = unwrap_handler(fn)
    try:
        inspect.getsource(fn)
    except (OSError, TypeError) as exc:
        raise ValueError(
            f"Cannot capture source for {fn!r}: {exc}. "
            "Versioned handlers must be defined in importable source files."
        ) from exc
    reach = _Reach()
    reach.function("<handler>", fn)
    digest = hashlib.sha256()
    for label, text in sorted(reach.parts):
        digest.update(f"{label}\0{text}\0".encode())
    return Fingerprint(
        FINGERPRINT_PREFIX + digest.hexdigest(), tuple(sorted(set(reach.impure)))
    )


def hash_function_source(fn: Any) -> str:
    """The fingerprint digest of `fn` (see `fingerprint_handler`)."""
    return fingerprint_handler(fn).digest
