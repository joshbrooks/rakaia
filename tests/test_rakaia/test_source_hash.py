"""Tests for rakaia.source_hash.hash_function_source (drift-detection hashing)."""

from __future__ import annotations

import pytest

from rakaia.source_hash import hash_function_source


def sample_handler(event):
    return {"value": event["v"]}


def _nested_factory():
    # Same body as the module-level ``sample_handler`` but indented one level.
    # textwrap.dedent must normalise the indentation so both hash identically.
    def sample_handler(event):
        return {"value": event["v"]}

    return sample_handler


class TestHashFunctionSource:
    def test_is_deterministic_sha256_hex(self) -> None:
        h1 = hash_function_source(sample_handler)
        h2 = hash_function_source(sample_handler)
        assert h1 == h2
        assert len(h1) == 64
        assert all(c in "0123456789abcdef" for c in h1)

    def test_indentation_normalised(self) -> None:
        # The drift-detection contract: re-indenting a handler body (e.g. moving
        # it inside a factory) must NOT change its hash.
        assert hash_function_source(sample_handler) == hash_function_source(
            _nested_factory()
        )

    def test_different_body_different_hash(self) -> None:
        def other(event):
            return {"value": event["w"]}

        assert hash_function_source(sample_handler) != hash_function_source(other)

    def test_builtin_without_source_raises_valueerror(self) -> None:
        # Builtins have no retrievable source; inspect raises, which the helper
        # surfaces as a clear ValueError.
        with pytest.raises(ValueError, match="Cannot capture source"):
            hash_function_source(len)
