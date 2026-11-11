"""Import pytest for a contract suite, or say plainly that it is missing."""

from __future__ import annotations

try:
    import pytest
except ImportError as exc:
    raise ImportError(
        "rakaia.testing's contract suites need pytest to run: install pytest "
        "(for example `pip install pytest`) in the environment that runs your tests."
    ) from exc

__all__ = ["pytest"]
