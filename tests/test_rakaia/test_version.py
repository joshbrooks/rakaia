"""`rakaia.__version__` must be the version that was actually installed.

It was a hard-coded literal, and nothing checked it against `pyproject.toml`. The
publish workflow guards the *tag* against `pyproject.toml`, so a mis-tagged
release cannot ship — but nothing guarded `__version__`, which is the number a
consumer reads at runtime to decide what it is talking to.

That gap was live: `__version__` stayed `"0.1.0"` across 23 commits and 17
substantive changes, several of them breaking. So the string "0.1.0" denoted two
materially different codebases — the sdist on PyPI, and `main`. A consumer
checking `rakaia.__version__` to gate behaviour would have been told the wrong
thing, confidently.

Deriving it from installed package metadata removes the second copy rather than
testing that two copies agree, which is the same move as every other
de-duplication in this codebase: one registration log, one append decision, one
producer response table.
"""

from __future__ import annotations

import pathlib

import pytest
import tomllib

import rakaia


def _pyproject_version() -> str | None:
    """The version declared in `pyproject.toml`, or None when it isn't reachable.

    An installed wheel has no `pyproject.toml` beside it. That is the normal
    case for a consumer, so its absence must skip rather than fail — the point
    of this module is the *source checkout*, where the two can disagree.
    """
    root = pathlib.Path(__file__).resolve().parents[2]
    pyproject = root / "pyproject.toml"
    if not pyproject.is_file():
        return None
    return tomllib.loads(pyproject.read_text())["project"]["version"]


class TestVersionIsSingleSourced:
    def test_version_is_a_non_empty_string(self):
        assert isinstance(rakaia.__version__, str)
        assert rakaia.__version__

    def test_version_matches_pyproject(self):
        """The check that did not exist. `pyproject.toml` is the one declaration;
        `__version__` must be reporting it, not a second hand-maintained copy."""
        declared = _pyproject_version()
        if declared is None:
            pytest.skip("no pyproject.toml beside the package (installed wheel)")
        assert rakaia.__version__ == declared, (
            f"rakaia.__version__ is {rakaia.__version__!r} but pyproject.toml "
            f"declares {declared!r} — bump both, or better, stop maintaining two."
        )

    def test_version_is_exported(self):
        assert "__version__" in rakaia.__all__

    def test_version_is_not_hard_coded_in_the_source(self):
        """The literal is what allowed the drift. Reading it from installed
        metadata means there is nothing to forget to bump."""
        source = (
            pathlib.Path(rakaia.__file__).read_text()  # type: ignore[arg-type]
        )
        assert '__version__ = "' not in source, (
            "__version__ is a hard-coded literal again — derive it from package "
            "metadata so it cannot disagree with pyproject.toml"
        )
