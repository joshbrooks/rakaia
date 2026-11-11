"""The README's no-Django replay quick start must run and print what it says it prints.

A snippet in prose rots silently; this one is executed. It is found by its heading, so
moving it is fine and deleting it fails here rather than going unnoticed.
"""

from __future__ import annotations

import re
import runpy
from pathlib import Path

README = Path(__file__).resolve().parents[2] / "README.md"


def _snippet_under(heading: str) -> str:
    text = README.read_text()
    section = text.split(f"## {heading}", 1)[1]
    match = re.search(r"```python\n(.*?)```", section, re.DOTALL)
    assert match, f"no python block under {heading!r}"
    return match.group(1)


def test_replay_quick_start_prints_the_rows_it_claims(capsys, tmp_path):
    code = _snippet_under("Quick start (replay, no Django)")
    expected = code.split("# ")[-1].strip()  # the trailing "# [...]" comment
    # An empty expectation would match a snippet that prints nothing.
    assert expected.startswith("[{"), f"no expected rows after the print: {expected!r}"

    # Handlers fingerprint their own source, so the snippet has to be a real file.
    snippet = tmp_path / "readme_snippet.py"
    snippet.write_text(code)
    runpy.run_path(str(snippet))

    assert capsys.readouterr().out.strip() == expected
