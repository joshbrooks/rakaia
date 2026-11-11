"""The "Write your own store" page is true, and so is its promise (#363).

The page's store is not a sketch: this test writes its two code blocks to a
scratch directory exactly as printed and runs the contract suite against the
store with a fresh pytest, which is what a reader who copied them would do. It
also holds every `rakaia` import in the page to the supported surface, so the
page cannot teach a path the public-API page says is free to move.
"""

from __future__ import annotations

import ast
import re
import subprocess
import sys
from pathlib import Path

import pytest

import rakaia

PAGE = Path(__file__).parents[2] / "docs" / "write-your-own-store.md"


def _python_blocks() -> list[str]:
    return re.findall(r"```python\n(.*?)```", PAGE.read_text(), flags=re.DOTALL)


def _block_with(text: str) -> str:
    (block,) = [b for b in _python_blocks() if text in b]
    return block


def test_the_page_imports_only_the_supported_surface():
    blocks = _python_blocks()
    assert blocks, "the page has no python code blocks"
    for block in blocks:
        for node in ast.walk(ast.parse(block)):
            if isinstance(node, ast.Import):
                for alias in node.names:
                    assert not alias.name.startswith("rakaia."), alias.name
            elif isinstance(node, ast.ImportFrom) and node.module:
                if node.module == "rakaia":
                    for alias in node.names:
                        assert alias.name in rakaia.__all__, alias.name
                else:
                    # Only the contract suites are a promised module path.
                    assert not node.module.startswith("rakaia") or (
                        node.module.startswith("rakaia.testing.")
                    ), node.module


def _run_the_contract(tmp_path: Path, store_source: str) -> subprocess.CompletedProcess:
    (tmp_path / "my_store.py").write_text(store_source)
    (tmp_path / "test_my_store.py").write_text(_block_with("StoreContract"))

    return subprocess.run(
        # A bare pytest, as a reader has: this repo's own plugins stay out of it.
        [
            sys.executable,
            "-m",
            "pytest",
            "-q",
            "-p",
            "no:cacheprovider",
            "-p",
            "no:django",
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )


def test_the_store_on_the_page_passes_the_store_contract(tmp_path):
    done = _run_the_contract(tmp_path, _block_with("class ListStore"))

    assert done.returncode == 0, done.stdout + done.stderr
    # Guards against the suite collecting nothing and passing.
    assert re.search(r"\b[1-9]\d* passed", done.stdout), done.stdout


@pytest.mark.parametrize("block", ["class ListStore", "StoreContract"])
def test_the_page_has_the_blocks_this_test_runs(block):
    _block_with(block)


def test_a_store_that_reads_the_clock_another_way_fails_as_the_page_warns(tmp_path):
    source = _block_with("class ListStore")
    assert "import time\n" in source and "time.time()" in source
    other_clock = source.replace("import time\n", "from time import time\n", 1)
    other_clock = other_clock.replace("time.time()", "time()")

    done = _run_the_contract(tmp_path, other_clock)

    assert done.returncode != 0, done.stdout
    assert re.search(r"\b[1-9]\d* failed", done.stdout), done.stdout


def _the_pages_store():
    namespace: dict = {}
    exec(_block_with("class ListStore"), namespace)
    store = namespace["ListStore"]()
    store.create("s")
    return store


def test_the_pages_store_ignores_a_close_as_the_page_says():
    store = _the_pages_store()

    closing = store.append("s", b"1", rakaia.AppendOptions(close=True))
    after = store.append("s", b"2")

    assert closing.message is not None and after.message is not None
    assert [m.data for m in store.read("s")[0]] == [b"1", b"2"]


def test_the_pages_store_does_not_fence_producers_as_the_page_says():
    store = _the_pages_store()
    same = rakaia.AppendOptions(producer_id="p", producer_epoch=0, producer_seq=0)

    store.append("s", b"1", same)
    store.append("s", b"1", same)

    assert len(store.read("s")[0]) == 2
