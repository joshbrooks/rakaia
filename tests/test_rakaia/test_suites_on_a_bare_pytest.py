"""Each contract suite runs on a bare pytest, as an installed user would run it (#363).

This repository's pytest settings (`asyncio_mode = "auto"`, the Django plugin, a
`pythonpath`) make every suite pass here whether or not it works anywhere else. So
each suite is run in a scratch directory with no config file and with the two
plugins that would paper over a gap switched off, against an implementation that
ships in the package.
"""

from __future__ import annotations

import re
import subprocess
import sys
import textwrap

import pytest

BARE = [
    "-m",
    "pytest",
    "-q",
    "-p",
    "no:cacheprovider",
    "-p",
    "no:django",
    "-p",
    "no:asyncio",
]

CASES = {
    "store_contract": """
        from rakaia import StreamStore
        from rakaia.testing.store_contract import StoreContract

        class TestIt(StoreContract):
            @pytest.fixture
            def store(self):
                return StreamStore()
    """,
    "server_store_contract": """
        from rakaia import StreamStore
        from rakaia.testing.server_store_contract import ServerStoreContract

        class TestIt(ServerStoreContract):
            @pytest.fixture
            def store(self):
                return StreamStore()
    """,
    "executor_contract": """
        from rakaia import InMemoryProjections
        from rakaia.testing.executor_contract import ExecutorContract, ExecutorSeam

        class TestIt(ExecutorContract):
            @pytest.fixture
            def seam(self):
                projections = InMemoryProjections()
                return ExecutorSeam(
                    executor=projections,
                    reader=projections,
                    model="app.Alert",
                    multiple_rows_error=Exception,
                )
    """,
    "projection_reader_contract": """
        from rakaia import InMemoryProjections
        from rakaia.testing.projection_reader_contract import (
            ProjectionReaderContract,
            ReaderSeam,
        )

        class TestIt(ProjectionReaderContract):
            @pytest.fixture
            def seam(self):
                projections = InMemoryProjections()
                return ReaderSeam(
                    apply=projections.apply,
                    make_reader=lambda _effects: projections,
                    model="app.Alert",
                )
    """,
    "outcome_store_contract": """
        from rakaia import InMemoryOutcomeStore
        from rakaia.testing.outcome_store_contract import OutcomeStoreContract

        class TestIt(OutcomeStoreContract):
            @pytest.fixture
            def outcomes(self):
                return InMemoryOutcomeStore()
    """,
}


def run_bare(tmp_path, source: str) -> subprocess.CompletedProcess[str]:
    (tmp_path / "test_it.py").write_text(
        "import pytest\n" + textwrap.dedent(source).lstrip()
    )
    return subprocess.run(
        [sys.executable, *BARE],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )


@pytest.mark.parametrize("suite", CASES)
def test_the_suite_passes_a_built_in_on_a_bare_pytest(suite, tmp_path):
    done = run_bare(tmp_path, CASES[suite])

    assert done.returncode == 0, done.stdout[-3000:] + done.stderr
    assert re.search(r"\b[1-9]\d* passed", done.stdout), done.stdout
