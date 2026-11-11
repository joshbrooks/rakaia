"""`rakaia.testing` holds the contract suites a store author runs (#363).

They used to sit in `tests/`, which ships in the sdist and not the wheel, so
nobody who installed the library could hold their own store to them. They are
importable now, and importing the package must not cost anyone a pytest install.
"""

from __future__ import annotations

import importlib
import subprocess
import sys
import textwrap

import pytest

SUITES = {
    "store_contract": "StoreContract",
    "server_store_contract": "ServerStoreContract",
    "executor_contract": "ExecutorContract",
    "outcome_store_contract": "OutcomeStoreContract",
    "projection_reader_contract": "ProjectionReaderContract",
}


@pytest.mark.parametrize(("module", "name"), SUITES.items())
def test_each_suite_is_importable_from_the_package(module, name):
    suite = importlib.import_module(f"rakaia.testing.{module}")
    assert isinstance(getattr(suite, name), type)


def _run_without_pytest(code: str) -> subprocess.CompletedProcess[str]:
    # `None` in `sys.modules` makes `import pytest` raise ImportError, as it
    # would in an environment where pytest is not installed.
    blocked = 'import sys; sys.modules["pytest"] = None\n'
    return subprocess.run(
        [sys.executable, "-c", blocked + textwrap.dedent(code)],
        capture_output=True,
        text=True,
        check=False,
    )


def test_importing_rakaia_and_the_testing_package_needs_no_pytest():
    done = _run_without_pytest("import rakaia, rakaia.testing")
    assert done.returncode == 0, done.stderr


# Every suite that uses pytest; `outcome_store_contract` does not.
NEEDS_PYTEST = [name for name in SUITES if name != "outcome_store_contract"]


@pytest.mark.parametrize("module", NEEDS_PYTEST)
def test_a_suite_that_needs_pytest_says_to_install_it(module):
    done = _run_without_pytest(f"import rakaia.testing.{module}")
    assert done.returncode != 0
    assert "install pytest" in done.stderr


def test_a_failed_check_shows_the_values_it_compared(tmp_path):
    """pytest only explains an `assert` in modules it rewrites, and the suites are
    installed code rather than test files; without that, a failure is a bare
    `AssertionError`."""
    (tmp_path / "test_it.py").write_text(
        textwrap.dedent(
            """
            import pytest
            from rakaia import StreamStore
            from rakaia.testing.store_contract import StoreContract

            class DropsTheFirstMessage(StreamStore):
                def read(self, path, offset=None, **kw):
                    messages, up_to_date = super().read(path, offset, **kw)
                    return messages[1:], up_to_date

            class TestIt(StoreContract):
                @pytest.fixture
                def store(self):
                    return DropsTheFirstMessage()
            """
        )
    )
    done = subprocess.run(
        [
            *[sys.executable, "-m", "pytest", "-q", "-p", "no:cacheprovider"],
            *["-p", "no:django", "-p", "no:asyncio", "-k", "roundtrips_in_order"],
        ],
        cwd=tmp_path,
        capture_output=True,
        text=True,
        check=False,
    )
    assert done.returncode != 0
    assert "{'id': 2}" in done.stdout, done.stdout


def test_the_outcome_suite_imports_without_pytest():
    """The docs say every suite but this one needs pytest to import."""
    done = _run_without_pytest("import rakaia.testing.outcome_store_contract")
    assert done.returncode == 0, done.stderr
