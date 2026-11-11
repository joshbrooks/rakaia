"""An exported name with no section in `scripts/gen_api_reference.py` fails the
generator, instead of landing under "Everything else" where nobody looks.
"""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]


@pytest.fixture
def gen_api_reference(tmp_path, monkeypatch):
    spec = importlib.util.spec_from_file_location(
        "gen_api_reference", ROOT / "scripts" / "gen_api_reference.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    # Never write the real file from a test.
    monkeypatch.setattr(module, "TARGET", tmp_path / "api-reference.md")
    return module


def test_the_committed_tree_is_fully_grouped(gen_api_reference):
    assert gen_api_reference.main() == 0
    assert gen_api_reference.TARGET.exists()


def test_a_name_missing_from_groups_fails_and_is_named(gen_api_reference, capsys):
    gen_api_reference.GROUPS.update(
        {
            title: tuple(n for n in names if n != "StreamStore")
            for title, names in gen_api_reference.GROUPS.items()
        }
    )

    assert gen_api_reference.main() == 1

    err = capsys.readouterr().err
    assert "StreamStore" in err and "GROUPS" in err
    assert not gen_api_reference.TARGET.exists()
