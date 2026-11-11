"""`scripts/gen_exports.py` writes `__all__` and the TYPE_CHECKING imports from
`_EXPORTS`, so adding a public name is one edit rather than three.
"""

from __future__ import annotations

import ast
import importlib.util
import pathlib
import shutil
import sys

import pytest

ROOT = pathlib.Path(__file__).resolve().parents[2]
PACKAGES = ("rakaia", "django_rakaia")


@pytest.fixture(scope="module")
def gen_exports():
    spec = importlib.util.spec_from_file_location(
        "gen_exports", ROOT / "scripts" / "gen_exports.py"
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules["gen_exports"] = module
    spec.loader.exec_module(module)
    return module


@pytest.fixture
def tree(tmp_path):
    """A copy of the two package roots, which a test is free to damage."""
    for package in PACKAGES:
        target = tmp_path / "src" / package
        target.mkdir(parents=True)
        shutil.copy(ROOT / "src" / package / "__init__.py", target)
    return tmp_path


def _add_export(root: pathlib.Path, package: str, name: str, module: str) -> None:
    path = root / "src" / package / "__init__.py"
    text = path.read_text()
    marker = "_EXPORTS: dict[str, str] = {\n"
    assert marker in text
    path.write_text(text.replace(marker, f'{marker}    "{name}": "{module}",\n', 1))


def test_the_committed_files_are_up_to_date(gen_exports):
    assert gen_exports.main(["--check"], root=ROOT) == 0


@pytest.mark.parametrize("package", PACKAGES)
def test_check_fails_when_an_export_is_not_yet_generated(
    gen_exports, tree, package, capsys
):
    _add_export(tree, package, "BrandNewName", f"{package}.store")
    path = tree / "src" / package / "__init__.py"
    before = path.read_text()

    assert gen_exports.main(["--check"], root=tree) == 1
    assert "just exports" in capsys.readouterr().err
    assert path.read_text() == before, "--check must not rewrite the file"


@pytest.mark.parametrize("package", PACKAGES)
def test_writing_adds_the_name_to_all_and_to_the_type_checking_block(
    gen_exports, tree, package
):
    _add_export(tree, package, "BrandNewName", f"{package}.store")

    assert gen_exports.main([], root=tree) == 0

    text = (tree / "src" / package / "__init__.py").read_text()
    assert '    "BrandNewName",\n' in text
    assert "from .store import BrandNewName" in text
    assert gen_exports.main(["--check"], root=tree) == 0


@pytest.mark.parametrize("package", PACKAGES)
def test_a_removed_export_leaves_both_lists(gen_exports, tree, package):
    path = tree / "src" / package / "__init__.py"
    victim = "StreamStore" if package == "rakaia" else "DjangoStreamStore"
    lines = path.read_text().splitlines(keepends=True)
    path.write_text(
        "".join(line for line in lines if not line.lstrip().startswith(f'"{victim}":'))
    )

    assert gen_exports.main([], root=tree) == 0

    text = path.read_text()
    imported = {
        alias.name
        for node in ast.walk(ast.parse(text))
        if isinstance(node, ast.ImportFrom)
        for alias in node.names
    }
    assert victim not in imported
    assert f'    "{victim}",\n' not in text


def test_the_two_computed_names_stay_in_rakaia_all(gen_exports, tree):
    gen_exports.main([], root=tree)

    text = (tree / "src" / "rakaia" / "__init__.py").read_text()
    assert '    "app",\n' in text and '    "__version__",\n' in text
    assert "app: Any\n" in text and "__version__: str\n" in text
