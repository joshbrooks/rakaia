"""Write each package's `__all__` and `TYPE_CHECKING` imports from `_EXPORTS`.

`_EXPORTS` (name -> defining module) in `src/rakaia/__init__.py` and
`src/django_rakaia/__init__.py` is the one table to edit when adding a public
name. A type checker reads those files without running them, so `__all__` and the
`TYPE_CHECKING` imports have to be written out literally; this script writes them.

Run `just exports` to rewrite the files, or `just exports-check` (this script with
`--check`) to fail when they are out of date. Only the text between the
`# BEGIN generated` and `# END generated` markers is touched. `__all__` is sorted
by name, and imports are grouped by module with the names in isort order.
"""

from __future__ import annotations

import ast
import pathlib
import re
import sys

ROOT = pathlib.Path(__file__).resolve().parent.parent
PACKAGES = ("rakaia", "django_rakaia")

# Names `__getattr__` computes rather than imports: in `__all__`, not `_EXPORTS`.
COMPUTED = {"rakaia": ("app", "__version__"), "django_rakaia": ()}

LINE_LENGTH = 88
INDENT = "    "


def read_exports(source: str) -> dict[str, str]:
    for node in ast.parse(source).body:
        if (
            isinstance(node, ast.AnnAssign)
            and isinstance(node.target, ast.Name)
            and node.target.id == "_EXPORTS"
            and node.value is not None
        ):
            return ast.literal_eval(node.value)
    raise SystemExit("no `_EXPORTS` dict found")


def _isort_key(name: str) -> tuple[int, str]:
    # CONSTANTS, then Classes, then everything else, as ruff's isort does.
    if name.isupper() and len(name) > 1:
        return (0, name.lower())
    return (1 if name[0].isupper() else 2, name.lower())


def render_imports(package: str, exports: dict[str, str]) -> str:
    by_module: dict[str, list[str]] = {}
    for name, module in exports.items():
        prefix = f"{package}."
        if not module.startswith(prefix):
            raise SystemExit(f"{name!r} maps to {module!r}, outside {package}")
        by_module.setdefault(module[len(prefix) :], []).append(name)

    lines: list[str] = []
    for module in sorted(by_module):
        names = sorted(by_module[module], key=_isort_key)
        one_line = f"{INDENT}from .{module} import {', '.join(names)}"
        if len(one_line) <= LINE_LENGTH:
            lines.append(one_line)
            continue
        lines.append(f"{INDENT}from .{module} import (")
        lines.extend(f"{INDENT * 2}{name}," for name in names)
        lines.append(f"{INDENT})")
    return "\n".join(lines) + "\n"


def render_all(package: str, exports: dict[str, str]) -> str:
    names = [*sorted(exports), *COMPUTED[package]]
    return "".join(f'{INDENT}"{name}",\n' for name in names)


def _replace_between_markers(source: str, what: str, body: str) -> str:
    pattern = re.compile(
        rf"(?P<begin>^[ \t]*# BEGIN generated: {re.escape(what)}\b[^\n]*\n)"
        r".*?"
        rf"(?P<end>^[ \t]*# END generated: {re.escape(what)}\b[^\n]*\n)",
        re.DOTALL | re.MULTILINE,
    )
    if not pattern.search(source):
        raise SystemExit(
            f"no `# BEGIN generated: {what}` ... `# END generated: {what}` pair found"
        )
    return pattern.sub(
        lambda m: m["begin"] + body + m["end"],
        source,
        count=1,
    )


def rewrite(package: str, source: str) -> str:
    exports = read_exports(source)
    source = _replace_between_markers(
        source, "imports", render_imports(package, exports)
    )
    return _replace_between_markers(source, "__all__", render_all(package, exports))


def main(argv: list[str] | None = None, root: pathlib.Path = ROOT) -> int:
    args = sys.argv[1:] if argv is None else argv
    check = "--check" in args
    stale: list[pathlib.Path] = []
    for package in PACKAGES:
        path = root / "src" / package / "__init__.py"
        current = path.read_text()
        wanted = rewrite(package, current)
        if wanted == current:
            continue
        stale.append(path)
        if not check:
            path.write_text(wanted)
            print(f"rewrote {path.relative_to(root)}")
    if check and stale:
        names = ", ".join(str(p.relative_to(root)) for p in stale)
        print(
            f"{names}: `__all__` or the TYPE_CHECKING imports do not match "
            "`_EXPORTS`. Run 'just exports' and commit.",
            file=sys.stderr,
        )
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
