"""Every setting and management command the library ships is named in `okf/`.

Nothing else checks this: `test_docs_names_resolve.py` gates examples only, so a
change that adds a setting or a command passes the suite with the machine-readable
bundle silent about it.
"""

from __future__ import annotations

import pathlib
import re

ROOT = pathlib.Path(__file__).resolve().parents[2]
# A setting is read by name: `getattr(settings, "X")`, `os.environ.get("X")` or
# `settings.X`. A bare mention is not enough: `RAKAIA_DRIFT` is a log-line
# prefix, not something a consumer sets.
SETTING = re.compile(r"""["'](RAKAIA_[A-Z0-9_]+)["']|\bsettings\.(RAKAIA_[A-Z0-9_]+)""")


def _okf_text() -> str:
    # Not `log.md`: it records that something was added, and would go on
    # claiming it after a page dropped the name.
    return "\n".join(
        p.read_text() for p in (ROOT / "okf").rglob("*.md") if p.name != "log.md"
    )


def _settings_read_in_src() -> set[str]:
    return {
        quoted or attribute
        for path in (ROOT / "src").rglob("*.py")
        for quoted, attribute in SETTING.findall(path.read_text())
    }


def _management_commands() -> set[str]:
    commands = ROOT / "src" / "django_rakaia" / "management" / "commands"
    return {p.stem for p in commands.glob("*.py") if p.stem != "__init__"}


def test_the_scan_finds_something():
    # A broken path or pattern would make the two checks below pass on nothing.
    assert "RAKAIA_STORE" in _settings_read_in_src()
    assert "prune_orphan_events" in _management_commands()


def test_every_setting_is_mentioned_in_okf():
    text = _okf_text()
    missing = sorted(n for n in _settings_read_in_src() if n not in text)
    assert not missing, (
        f"settings read in src/ but absent from okf/: {missing}. Add each to the "
        "concept page that owns it, and a dated entry to okf/log.md."
    )


def test_every_management_command_is_mentioned_in_okf():
    text = _okf_text()
    missing = sorted(
        c for c in _management_commands() if not re.search(rf"\b{c}\b", text)
    )
    assert not missing, (
        f"management commands absent from okf/: {missing}. Add each to the "
        "concept page that owns it, and a dated entry to okf/log.md."
    )
