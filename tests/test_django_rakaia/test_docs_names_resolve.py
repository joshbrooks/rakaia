"""Every dotted `rakaia.…` name the documentation puts in backticks must resolve.

`CLAUDE.md` already asks for this — *"Every name in a snippet must resolve and every
keyword must exist"* — and it has been a rule for people rather than a test. People are
bad at it: renaming one function left dead references in **four** different spellings
(bare backticked, dotted in a model docstring, dotted in a section written by an earlier
release, and one more), and three review rounds found three of them by hand. This finds
all of them in 65 names and under a second.

It is the cheap third of the problem. A generated page cannot drift — `docs/api-reference.md`
is written by `scripts/gen_api_reference.py` and gated by `just api-reference-check`, and it
has never gone stale. Hand-written prose can, and the part of it that is *mechanically*
checkable is exactly this: does the name exist. What is left over — "this landed in #247",
"the other five models", "that example hand-rolls the loop" — is not checkable at any
sensible cost, and the rule for it is to not write it rather than to check it harder.

Lives under `test_django_rakaia` because half the names need an app registry to resolve.
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path

import pytest

#: A backticked name rooted at one of our two packages, with at least one dot.
#: Anchored on the package so `docs/public-api.md` and prose like `foo.bar` are not
#: mistaken for code.
_DOTTED = re.compile(r"`((?:django_)?rakaia(?:\.[A-Za-z_][A-Za-z0-9_]*)+)`")

#: Django system-check identifiers — `rakaia.E001` is a check *id*, a string, not an
#: attribute of the package. Excluded by shape rather than by name so a new check does
#: not have to be added to the exception list below.
_CHECK_ID = re.compile(r"^[EWI]\d{3}$")

#: Names that do not resolve **on purpose** — mapped to the exact files allowed to say
#: them, and why. Adding to this is a deliberate act: a name only belongs here when the
#: prose is talking about the past, and history is the one thing prose may assert that
#: code cannot confirm. If a doc is simply wrong, fix the doc.
#:
#: **Scoped by file, and that is the point.** A per-name exemption was the first version
#: and mutation proved it dangerous: `rakaia.outcomes.encode` is legitimately named in
#: UPGRADING's rename announcement, and a name-wide exemption for that silently forgave
#: the same dead reference in a model docstring — which is exactly the defect a review
#: round had just found by hand. An exemption is for one sentence in one file, never for
#: a name everywhere.
_DELIBERATELY_ABSENT: dict[str, tuple[frozenset[str], str]] = {
    "django_rakaia.protocol_views": (
        frozenset(
            {
                "CHANGELOG.md",
                "docs/adr/0002-framework-vs-protocol-server-boundary.md",
                "docs/django-integration.md",
            }
        ),
        "the module was removed; these name it as what went",
    ),
    "rakaia.outcomes.encode": (
        frozenset({"UPGRADING.md"}),
        "the pre-rename spelling, in the paragraph announcing the rename",
    ),
    "rakaia.jsonl_store.something": (
        frozenset({"UPGRADING.md"}),
        "an illustrative placeholder, not a real attribute",
    ),
}


def _documents() -> list[Path]:
    """Markdown **and** source, because a docstring makes the same claims.

    Scanning only `.md` was the first version of this test, and mutating it proved that
    weaker than it looked: reintroducing the dead reference in `models.py`'s docstring —
    which is exactly where round 2 found one — left it green. Prose is prose wherever it
    is written.
    """
    root = Path(__file__).resolve().parents[2]
    found = sorted(
        {*root.glob("*.md"), *root.glob("docs/**/*.md"), *root.glob("src/**/*.py")}
    )
    return [p for p in found if ".venv" not in p.parts]


def _mentions() -> dict[str, set[str]]:
    """Every dotted name in the docs, mapped to the files that mention it."""
    root = Path(__file__).resolve().parents[2]
    out: dict[str, set[str]] = {}
    for doc in _documents():
        for match in _DOTTED.finditer(doc.read_text()):
            name = match.group(1)
            if _CHECK_ID.match(name.rsplit(".", 1)[-1]):
                continue
            out.setdefault(name, set()).add(str(doc.relative_to(root)))
    return out


def _resolves(name: str) -> bool:
    """Import the longest importable prefix, then walk the rest as attributes."""
    parts = name.split(".")
    for stop in range(len(parts), 0, -1):
        try:
            obj = importlib.import_module(".".join(parts[:stop]))
        except ImportError:
            continue
        for attr in parts[stop:]:
            try:
                obj = getattr(obj, attr)
            except AttributeError:
                return False
        return True
    return False


_MENTIONED = _mentions()


class TestTheDocsNameThingsThatExist:
    @pytest.mark.parametrize("name", sorted(_MENTIONED))
    def test_a_documented_name_resolves(self, name: str) -> None:
        if _resolves(name):
            return
        allowed, reason = _DELIBERATELY_ABSENT.get(name, (frozenset(), ""))
        offenders = sorted(_MENTIONED[name] - allowed)
        assert not offenders, (
            f"{name} does not exist and is named in {', '.join(offenders)}. Either the "
            "prose is stale — the usual case, and the reason this test exists — or it is "
            "deliberately historical, in which case add that file to _DELIBERATELY_ABSENT "
            "with the reason."
            + (
                f" It is already allowed in {', '.join(sorted(allowed))}, because {reason}."
                if allowed
                else ""
            )
        )

    def test_the_exception_list_has_no_dead_entries(self) -> None:
        """An exception that now resolves is an exception that should go.

        Otherwise the list only grows: a name gets an exemption for one release, the
        reason expires, and nothing notices. This is the same argument as the docstring
        above — a rule nothing checks is a rule that rots.
        """
        alive = sorted(n for n in _DELIBERATELY_ABSENT if _resolves(n))
        assert not alive, (
            f"these resolve now and no longer need an exception: {alive}. "
            "Remove them from _DELIBERATELY_ABSENT."
        )

    def test_the_exception_list_is_not_a_dumping_ground(self) -> None:
        """Every exception must still be mentioned somewhere, or it is dead weight."""
        unmentioned = sorted(set(_DELIBERATELY_ABSENT) - set(_MENTIONED))
        assert not unmentioned, (
            f"no document mentions these any more: {unmentioned}. Remove them."
        )

    def test_no_exemption_names_a_file_that_does_not_mention_it(self) -> None:
        """A file listed for a name it no longer contains is a widened exemption."""
        stale = {
            name: sorted(allowed - _MENTIONED.get(name, set()))
            for name, (allowed, _) in _DELIBERATELY_ABSENT.items()
            if allowed - _MENTIONED.get(name, set())
        }
        assert not stale, (
            f"these exemptions cover files that no longer say the name: {stale}"
        )

    def test_the_sweep_actually_found_something(self) -> None:
        """Guards against the regex silently matching nothing — which would make every
        test above pass while checking no names at all."""
        assert len(_MENTIONED) > 40, (
            f"only {len(_MENTIONED)} dotted names found; the regex is probably wrong"
        )
