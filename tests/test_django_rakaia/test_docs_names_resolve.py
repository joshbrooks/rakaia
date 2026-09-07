"""Every dotted `rakaia.…` name written in backticks must resolve.

`CLAUDE.md` asks for this already — *"Every name in a snippet must resolve and every
keyword must exist"* — and it has been a rule addressed to people, who are bad at it: one
renamed function left dead references in several spellings, and review found them one at a
time, by hand, over successive rounds.

Lives under `test_django_rakaia` because some names need an app registry to resolve.

No counts in this prose. Earlier versions carried them freely and every review round since
found one stale — the defect this file exists to catch, turned on its author.

**What this does and does not defend against.** It catches prose that has rotted: a name
renamed, moved or removed while a sentence still says it exists. It does not defend against
someone editing the sweep to check less. Five review rounds tried: each guard added created
a new expression downstream of itself, and the next round narrowed that one instead — the
file set, the pattern, the scan body, the iteration, then the file set passed per name. That
search was stopped on purpose rather than finished, because the guards were growing faster
than what they protected.
"""

from __future__ import annotations

import importlib
import re
from pathlib import Path

#: What counts as a name.
_DOTTED = re.compile(r"`((?:django_)?rakaia(?:\.[A-Za-z_][A-Za-z0-9_]*)+)`")

#: Django system-check identifiers. `rakaia.E001` is a check *id*, a string, not an
#: attribute. Excluded by shape so a new check needs no exemption.
_CHECK_ID = re.compile(r"^[EWI]\d{3}$")

#: Where prose lives. One pattern per distinction that matters — the two packages are
#: separate because a single `src/**/*.py` was narrowed to one of them with the suite green.
_SWEPT: tuple[str, ...] = (
    "*.md",
    "docs/**/*.md",
    "src/rakaia/**/*.py",
    "src/django_rakaia/**/*.py",
    "examples/**/*.py",
    "examples/**/*.md",
    "scripts/**/*.py",
    "tests/**/*.py",
)

#: The declarations above decide what gets checked, so each is pinned by
#: `test_the_declarations_that_decide_coverage_are_pinned`. Narrowing any one of them
#: shrinks the sweep in silence, and each has been narrowed in review at least once. The
#: file set was pinned first; the next round narrowed the regex instead, dropping
#: `django_rakaia` entirely with the run still green. Pinning one input was not enough,
#: because coverage has several.
_COVERAGE_DECLARATIONS = {
    "_SWEPT": _SWEPT,
    "_DOTTED": _DOTTED.pattern,
    "_CHECK_ID": _CHECK_ID.pattern,
}

#: Names that do not resolve **on purpose**, mapped to the files allowed to say them and
#: why. A name belongs here only when the prose is talking about the past; if a document is
#: simply wrong, fix the document.
#:
#: Scoped per file: a name-wide exemption for one legitimate historical mention silently
#: forgave the same dead reference everywhere else, which review had just found by hand.
_DELIBERATELY_ABSENT: dict[str, tuple[frozenset[str], str]] = {
    "django_rakaia.protocol_views": (
        frozenset(
            {
                "CHANGELOG.md",
                "docs/adr/0002-framework-vs-protocol-server-boundary.md",
                "docs/django-integration.md",
                "tests/test_django_rakaia/test_protocol_server.py",
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

_ROOT = Path(__file__).resolve().parents[2]


def _documents() -> list[Path]:
    """Markdown and source alike, because a docstring makes the same claims."""
    found = {p for pattern in _SWEPT for p in _ROOT.glob(pattern)}
    # This file names dead spellings as *data* — the exemption keys — so scanning itself
    # would report its own table as a finding.
    return sorted(p for p in found if p != Path(__file__).resolve())


def _mentions() -> dict[str, set[str]]:
    """Every name, mapped to the files that say it."""
    out: dict[str, set[str]] = {}
    for doc in _documents():
        for match in _DOTTED.finditer(doc.read_text()):
            name = match.group(1)
            if _CHECK_ID.match(name.rsplit(".", 1)[-1]):
                continue
            out.setdefault(name, set()).add(str(doc.relative_to(_ROOT)))
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
    # `_DOTTED` roots every name at one of the two packages, both always importable, so the
    # loop returns on its last iteration at the latest. Reachable only if the regex is
    # widened past them, which the pin above makes a deliberate act.
    raise AssertionError(f"{name} has no importable prefix; did `_DOTTED` widen?")


def _offenders(name: str, mentioned: set[str]) -> list[str]:
    """The files naming `name` without a licence to.

    A named function so the scoping rule can be tested. Inline, `offenders = []` — the
    name-wide behaviour of an earlier version — left every assertion here green.
    """
    allowed, _ = _DELIBERATELY_ABSENT.get(name, (frozenset(), ""))
    return sorted(mentioned - allowed)


_MENTIONED = _mentions()


class TestTheDocsNameThingsThatExist:
    def test_every_documented_name_resolves(self) -> None:
        """One test over every name, deliberately not parametrised.

        A parametrise expression sits downstream of everything that builds `_MENTIONED`, so
        narrowing it shrank what was checked with the suite green — filtering it to the
        names that already resolve made this a tautology. The `examined` count below is what
        catches that; folding the loop, which was proposed as the structural fix, does not.
        The fold is kept because one failure listing every offender suits a sweep better
        than one failure per name.
        """
        problems: list[str] = []
        examined = 0
        for name in sorted(_MENTIONED):
            examined += 1
            if _resolves(name):
                continue
            offenders = _offenders(name, _MENTIONED[name])
            if not offenders:
                continue
            allowed, reason = _DELIBERATELY_ABSENT.get(name, (frozenset(), ""))
            note = (
                f" (already allowed in {', '.join(sorted(allowed))}, because {reason})"
                if allowed
                else ""
            )
            problems.append(f"{name} — named in {', '.join(offenders)}{note}")

        assert examined == len(_MENTIONED), (
            f"the loop visited {examined} of {len(_MENTIONED)} names. Narrowing what this "
            "iterates is how it stops checking without anything going red — the floors "
            "below measure what was collected, not what was examined."
        )
        assert not problems, (
            "these names do not exist and are written as though they do:\n  "
            + "\n  ".join(problems)
            + "\n\nEither the prose is stale — the usual case, and the reason this test "
            "exists — or the mention is deliberately historical, in which case add that "
            "file to _DELIBERATELY_ABSENT with the reason."
        )

    def test_the_declarations_that_decide_coverage_are_pinned(self) -> None:
        """Narrowing any of them shrinks the sweep with nothing red.

        Pinned as one dictionary rather than one test each, because the failure is the same
        failure: review narrowed the file set, that was pinned, and the next round narrowed
        the regex instead — the same defect one input over. Enumerating every input is what
        closes it; a guard parametrised over these values cannot, since it disappears along
        with whatever it was guarding.
        """
        assert _COVERAGE_DECLARATIONS == {
            "_SWEPT": (
                "*.md",
                "docs/**/*.md",
                "src/rakaia/**/*.py",
                "src/django_rakaia/**/*.py",
                "examples/**/*.py",
                "examples/**/*.md",
                "scripts/**/*.py",
                "tests/**/*.py",
            ),
            "_DOTTED": r"`((?:django_)?rakaia(?:\.[A-Za-z_][A-Za-z0-9_]*)+)`",
            "_CHECK_ID": r"^[EWI]\d{3}$",
        }, (
            "what this test checks has changed. Widening is fine and this pin is the place "
            "to say so; narrowing is how the sweep quietly stops working, so change both "
            "declarations on purpose or not at all."
        )

    def test_the_exception_list_has_no_dead_entries(self) -> None:
        """An exemption whose name resolves again should go, or the list only grows."""
        alive = sorted(n for n in _DELIBERATELY_ABSENT if _resolves(n))
        assert not alive, (
            f"these resolve now and no longer need an exception: {alive}. "
            "Remove them from _DELIBERATELY_ABSENT."
        )

    def test_every_exemption_covers_a_file_that_still_names_it(self) -> None:
        """An exemption may only forgive files that still say the name.

        One test rather than two: a separate "mentioned nowhere" check never went red
        without this one going red too. The empty-`allowed` case is the one the merge could
        have lost, so it is asserted below.
        """
        stale = {
            name: sorted(allowed - _MENTIONED.get(name, set()))
            for name, (allowed, _) in _DELIBERATELY_ABSENT.items()
            if allowed - _MENTIONED.get(name, set())
        }
        assert not stale, (
            f"these exemptions cover files that no longer say the name: {stale}. Either "
            "the prose was fixed — remove the entry — or it moved, and the entry should "
            "name where it moved to."
        )

        empty = sorted(
            n for n, (allowed, _) in _DELIBERATELY_ABSENT.items() if not allowed
        )
        assert not empty, f"these exemptions forgive no file and do nothing: {empty}"

    def test_an_exemption_only_covers_the_files_it_names(self) -> None:
        """Scoping is the mechanism, so it is tested directly.

        Neutered to `offenders = []` — the name-wide behaviour — every other assertion here
        stayed green.
        """
        name, (allowed, _) = next(iter(sorted(_DELIBERATELY_ABSENT.items())))
        elsewhere = "some/other/file.md"  # a path no exemption lists

        assert _offenders(name, set(allowed)) == [], (
            f"{name} is exempt in {sorted(allowed)} and should be forgiven there"
        )
        assert _offenders(name, {*allowed, elsewhere}) == [elsewhere], (
            "an exemption must forgive only the files it names, and this one forgave "
            f"{elsewhere}, which it does not list"
        )
        assert _offenders("rakaia.no_such_module", {elsewhere}) == [elsewhere], (
            "a name with no exemption at all must have every mention reported"
        )
