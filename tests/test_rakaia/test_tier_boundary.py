"""The framework tier does not depend on the protocol-server tier.

ADR 0002 records that rakaia is two products in one package — the event-sourcing
framework people build on, and the Durable Streams protocol server — and chose
*not* to split them, on the explicit understanding that the boundary would be
"a **convention** enforced by docs and layering discipline, not by the
packaging". #191 notes the ADR's own trigger for revisiting the split has since
fired.

This file turns that convention into an assertion, which is worth doing whichever
way the split question is eventually answered:

* If the tiers really are separable, a split is mechanical and this is the proof.
* If they are not, the import that says so is named here rather than discovered
  half way through the split.

It is a fitness function, not a style check: the failure it prevents is a
framework module reaching for a protocol-server implementation, which is how a
seam ends up typed against one store instead of the protocol every store
satisfies. That had already happened — `HandlerRegistry(store=...)` was annotated
as the in-memory `StreamStore`, so pyright rejected the durable store, which is
the one case the parameter exists for (a meta-stream surviving a process
restart). Nothing in the suite could see it, because every in-repo caller passes
the in-memory store.
"""

from __future__ import annotations

import ast
import pathlib

import pytest

SRC = pathlib.Path(__file__).resolve().parents[2] / "src" / "rakaia"

#: The event-sourcing framework: effects, registries, replay, projections. What a
#: consumer builds on, and what would become the "projections" package.
FRAMEWORK = {
    "append",
    "context",
    "drift",
    "effects",
    "executors",
    "history",
    "projections",
    "registration_log",
    "registry",
    "replay",
    "seed",
    "source_hash",
}

#: The Durable Streams protocol server: ASGI surface, wire semantics, the stores
#: that implement the protocol lifecycle, and the pure rule modules those two
#: decide with. What would become the "streams" package.
PROTOCOL_SERVER = {
    "_asgi",
    "append_decision",
    "cursor",
    "handler",
    "jsonl_store",
    "producer",
    "read_decision",
    "store",
    "subscription",
}

#: Shared vocabulary both tiers are allowed to import: the types on the wire and
#: in the log, the protocols that describe a store without being one, and the two
#: format rules (`json_mode`, `offsets`) both tiers must agree on. ADR 0002 calls
#: these out as the reason the tiers "genuinely share types today" — they are the
#: substance of the split question, so a change here is the thing to look at.
SHARED = {"types", "protocols", "json_mode", "offsets"}

#: Deliberate, documented crossings. Keep this empty if you can; every entry is a
#: thing a package split would have to resolve, so the list is the running cost
#: of not having split.
ALLOWED_CROSSINGS = {
    # `seed_stream` is a convenience whose whole contract is "omit the store and
    # get a fresh in-memory one". It already types its parameter as
    # `WritableStore`; the concrete import is the default value, not a
    # dependency of the framework on the server. `seed` stays in FRAMEWORK so
    # the rest of its imports are still checked — an exemption is per crossing,
    # not per module, or a module could escape the rule by being listed here.
    ("seed", "store"),
}


def _imports_in(source: str) -> set[str]:
    """Sibling `rakaia` modules `source` imports, including under `TYPE_CHECKING`.

    Type-only imports count. The defect this file exists for was type-only: a
    `TYPE_CHECKING` import of the in-memory store, used in a public signature,
    which made the annotation wrong without creating any runtime coupling at all.

    All four spellings of the same dependency are read, because a checker that
    saw only the house style would be evaded by the first author who wrote one of
    the others — and evaded silently, which is the one thing a fitness function
    must not be.
    """
    found: set[str] = set()
    for node in ast.walk(ast.parse(source)):
        if isinstance(node, ast.ImportFrom):
            if node.level == 1 and node.module:  # from .store import X
                found.add(node.module.split(".")[0])
            elif node.level == 1 or node.module == "rakaia":
                # `from . import store`, `from rakaia import store` — the module
                # is the imported *name*. A name that is a function rather than a
                # module can land here; the cost is a false positive, which is
                # loud, rather than a miss, which is not.
                found.update(alias.name for alias in node.names)
            elif node.module and node.module.startswith("rakaia."):
                found.add(node.module.split(".")[1])  # from rakaia.store import X
        elif isinstance(node, ast.Import):
            for alias in node.names:  # import rakaia.store
                if alias.name.startswith("rakaia."):
                    found.add(alias.name.split(".")[1])
    return found


def _internal_imports(module: str) -> set[str]:
    return _imports_in((SRC / f"{module}.py").read_text())


def _modules() -> set[str]:
    return {p.stem for p in SRC.glob("*.py") if p.stem not in {"__init__", "__main__"}}


def test_the_tier_map_covers_every_module() -> None:
    """A new module must be classified, or the check below silently skips it.

    This is the assertion that keeps the rest honest: without it, adding
    `src/rakaia/whatever.py` to neither set would leave its imports unexamined
    and the boundary would erode exactly where nobody looked.
    """
    unclassified = _modules() - (FRAMEWORK | PROTOCOL_SERVER | SHARED)

    assert not unclassified, (
        f"unclassified module(s): {sorted(unclassified)} — add each to FRAMEWORK, "
        "PROTOCOL_SERVER or SHARED in this file. Which tier a module belongs to "
        "is a design decision; leaving it out is not a way of deferring it."
    )


@pytest.mark.parametrize(
    "source",
    [
        "from .store import StreamStore",
        "from . import store",
        "from rakaia.store import StreamStore",
        "from rakaia import store",
        "import rakaia.store",
        "if TYPE_CHECKING:\n    from .store import StreamStore",
    ],
)
def test_the_reader_sees_every_way_of_spelling_a_dependency(source: str) -> None:
    """The check below is only as good as this, so this is checked directly.

    Every line here is the same dependency, and the repo happens to write only
    the first. A reader that recognised just the house style would let the other
    five through without failing — a boundary check with five undocumented ways
    around it is worse than none, because it reports a boundary that is not there.
    """
    assert "store" in _imports_in(source)


def test_the_tier_sets_do_not_overlap() -> None:
    assert not FRAMEWORK & PROTOCOL_SERVER
    assert not (FRAMEWORK | PROTOCOL_SERVER) & SHARED


@pytest.mark.parametrize("module", sorted(FRAMEWORK))
def test_a_framework_module_does_not_import_the_protocol_server(module: str) -> None:
    """The boundary, in the direction that matters.

    The framework must not know how the protocol server stores anything. The
    reverse *is* allowed and is not checked: the server legitimately builds on
    framework types, which is why this is a one-way rule rather than a partition.
    """
    crossings = {
        imported
        for imported in _internal_imports(module)
        if imported in PROTOCOL_SERVER and (module, imported) not in ALLOWED_CROSSINGS
    }

    assert not crossings, (
        f"`rakaia.{module}` (framework) imports "
        f"{sorted(f'rakaia.{c}' for c in crossings)} (protocol server). If the "
        "framework needs a capability, name the *protocol* it needs from "
        "`rakaia.protocols` — `WritableStore`, `ReadableStore`, "
        "`ProjectionReader` — rather than one implementation of it. If the "
        "crossing is genuinely intended, add it to ALLOWED_CROSSINGS with the "
        "reason, and know that a package split would have to resolve it."
    )


def test_the_shared_vocabulary_depends_on_neither_tier() -> None:
    """What both halves import cannot import either half back.

    `types`, `protocols`, `json_mode` and `offsets` are the whole of what a split
    would have to duplicate, move, or extract into a third package. Keeping them
    dependency-free in both directions is what makes that a real option; a shared
    module that reached into either tier would quietly make the two inseparable.
    """
    for module in sorted(SHARED):
        reached = _internal_imports(module) & (FRAMEWORK | PROTOCOL_SERVER)
        assert not reached, (
            f"`rakaia.{module}` is shared vocabulary but imports "
            f"{sorted(reached)}. Shared code that depends on a tier is not "
            "shared — it is that tier's code with a misleading home."
        )
