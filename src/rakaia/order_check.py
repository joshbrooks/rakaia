"""Check a consumer's own order of steps against what each step reads and writes.

A rebuild that runs several steps one after another is right only if each step reads what
the earlier steps have finished writing, and only overwrites what it means to. rakaia does
not own that list: the consumer builds it and passes the steps to `check_order` in the order
it runs them. Each `PipelineStep` names the tables (``app.Model``) or columns
(``app.Model.field``) it reads and writes, as handlers do with ``reads=`` / ``writes=``,
and the earlier steps it may overwrite.

The order is checked, never worked out. Declarations are hand-written and often
incomplete, so a step that declares nothing is reported as unknown rather than passed.
A table name covers its rows and every column. Writing a table (creating or removing
rows) changes what any read of that table or its columns sees, and overwrites every
column an earlier step wrote on it. Writing a column changes reads of that column and
of the whole table, and replaces only that column.
"""

from __future__ import annotations

from collections.abc import Iterable, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Literal

from .declared_tables import canonical_tables

if TYPE_CHECKING:
    from .registry import HandlerRegistry, HandlerVersion

FindingKind = Literal[
    "stale_read", "unnamed_overwrite", "shared_writer", "outside_input", "undeclared"
]


def _table(unit: str) -> str:
    return ".".join(unit.split(".")[:2])


def _is_table(unit: str) -> bool:
    return unit.count(".") == 1


def _sorted(names: frozenset[str] | None) -> list[str] | None:
    return None if names is None else sorted(names)


def _affects(written: str, read: str) -> bool:
    """Whether a write of ``written`` changes what a read of ``read`` sees.

    A table name covers its rows and every column, so a write and a read of the same
    table affect each other unless both name different single columns.
    """
    if written == read:
        return True
    return _table(written) == _table(read) and (_is_table(written) or _is_table(read))


def _fills(written: str, read: str) -> bool:
    """Whether a write of ``written`` provides what a read of ``read`` needs.

    Narrower than `_affects`: setting one column does not create the rows a read of the
    whole table needs, so only a write of the table itself fills that.
    """
    return written == read or (_is_table(written) and _table(read) == written)


class PipelineStep:
    """One step of a consumer's pipeline, and what it reads and writes.

    ``reads`` and ``writes`` take the same names as a handler's: ``"app.Model"`` or
    ``"app.Model.field"``; ``None`` means not declared, and the step is then reported as
    unknown. ``may_overwrite`` names the earlier steps whose writes this one is meant to
    replace. ``by`` names the function that creates or removes the rows of each table
    the step writes, so `check_order` can tell two functions doing that from one function
    used twice; a step without ``by`` (or ``writers``) names no function and is left out
    of that check. `from_registry` builds a step from its handlers' declarations.
    """

    __slots__ = ("may_overwrite", "name", "reads", "undeclared", "writers", "writes")

    name: str
    reads: frozenset[str] | None
    writes: frozenset[str] | None
    may_overwrite: frozenset[str]
    writers: dict[str, frozenset[str]]
    """For each table this step creates or removes rows of, the functions that do it."""
    undeclared: tuple[str, ...]
    """Parts of the step that declare nothing, such as a handler with no ``writes=``
    or a reducer. A step with any is reported as unknown."""

    def __init__(
        self,
        name: str,
        reads: str | Iterable[str] | None = None,
        writes: str | Iterable[str] | None = None,
        *,
        may_overwrite: str | Iterable[str] = (),
        by: str | None = None,
        writers: dict[str, frozenset[str]] | None = None,
        undeclared: Iterable[str] = (),
    ) -> None:
        self.name = name
        self.reads = canonical_tables(reads, f"step {name!r} reads")
        self.writes = canonical_tables(writes, f"step {name!r} writes")
        self.may_overwrite = frozenset(
            [may_overwrite] if isinstance(may_overwrite, str) else may_overwrite
        )
        if writers is None:
            writers = {
                unit: frozenset({by})
                for unit in self.writes or ()
                if by is not None and _is_table(unit)
            }
        elif by is not None:
            raise ValueError(f"step {name!r}: pass by= or writers=, not both")
        elif stray := sorted(
            unit
            for unit in writers
            if not _is_table(unit) or unit not in (self.writes or ())
        ):
            raise ValueError(
                f"step {name!r}: writers names {stray}, which are not tables it writes"
            )
        self.writers = dict(writers)
        self.undeclared = tuple(undeclared)

    def __repr__(self) -> str:
        return (
            f"PipelineStep({self.name!r}, reads={_sorted(self.reads)}, "
            f"writes={_sorted(self.writes)}, may_overwrite={sorted(self.may_overwrite)})"
        )

    @property
    def known(self) -> bool:
        """Whether the step declares both its reads and its writes, in full."""
        return (
            self.reads is not None and self.writes is not None and not self.undeclared
        )

    @classmethod
    def from_registry(
        cls,
        name: str,
        registry: HandlerRegistry,
        *,
        stage: int | None = None,
        may_overwrite: str | Iterable[str] = (),
    ) -> PipelineStep:
        """A step that replays through ``registry``, declared by its handlers.

        Reads and writes are the union over every handler version, retired ones included,
        since a rebuild replays old events through them. Each table's writers are the
        handlers' functions, one per handler (a name and the events it matches, as the
        registry keys it), labelled with its newest version's function and covering the
        tables of all its versions, so versioning a handler does not look like a second
        function but two handlers, or a retired version's tables, are not lost. A handler missing
        ``reads=`` or ``writes=``, and any reducer (reducers cannot declare tables yet),
        is listed in ``undeclared``.

        A staged registry replays its stages in order, which is a step order too. Pass
        ``stage`` to build the step for one stage only, one step per `registry.stages()`
        entry, so `check_order` can see a stage that reads what a later stage writes.
        """
        reads: set[str] = set()
        writes: set[str] = set()
        newest: dict[tuple[str, object], HandlerVersion] = {}
        tables: dict[tuple[str, object], set[str]] = {}
        undeclared: set[str] = set()
        for version in registry.all_versions():
            if stage is not None and version.stage != stage:
                continue
            if version.reads is None or version.writes is None:
                undeclared.add(f"handler {version.name}")
            reads |= version.reads or frozenset()
            writes |= version.writes or frozenset()
            handler = (version.name, version.event_match)
            current = newest.get(handler)
            if current is None or version.effective_from > current.effective_from:
                newest[handler] = version
            tables.setdefault(handler, set()).update(
                u for u in version.writes or frozenset() if _is_table(u)
            )
        writers: dict[str, set[str]] = {}
        for handler, version in newest.items():
            for unit in tables[handler]:
                writers.setdefault(unit, set()).add(version.dotted_path)
        undeclared |= {
            f"reducer {r.name}"
            for r in registry.all_reducers()
            if stage is None or r.stage == stage
        }
        return cls(
            name,
            reads=frozenset(reads),
            writes=frozenset(writes),
            may_overwrite=may_overwrite,
            writers={unit: frozenset(fns) for unit, fns in writers.items()},
            undeclared=tuple(sorted(undeclared)),
        )


@dataclass(frozen=True)
class OrderFinding:
    """One thing `check_order` found, about one step and one table or column."""

    kind: FindingKind
    step: str
    unit: str
    others: tuple[str, ...] = ()
    """The other steps or functions involved, depending on ``kind``."""

    def __str__(self) -> str:
        others = ", ".join(self.others)
        if self.kind == "stale_read":
            return f"{self.step} reads {self.unit}, which later step(s) {others} still change"
        if self.kind == "unnamed_overwrite":
            return f"{self.step} overwrites {self.unit} from {others}, which it does not name in may_overwrite"
        if self.kind == "shared_writer":
            return f"rows of {self.unit} are created or removed by more than one function: {others}"
        if self.kind == "outside_input":
            return f"{self.step} reads {self.unit}, which no step fills"
        return f"{self.step} is unknown: {others or 'it does not declare its reads and writes'}"


@dataclass(frozen=True)
class OrderReport:
    """What `check_order` found. ``ok`` only when every step is known and nothing reads
    stale data or overwrites without saying so; shared writers and outside inputs are
    information, not failures."""

    findings: tuple[OrderFinding, ...]

    def _of(self, kind: FindingKind) -> tuple[OrderFinding, ...]:
        return tuple(f for f in self.findings if f.kind == kind)

    @property
    def stale_reads(self) -> tuple[OrderFinding, ...]:
        return self._of("stale_read")

    @property
    def unnamed_overwrites(self) -> tuple[OrderFinding, ...]:
        return self._of("unnamed_overwrite")

    @property
    def shared_writers(self) -> tuple[OrderFinding, ...]:
        return self._of("shared_writer")

    @property
    def outside_inputs(self) -> tuple[OrderFinding, ...]:
        return self._of("outside_input")

    @property
    def undeclared(self) -> tuple[OrderFinding, ...]:
        return self._of("undeclared")

    @property
    def ok(self) -> bool:
        return not (self.stale_reads or self.unnamed_overwrites or self.undeclared)

    def __str__(self) -> str:
        if not self.findings:
            return "order ok: nothing found"
        head = "order ok" if self.ok else "order NOT ok"
        return "\n".join([head, *(f"  {f.kind}: {f}" for f in self.findings)])


def check_order(steps: Sequence[PipelineStep]) -> OrderReport:
    """Check that ``steps``, in the order given, read and overwrite only what they should.

    Reports a read that a later step still changes (``stale_read``), a write over an
    earlier step's that the step does not name in ``may_overwrite``
    (``unnamed_overwrite``), a table whose rows more than one named function creates or
    removes (``shared_writer``), a read no step fills (``outside_input``: for a table,
    no step writes its rows), and every step that
    does not declare in full (``undeclared``). Raises `ValueError` for two steps with one
    name, or a ``may_overwrite`` naming no step at all. Naming a step that runs later is
    allowed: it overwrites nothing of that step's, and the order is what gets reported.
    """
    names = [step.name for step in steps]
    if len(set(names)) != len(names):
        dupes = sorted({n for n in names if names.count(n) > 1})
        raise ValueError(f"step names must be unique; repeated: {dupes}")
    for step in steps:
        if unknown := step.may_overwrite - set(names):
            raise ValueError(
                f"step {step.name!r} may_overwrite names {sorted(unknown)}, "
                "which are not steps here"
            )

    findings: list[OrderFinding] = []
    for i, step in enumerate(steps):
        if not step.known:
            findings.append(OrderFinding("undeclared", step.name, "", step.undeclared))
        for unit in sorted(step.reads or ()):
            later = tuple(
                s.name
                for s in steps[i + 1 :]
                if any(_affects(w, unit) for w in s.writes or ())
            )
            # The step itself counts: a replay's handlers fill what its others read.
            filled = any(
                _fills(w, unit) for s in steps[: i + 1] for w in s.writes or ()
            )
            if later:
                findings.append(OrderFinding("stale_read", step.name, unit, later))
            elif not filled:
                findings.append(OrderFinding("outside_input", step.name, unit))

    last: dict[str, str] = {}
    for step in steps:
        for unit in sorted(step.writes or ()):
            # Writing a table recreates or removes its rows, which wipes every column
            # an earlier step wrote on them; writing a column replaces only that column.
            replaced = [unit]
            if _is_table(unit):
                replaced += [u for u in last if not _is_table(u) and _table(u) == unit]
            for gone in sorted(set(replaced)):
                previous = last.get(gone)
                if (
                    previous is not None
                    and previous != step.name
                    and previous not in step.may_overwrite
                ):
                    findings.append(
                        OrderFinding("unnamed_overwrite", step.name, gone, (previous,))
                    )
                if gone == unit:
                    last[gone] = step.name
                else:
                    # The column went with its rows: a later step that fills it in
                    # again is not overwriting anyone.
                    del last[gone]

    owners: dict[str, set[str]] = {}
    for step in steps:
        for unit, functions in step.writers.items():
            owners.setdefault(unit, set()).update(functions)
    findings.extend(
        OrderFinding("shared_writer", "", unit, tuple(sorted(functions)))
        for unit, functions in sorted(owners.items())
        if len(functions) > 1
    )
    return OrderReport(tuple(findings))
