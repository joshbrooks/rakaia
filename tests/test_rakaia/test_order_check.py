"""`check_order`: a consumer's own step order, checked against declared reads and writes.

The steps are a reduced copy of the trial run against partisipa-import's rebuild order
(GitHub #374): form rows, then removing lines a report no longer lists, then the totals
read from those lines, then status, then clearing stale project links, then the links.
The correct order passes, and each bad order is caught for the reason the trial found.
"""

from __future__ import annotations

import pytest

from rakaia import HandlerRegistry, PipelineStep, Upsert, check_order

FORM = "app.Form"
LINE = "app.Line"


def _steps() -> dict[str, PipelineStep]:
    return {
        "form_rows": PipelineStep(
            "form_rows",
            reads=set(),
            writes={FORM, LINE, f"{FORM}.status", f"{FORM}.project", f"{LINE}.cost"},
        ),
        "lines_removed": PipelineStep(
            "lines_removed",
            reads={f"{LINE}.parent"},
            writes={LINE},
            may_overwrite={"form_rows"},
        ),
        "totals": PipelineStep(
            "totals", reads={LINE, f"{LINE}.cost"}, writes={f"{FORM}.total"}
        ),
        "status": PipelineStep(
            "status",
            reads={LINE},
            writes={f"{FORM}.status"},
            may_overwrite={"form_rows"},
        ),
        "clear_links": PipelineStep(
            "clear_links",
            reads=set(),
            writes={f"{FORM}.project"},
            may_overwrite={"form_rows"},
        ),
        "links": PipelineStep(
            "links",
            reads={f"{FORM}.suku"},
            writes={f"{FORM}.project"},
            may_overwrite={"form_rows", "clear_links"},
        ),
    }


GOOD = ["form_rows", "lines_removed", "totals", "status", "clear_links", "links"]


def _check(order: list[str]):
    steps = _steps()
    return check_order([steps[name] for name in order])


def test_the_correct_order_passes() -> None:
    report = _check(GOOD)

    assert report.ok, str(report)
    assert report.findings == (), str(report)


def test_a_read_a_later_step_still_changes_is_stale() -> None:
    order = ["form_rows", "totals", "lines_removed", "status", "clear_links", "links"]

    report = _check(order)

    assert not report.ok
    assert [(f.step, f.unit, f.others) for f in report.stale_reads] == [
        ("totals", LINE, ("lines_removed",)),
        ("totals", f"{LINE}.cost", ("lines_removed",)),
    ]


def test_removing_rows_later_makes_a_column_read_stale() -> None:
    steps = [
        PipelineStep("read_cost", reads={f"{LINE}.cost"}, writes=set()),
        PipelineStep("add_lines", reads=set(), writes={LINE}),
    ]

    assert [f.unit for f in check_order(steps).stale_reads] == [f"{LINE}.cost"]


def test_a_later_column_write_makes_a_table_read_stale() -> None:
    """A table name covers every column, as in a handler's ``reads=``."""
    steps = [
        PipelineStep("lines", reads=set(), writes={LINE}),
        PipelineStep("totals", reads={LINE}, writes={f"{FORM}.total"}),
        PipelineStep("fix_cost", reads=set(), writes={f"{LINE}.cost"}),
    ]

    report = check_order(steps)

    assert [(f.step, f.unit, f.others) for f in report.stale_reads] == [
        ("totals", LINE, ("fix_cost",))
    ]


def test_writes_to_two_different_columns_do_not_affect_each_other() -> None:
    steps = [
        PipelineStep("lines", reads=set(), writes={LINE}),
        PipelineStep("totals", reads={f"{LINE}.cost"}, writes={f"{FORM}.total"}),
        PipelineStep("tidy", reads=set(), writes={f"{LINE}.note"}),
    ]

    assert check_order(steps).stale_reads == ()


def test_recreating_rows_overwrites_the_columns_earlier_steps_wrote() -> None:
    steps = [
        PipelineStep("rows", reads=set(), writes={FORM}),
        PipelineStep("status", reads=set(), writes={f"{FORM}.status"}),
        PipelineStep("rows_again", reads=set(), writes={FORM}, may_overwrite={"rows"}),
    ]

    report = check_order(steps)

    assert [(f.step, f.unit, f.others) for f in report.unnamed_overwrites] == [
        ("rows_again", f"{FORM}.status", ("status",))
    ]


def test_filling_a_column_again_after_recreating_rows_is_not_an_overwrite() -> None:
    """Recreating the rows wiped the column, so setting it again replaces nothing."""
    steps = [
        PipelineStep("rows", reads=set(), writes={LINE, f"{LINE}.cost"}),
        PipelineStep("lines_removed", reads=set(), writes={LINE}, may_overwrite="rows"),
        PipelineStep("cost", reads=set(), writes={f"{LINE}.cost"}),
    ]

    assert check_order(steps).unnamed_overwrites == ()


def test_changing_the_writers_dict_afterwards_does_not_change_the_step() -> None:
    writers = {FORM: frozenset({"app.rows"})}
    step = PipelineStep("rows", reads=set(), writes={FORM}, writers=writers)

    writers[LINE] = frozenset({"app.other"})

    assert set(step.writers) == {FORM}


def test_a_step_writing_a_table_and_its_column_never_overwrites_itself() -> None:
    steps = [
        PipelineStep("rows", reads=set(), writes={FORM, f"{FORM}.status"}),
        PipelineStep(
            "status", reads=set(), writes={f"{FORM}.status"}, may_overwrite="rows"
        ),
        PipelineStep(
            "rows_again",
            reads=set(),
            writes={FORM, f"{FORM}.status"},
            may_overwrite={"rows"},
        ),
    ]

    report = check_order(steps)

    assert [(f.step, f.unit, f.others) for f in report.unnamed_overwrites] == [
        ("rows_again", f"{FORM}.status", ("status",))
    ]


def test_filling_a_column_after_creating_rows_is_not_an_overwrite() -> None:
    steps = [
        PipelineStep("rows", reads=set(), writes={FORM}),
        PipelineStep("status", reads=set(), writes={f"{FORM}.status"}),
    ]

    assert check_order(steps).unnamed_overwrites == ()


def test_an_overwrite_the_step_does_not_name_is_reported() -> None:
    order = ["status", "form_rows", "lines_removed", "totals", "clear_links", "links"]
    steps = _steps()
    steps["status"] = PipelineStep("status", reads={LINE}, writes={f"{FORM}.status"})

    report = check_order([steps[name] for name in order])

    assert [(f.step, f.unit, f.others) for f in report.unnamed_overwrites] == [
        ("form_rows", f"{FORM}.status", ("status",))
    ]
    assert [f.step for f in report.stale_reads] == ["status"]


def test_clearing_links_after_making_them_is_caught_only_as_an_overwrite() -> None:
    """Nothing reads the links afterwards, so no read looks stale; only the
    undeclared overwrite shows that clearing wipes every link just made. (Had
    ``links`` declared the whole ``app.Form`` table, the later clear would also
    make its read stale: a table name covers every column.)"""
    order = ["form_rows", "lines_removed", "totals", "status", "links", "clear_links"]

    report = _check(order)

    assert report.stale_reads == ()
    assert [(f.step, f.unit, f.others) for f in report.unnamed_overwrites] == [
        ("clear_links", f"{FORM}.project", ("links",))
    ]


def test_two_functions_creating_one_tables_rows_is_reported_but_not_a_failure() -> None:
    steps = [
        PipelineStep("live", reads=set(), writes={FORM}, by="app.save_rows"),
        PipelineStep(
            "fix",
            reads=set(),
            writes={FORM},
            may_overwrite={"live"},
            by="app.repair_rows",
        ),
    ]

    report = check_order(steps)

    assert [(f.unit, f.others) for f in report.shared_writers] == [
        (FORM, ("app.repair_rows", "app.save_rows"))
    ]
    assert report.ok


def test_one_function_used_by_two_steps_is_not_shared() -> None:
    steps = [
        PipelineStep("a", reads=set(), writes={FORM}, by="app.rows"),
        PipelineStep(
            "b", reads=set(), writes={FORM}, may_overwrite={"a"}, by="app.rows"
        ),
    ]

    assert check_order(steps).shared_writers == ()


def test_a_step_that_declares_nothing_is_unknown_never_clean() -> None:
    steps = [PipelineStep("rows", reads=set(), writes={FORM}), PipelineStep("mystery")]

    report = check_order(steps)

    assert not report.ok
    assert [f.step for f in report.undeclared] == ["mystery"]


def test_a_step_that_declares_only_its_reads_is_unknown() -> None:
    step = PipelineStep("half", reads={FORM})

    assert not step.known
    assert [f.step for f in check_order([step]).undeclared] == ["half"]


def test_may_overwrite_can_be_one_name() -> None:
    steps = [
        PipelineStep("rows", reads=set(), writes={FORM}),
        PipelineStep("again", reads=set(), writes={FORM}, may_overwrite="rows"),
    ]

    assert check_order(steps).unnamed_overwrites == ()


def test_only_rows_count_towards_a_shared_writer() -> None:
    """Two functions setting one column is ordinary; only creating rows is shared."""
    steps = [
        PipelineStep("rows", reads=set(), writes={FORM}, by="app.rows"),
        PipelineStep("a", reads=set(), writes={f"{FORM}.status"}, by="app.set_a"),
        PipelineStep(
            "b",
            reads=set(),
            writes={f"{FORM}.status"},
            may_overwrite={"a"},
            by="app.set_b",
        ),
    ]

    assert check_order(steps).shared_writers == ()


def test_steps_without_by_are_not_reported_as_shared_writers() -> None:
    """Without ``by`` the step names no function, so there is nothing to compare."""
    steps = [
        PipelineStep("rows", reads=set(), writes={LINE}),
        PipelineStep("removed", reads=set(), writes={LINE}, may_overwrite="rows"),
    ]

    assert check_order(steps).shared_writers == ()


def test_by_and_writers_together_are_refused() -> None:
    with pytest.raises(ValueError, match="not both"):
        PipelineStep(
            "s", set(), {FORM}, by="app.rows", writers={FORM: frozenset({"app.rows"})}
        )


def test_writers_must_name_tables_the_step_writes() -> None:
    with pytest.raises(ValueError, match="not tables it writes"):
        PipelineStep(
            "s",
            set(),
            {f"{FORM}.status"},
            writers={f"{FORM}.status": frozenset({"app.a", "app.b"})},
        )
    with pytest.raises(ValueError, match="not tables it writes"):
        PipelineStep("s", set(), {FORM}, writers={LINE: frozenset({"app.a"})})


def test_a_read_no_step_fills_is_an_outside_input() -> None:
    steps = [
        PipelineStep("rows", reads=set(), writes={FORM}),
        PipelineStep("totals", reads={"ref.Rate"}, writes={f"{FORM}.total"}),
    ]

    report = check_order(steps)

    assert [(f.step, f.unit) for f in report.outside_inputs] == [("totals", "ref.Rate")]
    assert report.ok


def test_setting_a_column_does_not_fill_a_read_of_the_whole_table() -> None:
    """Nothing creates the lines, so reading the table still needs an outside source."""
    steps = [
        PipelineStep("fix_cost", reads=set(), writes={f"{LINE}.cost"}),
        PipelineStep("totals", reads={LINE, f"{LINE}.cost"}, writes={f"{FORM}.total"}),
    ]

    assert [f.unit for f in check_order(steps).outside_inputs] == [LINE]


def test_a_read_the_step_fills_itself_is_not_an_outside_input() -> None:
    steps = [PipelineStep("replay", reads={LINE}, writes={FORM, LINE})]

    assert check_order(steps).outside_inputs == ()


def _rows(event):  # pragma: no cover - never replayed here
    return Upsert(model_label=FORM, lookup={"key": event["key"]})


def _status(_event):  # pragma: no cover - never replayed here
    return []


def test_a_step_from_a_registry_takes_its_handlers_declarations() -> None:
    registry = HandlerRegistry()
    registry.register(
        "rows", "forms", _rows, effective_from=0, reads=set(), writes={FORM, LINE}
    )
    registry.register(
        "status",
        "forms",
        _status,
        effective_from=0,
        reads={LINE},
        writes=f"{FORM}.status",
    )

    step = PipelineStep.from_registry("replay", registry)

    assert step.known
    assert step.reads == {LINE}
    assert step.writes == {FORM, LINE, f"{FORM}.status"}
    assert step.writers[FORM] == {f"{__name__}._rows"}


def _rows_v2(event):  # pragma: no cover - never replayed here
    return Upsert(model_label=FORM, lookup={"key": event["key"], "v": 2})


def _cost(_event, _reader):  # pragma: no cover - never replayed here
    return []


def test_a_new_version_of_a_handler_is_not_a_second_writer() -> None:
    registry = HandlerRegistry()
    registry.register("rows", "forms", _rows, 0, 100, reads=set(), writes={FORM})
    registry.register("rows", "forms", _rows_v2, 100, reads=set(), writes={FORM})

    step = PipelineStep.from_registry("replay", registry)

    assert step.writers[FORM] == {f"{__name__}._rows_v2"}
    assert check_order([step]).shared_writers == ()


def test_a_stage_reading_what_a_later_stage_writes_is_stale() -> None:
    registry = HandlerRegistry()
    registry.register(
        "totals", "forms", _status, 0, reads={f"{LINE}.cost"}, writes=f"{FORM}.total"
    )
    registry.register(
        "cost", "forms", _cost, 0, stage=1, reads=set(), writes=f"{LINE}.cost"
    )

    steps = [
        PipelineStep.from_registry(f"replay[{stage}]", registry, stage=stage)
        for stage in registry.stages()
    ]

    assert [(s.reads, s.writes) for s in steps] == [
        ({f"{LINE}.cost"}, {f"{FORM}.total"}),
        (set(), {f"{LINE}.cost"}),
    ]
    assert [(f.step, f.unit, f.others) for f in check_order(steps).stale_reads] == [
        ("replay[0]", f"{LINE}.cost", ("replay[1]",))
    ]


def test_a_reducer_makes_only_its_own_stage_unknown() -> None:
    registry = HandlerRegistry()
    registry.register("rows", "forms", _rows, 0, reads=set(), writes={FORM})
    registry.register_reducer("tally", 1, lambda _reader: [])

    first, second = (
        PipelineStep.from_registry(f"replay[{stage}]", registry, stage=stage)
        for stage in registry.stages()
    )

    assert first.known
    assert second.undeclared == ("reducer tally",)


def test_from_registry_may_overwrite_can_be_one_name() -> None:
    registry = HandlerRegistry()
    registry.register("rows", "forms", _rows, 0, reads=set(), writes={FORM})

    steps = [
        PipelineStep("live", reads=set(), writes={FORM}),
        PipelineStep.from_registry("replay", registry, may_overwrite="live"),
    ]

    assert check_order(steps).unnamed_overwrites == ()


def test_a_handler_without_declarations_or_a_reducer_makes_the_step_unknown() -> None:
    registry = HandlerRegistry()
    registry.register("rows", "forms", _rows, effective_from=0)
    registry.register_reducer("tally", 1, lambda _reader: [])

    step = PipelineStep.from_registry("replay", registry)

    assert not step.known
    assert step.undeclared == ("handler rows", "reducer tally")
    assert not check_order([step]).ok


def test_names_must_be_unique_and_may_overwrite_must_name_a_step() -> None:
    with pytest.raises(ValueError, match="repeated"):
        check_order([PipelineStep("a", set(), set()), PipelineStep("a", set(), set())])
    with pytest.raises(ValueError, match="not steps here"):
        check_order([PipelineStep("a", set(), set(), may_overwrite={"typo"})])


def test_a_malformed_table_name_is_refused() -> None:
    with pytest.raises(ValueError, match=r"app\.Model"):
        PipelineStep("bad", reads={"Form"}, writes=set())


def test_the_report_reads_as_plain_lines() -> None:
    text = str(
        _check(
            ["form_rows", "totals", "lines_removed", "status", "clear_links", "links"]
        )
    )

    assert text.splitlines()[0] == "order NOT ok"
    assert (
        "  stale_read: totals reads app.Line, which later step(s) lines_removed still change"
        in text.splitlines()
    )
