# /// script
# requires-python = ">=3.12"
# dependencies = [
#     "rakaia-streams",
#     "marimo==0.25.1",
#     "altair==5.5.0",
#     "httpx==0.28.1",
# ]
#
# [tool.uv.sources]
# rakaia-streams = { path = "..", editable = true }
# ///
#
# "How rakaia works": a guided walk through the library for people adopting it,
# easy ideas first, with a short quiz after each section. Every example runs the
# real `rakaia` package from this checkout, in memory: no database, no server.
#
#     marimo edit --sandbox notebooks/how_rakaia_works.py

import marimo

__generated_with = "0.25.1"
app = marimo.App(width="medium", app_title="How rakaia works")

with app.setup(hide_code=True):
    import json

    import altair as alt
    import httpx
    import marimo as mo

    from rakaia import (
        AppendOptions,
        CollectingExecutor,
        Consumer,
        Delete,
        HandlerRegistry,
        InMemoryConsumerCursorStore,
        InMemoryOutcomeStore,
        InMemoryProjections,
        Retire,
        StreamStore,
        UpcasterRegistry,
        Update,
        Upsert,
        create_app,
        history_effects,
        project_latest,
        provenance,
        reconcile_by_key,
        reconcile_children,
        replay,
    )

    def make_quiz(questions):
        """One radio per question; the answers are marked in the next cell."""
        return mo.ui.array(
            [
                mo.ui.radio(options=q["options"], label=f"**{i + 1}. {q['q']}**")
                for i, q in enumerate(questions)
            ]
        )

    def mark(quiz, questions):
        """Score a quiz and explain every answered question."""
        lines, score = [], 0
        for i, (picked, q) in enumerate(zip(quiz.value, questions, strict=True)):
            if picked is None:
                lines.append(f"{i + 1}. *not answered yet*")
                continue
            right = picked == q["answer"]
            score += right
            verdict = "✅ Right." if right else f"❌ The answer is **{q['answer']}**."
            lines.append(f"{i + 1}. {verdict} {q['why']}")
        head = f"**Score: {score} / {len(questions)}**"
        kind = "success" if score == len(questions) else "info"
        return score, mo.callout(mo.md(head + "\n\n" + "\n\n".join(lines)), kind=kind)

    def stream_of(path, events, options=None):
        """A fresh in-memory store holding `events`, appended in list order."""
        store = StreamStore()
        store.create(path, content_type="application/json")
        for event in events:
            store.append(path, json.dumps(event).encode(), options)
        return store

    def show(rows, empty="*(empty)*"):
        """A table of dict rows, or a note when there are none."""
        if not rows:
            return mo.md(empty)
        return mo.ui.table(rows, selection=None, pagination=False)

    def effect_rows(effects):
        """Effects as table rows: what kind of change, to which row, with what."""
        rows = []
        for e in effects:
            values = getattr(e, "defaults", None) or getattr(e, "patch", None) or ""
            rows.append(
                {
                    "change": type(e).__name__,
                    "table": e.model_label,
                    "which row": json.dumps(e.lookup),
                    "values": json.dumps(values) if values else "",
                }
            )
        return rows

    def order_rule(event):
        """The rule used from section 3 on: one row per order, holding its amount."""
        return Upsert(
            model_label="shop.Order",
            lookup={"order": event["order"]},
            defaults={"amount": event["amount"]},
        )


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    # How rakaia works

    Rakaia keeps a **record of everything that happened**, in order, and builds your
    tables by reading that record back. When a table turns out to be wrong, you fix
    the code and rebuild it: no repair script, no guessing what a row should have
    been.

    This notebook builds the idea up one step at a time. It starts with what a
    stream is and ends with the protocol server and the Django integration.

    **How to use it.** Read a section, play with its example, then take the short
    quiz at the end. Each answer comes with a one-line reason, and your total is at
    the bottom. Every example runs the real `rakaia` package from this checkout, in
    memory. Nothing here needs a database or a running server, and everything is
    made up: a small shop with a handful of orders.

    | # | Section | Level |
    |---|---|---|
    | 1 | A stream is a list of facts | easy |
    | 2 | Positions, and picking up where you left off | easy |
    | 3 | Tables come from the log | easy |
    | 4 | A rule describes a change; it doesn't make it | medium |
    | 5 | Rebuild, and rehearse first | medium |
    | 6 | One change, many rows, nothing left behind | medium |
    | 7 | Rules that changed over time | medium |
    | 8 | Events whose shape changed | medium |
    | 9 | Who, when and why | medium |
    | 10 | Two tables from one log | harder |
    | 11 | Rules that read other tables | harder |
    | 12 | A person's decision and a rule's, on the same table | harder |
    | 13 | Consumers that keep their place | harder |
    | 14 | Over the wire, and in Django | harder |
    """)
    return


# ---------------------------------------------------------------- section 1


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 1. A stream is a list of facts

    A **stream** is a named list of **events**. Each event records one thing that
    *happened*: "order 1 was placed for K100", "order 1's amount was changed to
    K120".

    You can only do one thing to a stream: **append** to the end. Events are never
    edited and never deleted. If something was recorded wrong, a *new* event
    corrects it, so the stream shows both the mistake and the fix.

    Each event gets a **position** as it is appended: 0, 1, 2 and so on. The
    order of the stream is the order the appends arrived, nothing else.

    Try it. Pick the events that have happened, in any order, and look at the
    stream the real in-memory store builds from them.
    """)
    return


@app.cell(hide_code=True)
def _():
    s1_events = {
        "Order 1 placed, K100": {"type": "placed", "order": 1, "amount": 100},
        "Order 2 placed, K50": {"type": "placed", "order": 2, "amount": 50},
        "Order 1 amount corrected to K120": {
            "type": "amended",
            "order": 1,
            "amount": 120,
        },
        "Order 3 placed, K75": {"type": "placed", "order": 3, "amount": 75},
    }
    s1_pick = mo.ui.multiselect(
        options=list(s1_events),
        value=list(s1_events)[:3],
        label="Events, in the order they were appended",
        full_width=True,
    )
    s1_pick
    return s1_events, s1_pick


@app.cell
def _(s1_events, s1_pick):
    _store = stream_of("orders", [s1_events[k] for k in s1_pick.value])
    _messages, _ = _store.read("orders")
    show(
        [{"position": i, "event": m.data.decode()} for i, m in enumerate(_messages)],
        "*The stream is empty. Pick an event.*",
    )
    return


@app.cell(hide_code=True)
def _(s1_pick):
    _boxes = [f'  E{i}["{i}: {k}"]' for i, k in enumerate(s1_pick.value)]
    _chain = " --> ".join([f"E{i}" for i in range(len(s1_pick.value))] + ["N"])
    mo.mermaid(
        "flowchart LR\n"
        + "\n".join(_boxes)
        + '\n  N(["next append goes here"])\n'
        + f"  {_chain}\n"
        + "  classDef new stroke-dasharray: 4 4\n  class N new\n"
    ) if s1_pick.value else mo.md("")
    return


@app.cell(hide_code=True)
def _():
    q1 = [
        {
            "q": "Order 1's amount was entered wrong. How does the stream record the fix?",
            "options": [
                "Edit the event that has the wrong amount",
                "Delete that event and append a correct one",
                "Append a new event that corrects the amount",
            ],
            "answer": "Append a new event that corrects the amount",
            "why": "A stream is append-only: nothing in it is edited or deleted, so the mistake and its fix both stay on record.",
        },
        {
            "q": "What decides the order of the events in a stream?",
            "options": [
                "The order they were appended",
                "A timestamp inside each event",
                "The order number they mention",
            ],
            "answer": "The order they were appended",
            "why": "Positions are handed out as appends arrive. That order is the one every reader sees.",
        },
        {
            "q": "Which one is the original that everything else is built from?",
            "options": ["The stream", "The tables", "Both, kept in step"],
            "answer": "The stream",
            "why": "Rakaia turns the usual arrangement around: the log is the original and the tables are produced by reading it back.",
        },
    ]
    quiz1 = make_quiz(q1)
    mo.vstack([mo.md("### Quiz 1"), quiz1])
    return q1, quiz1


@app.cell(hide_code=True)
def _(q1, quiz1):
    score1, _fb = mark(quiz1, q1)
    _fb
    return (score1,)


# ---------------------------------------------------------------- section 2


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 2. Positions, and picking up where you left off

    Every event also gets an **offset**: a bookmark string. Anyone reading a stream
    keeps the offset of the last event they handled. Next time, they ask for
    "everything after this offset" and get only what is new.

    Two rules matter:

    * **Offsets are opaque.** Store them and hand them back, but don't take them
      apart or do arithmetic on them. The in-memory store used here counts bytes.
      The database and file stores count entries. A client that "adds one" works
      on one store and breaks on the next.
    * **Offsets only go up.** A later event always has a larger offset, so two
      offsets can be compared to see which came first.

    `-1` is a special offset meaning "from the start". Move the slider to say how
    many events you have already read. The cell then reads from your bookmark.
    """)
    return


@app.cell(hide_code=True)
def _():
    s2_read = mo.ui.slider(0, 5, value=2, label="Events already read", show_value=True)
    s2_read
    return (s2_read,)


@app.cell
def _(s2_read):
    _store = stream_of(
        "orders",
        [{"type": "placed", "order": n, "amount": 10 * n} for n in range(1, 6)],
    )
    _all, _ = _store.read("orders")
    _bookmark = _all[s2_read.value - 1].offset if s2_read.value else "-1"
    _new, _up_to_date = _store.read("orders", _bookmark)
    mo.vstack(
        [
            mo.md(f"Your bookmark: `{_bookmark}`"),
            show(
                [{"offset": m.offset, "event": m.data.decode()} for m in _new],
                "*Nothing new: you are up to date.*",
            ),
            mo.md(f"Up to date after this read: **{_up_to_date}**"),
        ]
    )
    return


@app.cell(hide_code=True)
def _(s2_read):
    _rows = [
        {
            "position": i,
            "state": "already read" if i < s2_read.value else "returned by this read",
        }
        for i in range(5)
    ]
    _points = (
        alt.Chart(alt.Data(values=_rows))
        .mark_square(size=600)
        .encode(
            x=alt.X("position:O", title="Position in the stream"),
            color=alt.Color(
                "state:N",
                title=None,
                scale=alt.Scale(
                    domain=["already read", "returned by this read"],
                    range=["#bdbdbd", "#2e7d32"],
                ),
                legend=alt.Legend(orient="top"),
            ),
        )
    )
    _mark = (
        alt.Chart(alt.Data(values=[{"position": max(s2_read.value - 1, 0)}]))
        .mark_text(text="▲ your bookmark", dy=28, fontWeight="bold")
        .encode(x="position:O")
    )
    (_points + _mark if s2_read.value else _points).properties(width=420, height=90)
    return


@app.cell(hide_code=True)
def _():
    q2 = [
        {
            "q": "You stopped after reading the event at bookmark B. Where does a read from B start?",
            "options": [
                "At the event at B, again",
                "At the event right after B",
                "At the start of the stream",
            ],
            "answer": "At the event right after B",
            "why": "A read returns what comes after the offset you hand it, so a reader that resumes from its bookmark sees each event once.",
        },
        {
            "q": "Can a client work out the next offset by adding to the one it has?",
            "options": [
                "Yes, offsets are counters",
                "No, offsets are opaque and their format depends on the store",
            ],
            "answer": "No, offsets are opaque and their format depends on the store",
            "why": "The protocol says clients must not interpret offsets. The in-memory store counts bytes, while the database and file stores count entries.",
        },
        {
            "q": "What does the offset `-1` ask for?",
            "options": [
                "The last event",
                "Everything from the start",
                "Only events appended from now on",
            ],
            "answer": "Everything from the start",
            "why": "`-1` is the protocol's sentinel for the beginning of a stream. Its sibling `now` skips to the end.",
        },
    ]
    quiz2 = make_quiz(q2)
    mo.vstack([mo.md("### Quiz 2"), quiz2])
    return q2, quiz2


@app.cell(hide_code=True)
def _(q2, quiz2):
    score2, _fb = mark(quiz2, q2)
    _fb
    return (score2,)


# ---------------------------------------------------------------- section 3


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 3. Tables come from the log

    A **projection** is a table built by reading the stream, never written to
    directly. To build it you write a **handler**: a small function that takes
    *one event* and says what that event means for the table.

    ```python
    def order_rule(event):
        return Upsert(
            model_label="shop.Order",
            lookup={"order": event["order"]},
            defaults={"amount": event["amount"]},
        )
    ```

    `Upsert` means "make the row for this order hold this amount, creating it if
    needed". **Replay** reads the stream from the start, runs every event through
    its handler and applies what comes back.

    Because the table only comes from the stream, you can drop it and build it
    again whenever you like. Pick events and watch the table follow.
    """)
    return


@app.cell(hide_code=True)
def _(s1_events):
    s3_pick = mo.ui.multiselect(
        options=list(s1_events),
        value=list(s1_events),
        label="Events in the stream",
        full_width=True,
    )
    s3_pick
    return (s3_pick,)


@app.cell
def _(s1_events, s3_pick):
    _store = stream_of("orders", [s1_events[k] for k in s3_pick.value])
    _rules = HandlerRegistry()
    _rules.register("order", "orders", order_rule, effective_from=0)
    _table = InMemoryProjections()
    _result = replay(_store, "orders", _table, handler_registry=_rules)
    mo.vstack(
        [
            mo.md(f"Replayed **{_result.events_processed}** events."),
            show(_table.rows("shop.Order"), "*No orders yet.*"),
        ]
    )
    return


@app.cell(hide_code=True)
def _():
    mo.mermaid(
        "flowchart LR\n"
        '  S[("stream: orders")] -- "replay: one event at a time" --> H["order_rule(event)"]\n'
        '  H -- "returns" --> U["Upsert shop.Order<br/>lookup order, defaults amount"]\n'
        '  U -- "applied" --> T[("table: shop.Order")]\n'
        '  T -. "drop it any time" .-> X(["rebuilt by the next replay"])\n'
    )
    return


@app.cell(hide_code=True)
def _():
    q3 = [
        {
            "q": "Someone drops the orders table by accident. What is lost?",
            "options": [
                "Every order",
                "Nothing: replaying the stream builds it again",
                "Only the corrections",
            ],
            "answer": "Nothing: replaying the stream builds it again",
            "why": "A projection is derived from the log, so you can drop it and regenerate it at will.",
        },
        {
            "q": "A column in the table was worked out wrong for months. How is it fixed?",
            "options": [
                "Run an UPDATE on the table",
                "Fix the handler and rebuild the table",
                "Append corrected rows to the table",
            ],
            "answer": "Fix the handler and rebuild the table",
            "why": "The table is produced by the handler, so a bug in the handler is fixed in the handler. Replay then corrects old rows too.",
        },
        {
            "q": "What does a handler receive?",
            "options": [
                "One event",
                "The whole stream",
                "A database connection",
            ],
            "answer": "One event",
            "why": "A handler is a function of one event. That is what makes it safe to run again, in any process, at any time.",
        },
    ]
    quiz3 = make_quiz(q3)
    mo.vstack([mo.md("### Quiz 3"), quiz3])
    return q3, quiz3


@app.cell(hide_code=True)
def _(q3, quiz3):
    score3, _fb = mark(quiz3, q3)
    _fb
    return (score3,)


# ---------------------------------------------------------------- section 4


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 4. A rule describes a change; it doesn't make it

    A handler never touches the database. It *returns* a description of a change,
    called an **effect**, and something else (an **executor**) applies it.
    There are exactly four kinds:

    | Effect | What it does to the rows matching its lookup |
    |---|---|
    | `Upsert` | Create the row, or update it if it exists. The usual one. |
    | `Update` | Update the row **only if it exists**. Never creates one. |
    | `Delete` | Remove the rows. |
    | `Retire` | Keep the rows, but stamp them, e.g. `cancelled_at`. A soft delete. |

    `Update` exists for a table that two handlers share: the second one fills in its
    own columns without ever being responsible for the row existing.

    `Retire` must stamp the *event's* time, never "now". If it used "now", the same
    replay would give a different table every time you ran it.

    Pick an effect and a target. The table starts with orders 1 and 2. Order 9
    doesn't exist.
    """)
    return


@app.cell(hide_code=True)
def _():
    s4_kind = mo.ui.radio(
        options=["Upsert", "Update", "Delete", "Retire"],
        value="Update",
        label="Effect",
        inline=True,
    )
    s4_target = mo.ui.dropdown(
        options={"order 1": 1, "order 2": 2, "order 9 (no such row)": 9},
        value="order 9 (no such row)",
        label="Target",
    )
    mo.hstack([s4_kind, s4_target], justify="start", gap=2)
    return s4_kind, s4_target


@app.cell
def _(s4_kind, s4_target):
    _table = InMemoryProjections()
    _table.apply(
        [
            Upsert("shop.Order", {"order": 1}, {"amount": 120}),
            Upsert("shop.Order", {"order": 2}, {"amount": 50}),
        ]
    )
    _before = [dict(r) for r in _table.rows("shop.Order")]
    _lookup = {"order": s4_target.value}
    _effect = {
        "Upsert": Upsert("shop.Order", _lookup, {"amount": 999}),
        "Update": Update("shop.Order", _lookup, {"amount": 999}),
        "Delete": Delete("shop.Order", _lookup),
        "Retire": Retire("shop.Order", _lookup, {"cancelled_at": "2026-10-04"}),
    }[s4_kind.value]
    _table.apply([_effect])
    mo.vstack(
        [
            mo.md("**The effect** (a description, nothing has happened yet):"),
            show(effect_rows([_effect])),
            mo.hstack(
                [
                    mo.vstack([mo.md("**Before**"), show(_before)]),
                    mo.vstack(
                        [
                            mo.md("**After applying it**"),
                            show(_table.rows("shop.Order")),
                        ]
                    ),
                ],
                widths="equal",
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _():
    mo.mermaid(
        "flowchart LR\n"
        '  E(["event"]) --> H["handler<br/>pure: decides what it means"]\n'
        '  H --> F["effects<br/>Upsert / Update / Delete / Retire<br/>(a description, as data)"]\n'
        '  F --> D["DjangoExecutor<br/>writes the database"]\n'
        '  F --> M["InMemoryProjections<br/>writes dicts (this notebook)"]\n'
        '  F --> C["CollectingExecutor<br/>writes nothing: a rehearsal"]\n'
        '  H --> X["ExternalEffect<br/>email, webhook"]\n'
        '  X --> Y(["your code, from ReplayResult.external<br/>never an executor"])\n'
    )
    return


@app.cell(hide_code=True)
def _():
    q4 = [
        {
            "q": "Why does a handler return a description of a change instead of making it?",
            "options": [
                "It is faster",
                "So the same handlers can write to the database, to memory, or to a rehearsal that writes nothing",
                "Django requires it",
            ],
            "answer": "So the same handlers can write to the database, to memory, or to a rehearsal that writes nothing",
            "why": "The effects are the same and the executor decides where they go. The next section uses that to rehearse a rebuild.",
        },
        {
            "q": "An `Update` names a row that doesn't exist. What happens?",
            "options": [
                "The row is created",
                "An error is raised",
                "Nothing",
            ],
            "answer": "Nothing",
            "why": "`Update` is update-if-exists, a clean no-op when nothing matches. It never claims responsibility for a row existing.",
        },
        {
            "q": "A `Retire` stamps `cancelled_at`. Where should the time come from?",
            "options": [
                "The clock, when the replay runs",
                "The event that caused it",
            ],
            "answer": "The event that caused it",
            "why": "A replay has to give the same table every time it runs. Using the current time would change the table on every run.",
        },
    ]
    quiz4 = make_quiz(q4)
    mo.vstack([mo.md("### Quiz 4"), quiz4])
    return q4, quiz4


@app.cell(hide_code=True)
def _(q4, quiz4):
    score4, _fb = mark(quiz4, q4)
    _fb
    return (score4,)


# ---------------------------------------------------------------- section 5


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 5. Rebuild, and rehearse first

    Two promises follow from handlers returning effects.

    **Running replay twice changes nothing.** Each `Upsert` finds its row by its
    lookup and overwrites it, so a second replay lands on the same rows. A rule
    that *inserted* a row per event would double the table on every run.

    **You can rehearse.** Hand replay a `CollectingExecutor` and it records every
    effect without applying any of them. You get a list of what a rebuild *would*
    write. The Django side goes further: its rehearsal blocks writes to the
    database outright, and its rebuild check refuses to call a run that compared
    nothing a pass.

    Move the slider to replay the same stream more times.
    """)
    return


@app.cell(hide_code=True)
def _():
    s5_runs = mo.ui.slider(1, 5, value=1, label="Times replayed", show_value=True)
    s5_runs
    return (s5_runs,)


@app.cell
def _(s1_events, s5_runs):
    _store = stream_of("orders", list(s1_events.values()))
    _rules = HandlerRegistry()
    _rules.register("order", "orders", order_rule, effective_from=0)

    _rehearsal = CollectingExecutor()
    replay(_store, "orders", _rehearsal, handler_registry=_rules)

    _table = InMemoryProjections()
    for _ in range(s5_runs.value):
        replay(_store, "orders", _table, handler_registry=_rules)

    mo.vstack(
        [
            mo.md(
                "**The rehearsal**: what a rebuild would write, with nothing written"
            ),
            show(effect_rows(_rehearsal.effects)),
            mo.md(
                f"**The table after {s5_runs.value} replay(s)**: "
                f"{len(_table.rows('shop.Order'))} rows. An insert-per-event rule "
                f"would have left {len(s1_events) * s5_runs.value}."
            ),
            show(_table.rows("shop.Order")),
        ]
    )
    return


@app.cell(hide_code=True)
def _(s1_events, s5_runs):
    _rows = []
    for _n in range(1, 6):
        _rows.append({"replays": _n, "rows": 3, "rule": "Upsert (rakaia)"})
        _rows.append(
            {"replays": _n, "rows": len(s1_events) * _n, "rule": "insert per event"}
        )
    _lines = (
        alt.Chart(alt.Data(values=_rows))
        .mark_line(point=True)
        .encode(
            x=alt.X("replays:O", title="Times replayed"),
            y=alt.Y("rows:Q", title="Rows in the table"),
            color=alt.Color("rule:N", title=None, legend=alt.Legend(orient="top")),
        )
    )
    _now = (
        alt.Chart(alt.Data(values=[{"replays": s5_runs.value}]))
        .mark_rule(strokeDash=[4, 4])
        .encode(x="replays:O")
    )
    (_lines + _now).properties(width=420, height=200)
    return


@app.cell(hide_code=True)
def _():
    q5 = [
        {
            "q": "Why can you replay the same stream five times and get the same table?",
            "options": [
                "Replay deletes the table first",
                "Each upsert finds its row by its lookup and overwrites it",
                "Replay remembers which events it already applied",
            ],
            "answer": "Each upsert finds its row by its lookup and overwrites it",
            "why": "That is what makes replay idempotent: re-running converges on the same rows instead of adding to them.",
        },
        {
            "q": "What does a replay through `CollectingExecutor` write?",
            "options": [
                "Nothing: it records the effects",
                "A copy of the table",
                "The effects, inside a transaction it rolls back",
            ],
            "answer": "Nothing: it records the effects",
            "why": "It is the dry-run executor. Same handlers, same effects, no destination.",
        },
        {
            "q": "On Django, a rebuild check that compared nothing at all is…",
            "options": [
                "a pass",
                "refused, unless you say an empty result was expected",
                "silently skipped",
            ],
            "answer": "refused, unless you say an empty result was expected",
            "why": "A check over nothing is vacuously true and is usually a broken rebuild, so it raises unless you pass `allow_empty`.",
        },
    ]
    quiz5 = make_quiz(q5)
    mo.vstack([mo.md("### Quiz 5"), quiz5])
    return q5, quiz5


@app.cell(hide_code=True)
def _(q5, quiz5):
    score5, _fb = mark(quiz5, q5)
    _fb
    return (score5,)


# ---------------------------------------------------------------- section 6


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 6. One change, many rows, nothing left behind

    Some events fill in many rows at once: an order's lines, a form's repeated
    answers. Upserting each line is not enough. When the order is saved again with
    *fewer* lines, the old extra row is never touched. It stays in the table as an
    **orphan** that no event mentions any more.

    `reconcile_children` returns one `Upsert` per line, followed by one `Delete`
    for this order's lines that *spares* the ones still present. The leftovers go,
    and running it again changes nothing.

    Choose the lines for the first save and the second save, and compare.
    """)
    return


@app.cell(hide_code=True)
def _():
    _items = ["tea", "milk", "bread", "rice"]
    s6_first = mo.ui.multiselect(_items, value=_items[:3], label="First save")
    s6_second = mo.ui.multiselect(_items, value=_items[:1], label="Second save")
    mo.hstack([s6_first, s6_second], justify="start", gap=2)
    return s6_first, s6_second


@app.cell
def _(s6_first, s6_second):
    def _upserts_only(items):
        return [
            Upsert("shop.Line", {"order": 1, "idx": i}, {"item": item})
            for i, item in enumerate(items)
        ]

    def _reconciled(items):
        return reconcile_children(
            "shop.Line", {"order": 1}, "idx", items, lambda item: {"item": item}
        )

    _naive, _reconcile = InMemoryProjections(), InMemoryProjections()
    for _save in (s6_first.value, s6_second.value):
        _naive.apply(_upserts_only(_save))
        _reconcile.apply(_reconciled(_save))

    mo.vstack(
        [
            mo.md("**What `reconcile_children` returns for the second save**"),
            show(effect_rows(_reconciled(s6_second.value))),
            mo.hstack(
                [
                    mo.vstack(
                        [mo.md("**Upserts only**"), show(_naive.rows("shop.Line"))]
                    ),
                    mo.vstack(
                        [
                            mo.md("**`reconcile_children`**"),
                            show(_reconcile.rows("shop.Line")),
                        ]
                    ),
                ],
                widths="equal",
            ),
        ]
    )
    return


@app.cell(hide_code=True)
def _():
    q6 = [
        {
            "q": "An order is saved with three lines, then with two. With upserts alone, how many line rows are left?",
            "options": ["Two", "Three", "Five"],
            "answer": "Three",
            "why": "The second save overwrites lines 0 and 1, and nothing ever touches line 2, so it is orphaned.",
        },
        {
            "q": "How does `reconcile_children` get rid of the leftover?",
            "options": [
                "It deletes every line and inserts them again",
                "One delete for the order's lines that spares the positions still present",
                "It leaves it for a cleanup job",
            ],
            "answer": "One delete for the order's lines that spares the positions still present",
            "why": "That is the `Delete` with `Exclude(idx__in=[...])` in the table above.",
        },
        {
            "q": "What identifies a line row to `reconcile_children`?",
            "options": [
                "The order plus the line's position",
                "The item name",
                "A random id",
            ],
            "answer": "The order plus the line's position",
            "why": "It is positional. When rows have a natural key instead, `reconcile_by_key` does the same job keyed on it (section 12).",
        },
    ]
    quiz6 = make_quiz(q6)
    mo.vstack([mo.md("### Quiz 6"), quiz6])
    return q6, quiz6


@app.cell(hide_code=True)
def _(q6, quiz6):
    score6, _fb = mark(quiz6, q6)
    _fb
    return (score6,)


# ---------------------------------------------------------------- section 7


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 7. Rules that changed over time

    Business rules change. Say orders were tax-free, and from some point on carry
    10% tax. If you simply change the handler, the next replay charges 10% on
    *every* order, including ones placed when there was no tax. The rebuild has
    quietly rewritten history.

    Instead, register each version of the rule against a **range of positions**:

    ```python
    rules.register("tax", "orders", no_tax, effective_from=0, effective_to=k)
    rules.register("tax", "orders", ten_percent, effective_from=k)
    ```

    Ranges include their start and exclude their end. Each event runs through the
    version whose range covers its position, so old orders keep their old answer.
    Two ranges that overlap are refused when they are registered. A position no
    range covers stops the replay when it reaches that event.

    **A position range is a single cutover point, so use it only when there is
    one.** It fits a rule the *server* changes at a moment of its choosing, for
    everyone at once, like a tax rate. It does not fit a change that clients pick
    up at their own pace, such as a new app version that sends a new event shape.
    During that rollout, old and new clients both keep appending, so the two
    versions arrive interleaved and no position separates them. That case is
    section 8.

    Move the slider to say where the new rule starts.
    """)
    return


@app.cell(hide_code=True)
def _():
    s7_from = mo.ui.slider(
        0, 6, value=3, label="10% tax from position", show_value=True
    )
    s7_gap = mo.ui.checkbox(label="Leave a gap of one event between the two versions")
    mo.vstack([s7_from, s7_gap])
    return s7_from, s7_gap


@app.cell
def _(s7_from, s7_gap):
    def _no_tax(event):
        return Upsert("shop.Order", {"order": event["order"]}, {"tax": 0})

    def _ten_percent(event):
        return Upsert(
            "shop.Order", {"order": event["order"]}, {"tax": event["amount"] // 10}
        )

    _store = stream_of(
        "orders",
        [{"type": "placed", "order": n, "amount": 100 * n} for n in range(1, 7)],
    )
    _k = s7_from.value
    _rules = HandlerRegistry()
    if _k > 0:
        _rules.register("tax", "orders", _no_tax, effective_from=0, effective_to=_k)
    _rules.register("tax", "orders", _ten_percent, effective_from=_k + s7_gap.value)
    _table = InMemoryProjections()
    try:
        replay(_store, "orders", _table, handler_registry=_rules)
        _out = show(_table.rows("shop.Order"))
    except Exception as _error:
        _out = mo.callout(
            mo.md(f"**{type(_error).__name__}**: {_error}"), kind="danger"
        )
    _out
    return


@app.cell(hide_code=True)
def _(s7_from, s7_gap):
    _k, _g = s7_from.value, int(s7_gap.value)
    _ranges = []
    if _k > 0:
        _ranges.append({"version": "no tax", "start": 0, "end": _k})
    _ranges.append({"version": "10% tax", "start": _k + _g, "end": 6})
    _events = []
    for _p in range(6):
        _who = (
            "no tax"
            if _p < _k
            else ("10% tax" if _p >= _k + _g else "no version: replay stops")
        )
        _events.append({"position": _p, "runs through": _who})
    _colours = alt.Scale(
        domain=["no tax", "10% tax", "no version: replay stops"],
        range=["#1565c0", "#2e7d32", "#c62828"],
    )
    _bars = (
        alt.Chart(alt.Data(values=_ranges))
        .mark_bar(height=18, opacity=0.35)
        .encode(
            x=alt.X("start:Q", title="Position", scale=alt.Scale(domain=[0, 6])),
            x2="end:Q",
            y=alt.Y("version:N", title=None),
            color=alt.Color("version:N", scale=_colours, legend=None),
        )
    )
    _dots = (
        alt.Chart(alt.Data(values=_events))
        .mark_circle(size=160)
        .encode(
            x=alt.X("position:Q"),
            color=alt.Color(
                "runs through:N", scale=_colours, legend=alt.Legend(orient="top")
            ),
        )
        .properties(height=40)
    )
    alt.vconcat(
        _bars.properties(
            width=460, height=70, title="Each version's range [start, end)"
        ),
        _dots.properties(width=460, title="Which version each event runs through"),
    )
    return


@app.cell(hide_code=True)
def _():
    q7 = [
        {
            "q": "Tax starts at 10% from position 3. Why not just change the handler?",
            "options": [
                "The next replay would charge 10% on the old orders too",
                "Handlers can't be edited once registered",
                "It would be slower",
            ],
            "answer": "The next replay would charge 10% on the old orders too",
            "why": "A replay runs every event through the code you give it, so changing the rule in place changes the past. A version range keeps the past's answer.",
        },
        {
            "q": "What picks which version of a rule an event runs through?",
            "options": [
                "The event's timestamp",
                "The event's position in the stream",
                "The order number",
            ],
            "answer": "The event's position in the stream",
            "why": "Ranges are counted in positions (0, 1, 2…), with the start included and the end excluded.",
        },
        {
            "q": "When is a position range the right way to change a rule?",
            "options": [
                "When the server changes it at one moment, for everyone",
                "When clients move to a new event format over a few weeks",
                "Whenever a handler is edited",
            ],
            "answer": "When the server changes it at one moment, for everyone",
            "why": "A range is one cutover point in the log. During a client rollout, old and new events interleave and there is no such point (section 8).",
        },
    ]
    quiz7 = make_quiz(q7)
    mo.vstack([mo.md("### Quiz 7"), quiz7])
    return q7, quiz7


@app.cell(hide_code=True)
def _(q7, quiz7):
    score7, _fb = mark(quiz7, q7)
    _fb
    return (score7,)


# ---------------------------------------------------------------- section 8


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 8. Events whose shape changed, while clients catch up

    Section 7 was about the *rule* changing. Sometimes the *event* changes: version
    1 of the app writes `qty` and version 2 writes `quantity`. Clients don't all
    update on the same day. For as long as the rollout takes, both versions keep
    appending, so the stream holds v1 and v2 events **interleaved**. An old
    client's event can arrive long after the first v2 event.

    So the version can't be worked out from the position. Each event has to say
    which version it is. A client stamps a `schema_version` on what it sends, and
    an event without one counts as 1. An **upcaster** is a small function that
    turns version N into version N+1. Replay runs the chain on each event as it is
    read, so the handler only ever sees today's shape, whatever order the
    versions arrived in:

    ```python
    def qty_to_quantity(event):
        event = dict(event)
        event["quantity"] = event.pop("qty")
        return event

    upcasters.register("orders", 1, qty_to_quantity)   # 1 -> 2
    ```

    The upcaster doesn't need to bump `schema_version`; the chain does that. If v2
    adds a field that v1 never had, the upcaster is where it gets its default.

    The stream below is a rollout in progress: v1 and v2 events, mixed. Compare the
    upcaster with the tempting alternative of a section 7 range that switches to
    the v2 handler at the first v2 event.
    """)
    return


@app.cell(hide_code=True)
def _():
    s8_how = mo.ui.radio(
        [
            "An upcaster, by each event's schema_version",
            "A position range: no version stamps, v2 handler from the first v2 event",
        ],
        value="An upcaster, by each event's schema_version",
        label="How the change is handled",
    )
    s8_how
    return (s8_how,)


@app.cell
def _(s8_how):
    def _qty_to_quantity(event):
        event = dict(event)
        event["quantity"] = event.pop("qty")
        return event

    def _line_count(event):
        return Upsert(
            "shop.Order", {"order": event["order"]}, {"items": event["quantity"]}
        )

    def _line_count_v1(event):
        return Upsert("shop.Order", {"order": event["order"]}, {"items": event["qty"]})

    _by_range = s8_how.value.startswith("A position range")
    # v1 clients send `qty`; v2 clients send `quantity` and, unless the team is
    # relying on a position range instead, stamp their version.
    _v2 = {} if _by_range else {"schema_version": 2}
    _events = [
        {"order": 1, "qty": 2},
        {"order": 2, "quantity": 3, **_v2},
        {"order": 3, "qty": 5},
        {"order": 4, "quantity": 4, **_v2},
        {"order": 5, "qty": 1},
    ]
    _store = stream_of("orders", _events)
    _first_v2 = next(i for i, e in enumerate(_events) if "quantity" in e)
    _rules = HandlerRegistry()
    _upcasters = UpcasterRegistry()
    if _by_range:
        _rules.register("items", "orders", _line_count_v1, 0, _first_v2)
        _rules.register("items", "orders", _line_count, _first_v2)
    else:
        _rules.register("items", "orders", _line_count, effective_from=0)
        _upcasters.register("orders", 1, _qty_to_quantity)
    _late_v1 = next(i for i, e in enumerate(_events) if i > _first_v2 and "qty" in e)
    _table = InMemoryProjections()
    try:
        replay(
            _store,
            "orders",
            _table,
            handler_registry=_rules,
            upcaster_registry=_upcasters,
        )
        _out = show(_table.rows("shop.Order"))
    except Exception as _error:
        _out = mo.callout(
            mo.md(
                f"**{type(_error).__name__}: {_error}**. The range sent position "
                f"{_late_v1}, an old client's event that arrived after the cutover, "
                "to the v2 handler."
            ),
            kind="danger",
        )
    _stored, _ = _store.read("orders")
    mo.vstack(
        [
            mo.hstack(
                [
                    mo.vstack(
                        [
                            mo.md("**Stored events (never rewritten)**"),
                            show(
                                [
                                    {"position": i, "event": m.data.decode()}
                                    for i, m in enumerate(_stored)
                                ]
                            ),
                        ]
                    ),
                    mo.vstack([mo.md("**Table**"), _out]),
                ],
                widths="equal",
            )
        ]
    )
    return


@app.cell(hide_code=True)
def _(s8_how):
    _by_range = s8_how.value.startswith("A position range")
    _versions = ["v1", "v2", "v1", "v2", "v1"]
    _first_v2 = _versions.index("v2")
    _rows = []
    for _p, _v in enumerate(_versions):
        if _by_range:
            _sent = "v1 handler" if _p < _first_v2 else "v2 handler"
            _ok = (_v == "v1") == (_p < _first_v2)
        else:
            _sent, _ok = "upcast to v2, then the handler", True
        _rows.append(
            {
                "position": _p,
                "client": _v,
                "sent to": _sent,
                "outcome": "ok" if _ok else "wrong handler",
            }
        )
    _dots = (
        alt.Chart(alt.Data(values=_rows))
        .mark_point(size=400, filled=True)
        .encode(
            x=alt.X("position:O", title="Position in the stream"),
            shape=alt.Shape("client:N", title="Sent by"),
            color=alt.Color(
                "outcome:N",
                scale=alt.Scale(
                    domain=["ok", "wrong handler"], range=["#2e7d32", "#c62828"]
                ),
                title=None,
            ),
            tooltip=["position:O", "client:N", "sent to:N", "outcome:N"],
        )
    )
    _layers = _dots
    if _by_range:
        _layers = (
            _dots
            + alt.Chart(alt.Data(values=[{"position": _first_v2}]))
            .mark_rule(strokeDash=[4, 4])
            .encode(x="position:O")
            + alt.Chart(alt.Data(values=[{"position": _first_v2, "t": "cutover"}]))
            .mark_text(dy=-30, align="left", dx=4)
            .encode(x="position:O", text="t:N")
        )
    _layers.properties(width=420, height=90)
    return


@app.cell(hide_code=True)
def _():
    q8 = [
        {
            "q": "Does registering an upcaster change the events already stored?",
            "options": [
                "Yes, it rewrites them once",
                "No, it translates them each time they are read",
            ],
            "answer": "No, it translates them each time they are read",
            "why": "Events are facts and are never edited. The left-hand table still says `qty` after a successful replay.",
        },
        {
            "q": "Clients move from v1 to v2 over a month, so both kinds of event arrive mixed together. What decides how each one is read?",
            "options": [
                "Its position, with a range starting at the first v2 event",
                "The `schema_version` each event carries, through the upcasters",
                "The date the event arrived",
            ],
            "answer": "The `schema_version` each event carries, through the upcasters",
            "why": "An old client's event can arrive after the first v2 one, so no position separates them. The second option above fails on exactly that event.",
        },
        {
            "q": "A v2 client forgets to stamp `schema_version: 2`. What happens to its events?",
            "options": [
                "They are read as v2 anyway",
                "They are read as version 1 and sent through the v1 upcaster",
                "They are refused when appended",
            ],
            "answer": "They are read as version 1 and sent through the v1 upcaster",
            "why": "A missing version counts as 1, so the version stamp is the producer's job and has to ship with the new client.",
        },
    ]
    quiz8 = make_quiz(q8)
    mo.vstack([mo.md("### Quiz 8"), quiz8])
    return q8, quiz8


@app.cell(hide_code=True)
def _(q8, quiz8):
    score8, _fb = mark(quiz8, q8)
    _fb
    return (score8,)


# ---------------------------------------------------------------- section 9


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 9. Who, when and why

    The event's payload says *what* happened. Next to it, each event carries an
    **envelope** that says *who, when and how*: a label (create, update, delete),
    open metadata such as the acting user, and two fields for finding things again:

    * **tags**, a set of short words such as `data-loss`;
    * a **correlation id**, one string such as an incident number, `INC-42`.

    You rarely pass these at every append. `provenance(...)` sets them once for a
    block of work, such as a whole web request or a whole recovery job, and every
    append inside it is stamped.

    On Django, tags and the correlation id are their own table and indexed column,
    not keys inside the metadata. That is so they can be found quickly on both
    SQLite and Postgres. A bad value, such as an empty tag or one over 100
    characters, is refused *before* anything is written.

    Fill in the job below. Two appends run inside the `provenance` block and one
    runs outside it.
    """)
    return


@app.cell(hide_code=True)
def _():
    s9_user = mo.ui.text(value="maria", label="Acting user")
    s9_incident = mo.ui.text(value="INC-42", label="Incident number (correlation)")
    s9_tags = mo.ui.multiselect(
        ["data-loss", "backup-restore", "manual-fix"],
        value=["backup-restore"],
        label="Tags",
    )
    s9_extra = mo.ui.text(
        value="", label="One more tag (try a blank or a very long one)"
    )
    mo.vstack(
        [mo.hstack([s9_user, s9_incident], justify="start", gap=2), s9_tags, s9_extra]
    )
    return s9_extra, s9_incident, s9_tags, s9_user


@app.cell
def _(s9_extra, s9_incident, s9_tags, s9_user):
    _store = stream_of("orders", [])
    _job = {"user": s9_user.value}
    if s9_incident.value:
        _job["correlation"] = s9_incident.value
    _tags = list(s9_tags.value) + ([s9_extra.value] if s9_extra.value else [])
    _refused = None
    try:
        with provenance(**_job):
            for _order in (1, 2):
                _store.append(
                    "orders",
                    json.dumps({"order": _order, "restored": True}).encode(),
                    AppendOptions(label="update", tags=tuple(_tags)),
                )
    except ValueError as _error:
        _refused = _error
    _store.append(
        "orders",
        json.dumps({"order": 3, "amount": 75}).encode(),
        AppendOptions(label="create"),
    )
    _messages, _ = _store.read("orders")
    _rows = [
        {
            "event": m.data.decode(),
            "label": m.label,
            "metadata": json.dumps(m.metadata),
            "tags": ", ".join(m.tags),
            "correlation id": m.correlation_id or "",
        }
        for m in _messages
    ]
    mo.vstack(
        [
            mo.callout(
                mo.md(f"**Refused, nothing written by the job:** {_refused}"),
                kind="danger",
            )
            if _refused
            else mo.md(""),
            show(_rows),
        ]
    )
    return


@app.cell(hide_code=True)
def _():
    q9 = [
        {
            "q": "Why are tags and the correlation id fields of their own, rather than keys inside the metadata?",
            "options": [
                "So Django can find them with an indexed lookup on any database",
                "Metadata can't hold lists",
                "To keep payloads small",
            ],
            "answer": "So Django can find them with an indexed lookup on any database",
            "why": "Finding a key inside JSON means a query that SQLite and Postgres spell differently and neither can index. A column and a table can be indexed on both.",
        },
        {
            "q": "A recovery job makes 200 appends that all belong to incident INC-42. What's the simplest way to file them?",
            "options": [
                "Pass the correlation id to each append",
                'Wrap the job in `provenance(correlation="INC-42")`',
                "Tag the stream",
            ],
            "answer": 'Wrap the job in `provenance(correlation="INC-42")`',
            "why": "Every append inside the block that doesn't give its own correlation id takes the block's. An explicit value still wins.",
        },
        {
            "q": "One tag in a batch is 101 characters long. What happens?",
            "options": [
                "It is cut to 100 characters",
                "The good tags are written and the bad one is dropped",
                "It is refused before anything is written",
            ],
            "answer": "It is refused before anything is written",
            "why": "Tags are checked when the append options are built, and again by every store, before any write.",
        },
    ]
    quiz9 = make_quiz(q9)
    mo.vstack([mo.md("### Quiz 9"), quiz9])
    return q9, quiz9


@app.cell(hide_code=True)
def _(q9, quiz9):
    score9, _fb = mark(quiz9, q9)
    _fb
    return (score9,)


# ---------------------------------------------------------------- section 10


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 10. Two tables from one log

    One stream can feed any number of tables. Two shapes come up all the time:

    * **Latest state** *folds* the stream: one row per order, overwritten by each
      event, and removed when the order is deleted (`project_latest`).
    * **History** *multiplies* it: one row per event, keyed by the order and a
      version number that counts that order's changes (`history_effects`). This is
      the audit trail, ready to query.

    Because both come from the same events, they can't disagree. Pick which of
    order 1's events have happened.
    """)
    return


@app.cell(hide_code=True)
def _():
    s10_events = {
        "created, K100": ("create", {"order": 1, "amount": 100}),
        "amended to K120": ("update", {"order": 1, "amount": 120}),
        "amended to K90": ("update", {"order": 1, "amount": 90}),
        "deleted": ("delete", {"order": 1}),
    }
    s10_pick = mo.ui.multiselect(
        list(s10_events), value=list(s10_events)[:3], label="Order 1's events"
    )
    s10_pick
    return s10_events, s10_pick


@app.cell
def _(s10_events, s10_pick):
    _store = stream_of("orders", [])
    for _key in s10_pick.value:
        _label, _event = s10_events[_key]
        _store.append(
            "orders",
            json.dumps(_event).encode(),
            AppendOptions(label=_label, metadata={"user": "maria"}),
        )
    _messages, _ = _store.read("orders")

    def _order_of(event):
        return event["order"]

    def _amount(_message, event):
        return {"amount": event.get("amount")}

    _latest, _history = InMemoryProjections(), InMemoryProjections()
    _latest.apply(
        project_latest(
            _messages, "shop.Order", subject_of=_order_of, defaults_of=_amount
        )
    )
    _history.apply(
        history_effects(
            _messages, "shop.OrderHistory", subject_of=_order_of, defaults_of=_amount
        )
    )
    mo.hstack(
        [
            mo.vstack(
                [
                    mo.md("**Latest state**"),
                    show(_latest.rows("shop.Order"), "*No row.*"),
                ]
            ),
            mo.vstack(
                [
                    mo.md("**History**"),
                    show(_history.rows("shop.OrderHistory"), "*No rows.*"),
                ]
            ),
        ],
        widths="equal",
    )
    return


@app.cell(hide_code=True)
def _():
    q10 = [
        {
            "q": "Order 1 is created, amended twice, then deleted. How many rows does each table have?",
            "options": [
                "Latest state 1, history 4",
                "Latest state 0, history 4",
                "Latest state 0, history 0",
            ],
            "answer": "Latest state 0, history 4",
            "why": "A delete removes the latest-state row. History keeps one row per event, including the delete.",
        },
        {
            "q": "What identifies a history row?",
            "options": [
                "The order and its version",
                "The order alone",
                "The time it was written",
            ],
            "answer": "The order and its version",
            "why": "History rows are keyed by `(subject, version)`, so replaying the same events upserts the same rows.",
        },
        {
            "q": "Why build the audit trail from the log rather than writing a history table alongside the real one?",
            "options": [
                "It uses less disk",
                "Both tables come from the same events, so they can't disagree",
                "Django can't write two tables in one save",
            ],
            "answer": "Both tables come from the same events, so they can't disagree",
            "why": "A history table written alongside is a copy that can drift. Two projections of one log are two readings of the same facts.",
        },
    ]
    quiz10 = make_quiz(q10)
    mo.vstack([mo.md("### Quiz 10"), quiz10])
    return q10, quiz10


@app.cell(hide_code=True)
def _(q10, quiz10):
    score10, _fb = mark(quiz10, q10)
    _fb
    return (score10,)


# ---------------------------------------------------------------- section 11


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 11. Rules that read other tables

    Some rows need a fact from a *different* event. A delivery report names a
    project by its code, and the report row should hold the project's name. The
    project's own event may arrive *after* the report.

    A handler can't query the database for it: that would make it depend on
    whatever happens to be in the table today, and the replay would stop being
    repeatable. Instead, give the handler a **stage**. Replay makes one pass per
    stage, in order. A handler in stage 1 is called with a read-only **reader**
    over the tables the earlier stages wrote:

    ```python
    def report(event, reader):          # stage 1
        project = reader.get("x.Project", code=event["project"])
        ...

    replay(store, "forms", table, handler_registry=rules, reader=table)
    ```

    Try both orders of events with each setup.
    """)
    return


@app.cell(hide_code=True)
def _():
    s11_order = mo.ui.radio(
        ["The report arrives first", "The project arrives first"],
        value="The report arrives first",
        label="Order in the stream",
    )
    s11_staged = mo.ui.radio(
        ["Two stages (projects, then reports)", "One pass for both"],
        value="Two stages (projects, then reports)",
        label="Setup",
    )
    mo.hstack([s11_order, s11_staged], justify="start", gap=4)
    return s11_order, s11_staged


@app.cell
def _(s11_order, s11_staged):
    _report = {"type": "report", "project": "P1", "wells": 3}
    _project = {"type": "project", "code": "P1", "name": "Village wells"}
    _events = (
        [_report, _project]
        if s11_order.value.startswith("The report")
        else [_project, _report]
    )
    _store = stream_of("forms", _events)
    _project_stage = 0 if s11_staged.value.startswith("Two") else 1

    def _project_row(event, _reader=None):
        return Upsert("x.Project", {"code": event["code"]}, {"name": event["name"]})

    def _report_row(event, reader):
        project = reader.get("x.Project", code=event["project"])
        return Upsert(
            "x.Report",
            {"project": event["project"]},
            {
                "wells": event["wells"],
                "project_name": project.name if project else None,
            },
        )

    _rules = HandlerRegistry()
    _rules.register(
        "project", "project", _project_row, 0, match_field="type", stage=_project_stage
    )
    _rules.register("report", "report", _report_row, 0, match_field="type", stage=1)
    _table = InMemoryProjections()
    replay(_store, "forms", _table, handler_registry=_rules, reader=_table)
    show(_table.rows("x.Report"))
    return


@app.cell(hide_code=True)
def _(s11_order, s11_staged):
    _report_first = s11_order.value.startswith("The report")
    _two = s11_staged.value.startswith("Two")
    _seq = ["report", "project"] if _report_first else ["project", "report"]
    _lines = [
        "sequenceDiagram",
        "  participant R as replay",
        "  participant T as tables",
    ]
    if _two:
        _lines += [
            "  Note over R: pass 1 (stage 0): projects only",
            "  R->>T: write Project P1",
            "  Note over R: pass 2 (stage 1): reports, with a reader",
            "  R->>T: read Project P1 (found)",
            "  R->>T: write Report with project name",
        ]
    else:
        _lines.append("  Note over R: one pass, in stream order")
        _found = False
        for _e in _seq:
            if _e == "project":
                _lines.append("  R->>T: write Project P1")
                _found = True
            else:
                _lines.append(
                    f"  R->>T: read Project P1 ({'found' if _found else 'not there yet'})"
                )
                _lines.append(
                    f"  R->>T: write Report {'with project name' if _found else 'WITHOUT a project name'}"
                )
    mo.mermaid("\n".join(_lines))
    return


@app.cell(hide_code=True)
def _():
    q11 = [
        {
            "q": "The report arrives before its project, and both handlers run in one pass. What does the report row hold?",
            "options": [
                "The project's name",
                "No project name",
                "Replay raises an error",
            ],
            "answer": "No project name",
            "why": "In a single pass the report is handled before the project row exists. Splitting into stages makes the order of arrival stop mattering.",
        },
        {
            "q": "What does a stage-1 handler get that a stage-0 handler doesn't?",
            "options": [
                "A database connection",
                "A read-only reader over the tables earlier stages wrote",
                "The whole stream",
            ],
            "answer": "A read-only reader over the tables earlier stages wrote",
            "why": "A stage > 0 handler is called `fn(event, reader)`. It reads through the reader and nothing else.",
        },
        {
            "q": "Why read through the reader rather than querying the table directly?",
            "options": [
                "The reader is faster",
                "The handler stays a function of what the replay gives it, so the result is repeatable",
                "Django forbids queries in handlers",
            ],
            "answer": "The handler stays a function of what the replay gives it, so the result is repeatable",
            "why": "ADR 0003: a handler reads only through the injected reader. On Django a guard can make an ambient query fail loudly during a rebuild.",
        },
    ]
    quiz11 = make_quiz(q11)
    mo.vstack([mo.md("### Quiz 11"), quiz11])
    return q11, quiz11


@app.cell(hide_code=True)
def _(q11, quiz11):
    score11, _fb = mark(quiz11, q11)
    _fb
    return (score11,)


# ---------------------------------------------------------------- section 12


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 12. A person's decision and a rule's, on the same table

    Alerts can be raised two ways: a **person** flags something, or a **rule**
    finds a problem, such as a missing field. Both kinds of alert live in one
    table. The danger is that re-running the rules wipes out what a person
    decided.

    `reconcile_by_key` keeps the rule's alerts in step with what is failing *now*.
    It upserts one row per current problem, keyed by a natural key (here, the alert
    type and the field). It then *retires* rule alerts that are no longer failing,
    stamping `resolved_at` with the event's time. Two settings keep it safe:

    * a **retire filter** limits the retire to the rule's own alert types, so a
      person's note is never touched;
    * a **transition** asks for one notification per alert it *actually* resolved.
      Notifications come back to you to send. Rakaia never sends them, so a
      rebuild never sends anyone the same email twice.

    The rule first runs with all three fields failing. Choose what is still failing
    on the second run.
    """)
    return


@app.cell(hide_code=True)
def _():
    s12_failing = mo.ui.multiselect(
        ["age", "name", "phone"],
        value=["name"],
        label="Still failing on the second run",
    )
    s12_filter = mo.ui.checkbox(
        value=True, label="Scope the retire to the rule's own alerts"
    )
    mo.vstack([s12_failing, s12_filter])
    return s12_failing, s12_filter


@app.cell
def _(s12_failing, s12_filter):
    _alerts = InMemoryProjections()
    _alerts.apply(
        [
            Upsert(
                "x.Alert",
                {"site": "S1", "alert_type": "note", "field_key": "age"},
                {"resolved_at": None, "by": "maria"},
            )
        ]
    )

    def _rule_run(failing, ts):
        return reconcile_by_key(
            "x.Alert",
            {"site": "S1"},
            ("alert_type", "field_key"),
            failing,
            key_fn=lambda field: {"alert_type": "missing", "field_key": field},
            defaults_fn=lambda _field: {"resolved_at": None, "by": "rule"},
            retire_filter={"alert_type__in": ["missing"]} if s12_filter.value else None,
            retire={"resolved_at": ts},
            transition_kind="alert_resolved",
        )

    _alerts.apply(_rule_run(["age", "name", "phone"], "2026-10-01"))
    _report = _alerts.apply(_rule_run(s12_failing.value, "2026-10-02"))
    _resolved = [row for _retire, rows in _report.retire_flips for row in rows]
    mo.vstack(
        [
            show(_alerts.rows("x.Alert")),
            mo.md(f"**Notifications to send after the second run:** {len(_resolved)}"),
            show(_resolved, "*None.*"),
        ]
    )
    return


@app.cell(hide_code=True)
def _(s12_filter):
    _scope = "R" if s12_filter.value else "R,N"
    mo.mermaid(
        "flowchart LR\n"
        '  P(["the rule pass<br/>reconcile_by_key"]) -- "upserts what fails now" --> R["rule alerts<br/>type: missing"]\n'
        '  P -- "retires what passes now" --> SC{{"retire scope"}}\n'
        "  SC --> R\n"
        + (
            '  SC -. "no retire_filter" .-> N["a person\'s note<br/>type: note"]\n'
            if not s12_filter.value
            else '  N["a person\'s note<br/>type: note"]\n'
        )
        + '  M(["Maria"]) --> N\n'
        + "  classDef hit fill:#ef6c00,stroke:#e65100,color:#fff\n"
        + f"  class {_scope} hit\n"
    )
    return


@app.cell(hide_code=True)
def _():
    q12 = [
        {
            "q": "What stops the rule's pass from resolving a person's note?",
            "options": [
                "The retire filter, which limits the retire to the rule's own alert types",
                "Notes are stored in a different table",
                "Nothing: a person must raise it again",
            ],
            "answer": "The retire filter, which limits the retire to the rule's own alert types",
            "why": "Without it the retire covers every row in scope. Untick the checkbox above to watch Maria's note get resolved.",
        },
        {
            "q": "A rule stops failing. What happens to its alert row?",
            "options": [
                "It is deleted",
                "It stays, with `resolved_at` set to the event's time",
                "It stays open until a person closes it",
            ],
            "answer": "It stays, with `resolved_at` set to the event's time",
            "why": "A soft delete keeps the record. The stamp is the event's time, so a replay stamps the same value.",
        },
        {
            "q": 'Who sends the "alert resolved" email?',
            "options": [
                "The executor, as it applies the retire",
                "Your code, from the notifications the run hands back",
                "Django signals",
            ],
            "answer": "Your code, from the notifications the run hands back",
            "why": "A notification is an external effect, and no executor applies one. Replay returns them in `ReplayResult.external`.",
        },
    ]
    quiz12 = make_quiz(q12)
    mo.vstack([mo.md("### Quiz 12"), quiz12])
    return q12, quiz12


@app.cell(hide_code=True)
def _(q12, quiz12):
    score12, _fb = mark(quiz12, q12)
    _fb
    return (score12,)


# ---------------------------------------------------------------- section 13


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 13. Consumers that keep their place

    Replay rebuilds a table from scratch. A **consumer** does the everyday job:
    each time it runs, it reads what is new since last time, applies it, and moves
    its **cursor** (section 2's bookmark, kept for you).

    Some events can't be applied. The consumer then writes a **failure record**
    saying which event failed, with a short reason *code* rather than a message.
    A message can carry someone's name or bank details into a log. Your own
    exceptions are filed under `unhandled`, with the exception's type beside the
    code. Only failures are recorded: below the cursor, no record means it worked.

    What to do on a failure has **no default**:

    * `"skip"` records it and carries on, which suits a live stream;
    * `"halt"` records it and stops there, which suits a rebuild, where one
      silently skipped event makes the whole result untrue.

    The second event below has a negative amount, which this consumer refuses.
    """)
    return


@app.cell(hide_code=True)
def _():
    s13_policy = mo.ui.radio(
        ["skip", "halt"], value="skip", label="on_error", inline=True
    )
    s13_new = mo.ui.checkbox(
        value=True, label="A new event arrives before the second run"
    )
    mo.hstack([s13_policy, s13_new], justify="start", gap=4)
    return s13_new, s13_policy


@app.cell
def _(s13_new, s13_policy):
    _store = stream_of(
        "payments",
        [{"id": 1, "amount": 10}, {"id": 2, "amount": -5}, {"id": 3, "amount": 7}],
    )
    _consumer = Consumer(
        _store,
        "payments",
        "totals",
        cursors=InMemoryConsumerCursorStore(),
        outcomes=InMemoryOutcomeStore(),
    )
    _applied = []

    def _apply(message):
        payment = json.loads(message.data)
        if payment["amount"] < 0:
            raise ValueError("negative payment")
        _applied.append(payment["id"])

    _runs = []
    for _run in (1, 2):
        if _run == 2 and s13_new.value:
            _store.append("payments", json.dumps({"id": 4, "amount": 1}).encode())
        _before = len(_applied)
        _done = _consumer.run(_apply, on_error=s13_policy.value)
        _runs.append(
            {
                "run": _run,
                "applied": ", ".join(str(i) for i in _applied[_before:]) or "-",
                "stopped early": _done.halted,
                "cursor": _done.cursor,
            }
        )
    _records = [
        {
            "event offset": o.offset,
            "reason": ", ".join(o.reasons),
            "details": json.dumps(o.params),
        }
        for o in _consumer.outcomes.latest("totals", "payments")
    ]
    mo.vstack([show(_runs), mo.md("**Failure records**"), show(_records, "*None.*")])
    return


@app.cell(hide_code=True)
def _(s13_new, s13_policy):
    _store = stream_of(
        "payments",
        [{"id": 1, "amount": 10}, {"id": 2, "amount": -5}, {"id": 3, "amount": 7}],
    )
    if s13_new.value:
        _store.append("payments", json.dumps({"id": 4, "amount": 1}).encode())
    _consumer = Consumer(
        _store,
        "payments",
        "totals",
        cursors=InMemoryConsumerCursorStore(),
        outcomes=InMemoryOutcomeStore(),
    )
    _applied = set()

    def _apply(message):
        payment = json.loads(message.data)
        if payment["amount"] < 0:
            raise ValueError("negative payment")
        _applied.add(message.offset)

    for _ in (1, 2):
        _done = _consumer.run(_apply, on_error=s13_policy.value)
    _failed = {o.offset for o in _consumer.outcomes.latest("totals", "payments")}
    _messages, _ = _store.read("payments")
    _rows = [
        {
            "event": f"payment {json.loads(m.data)['id']}",
            "state": "applied"
            if m.offset in _applied
            else "failure recorded"
            if m.offset in _failed
            else "not reached",
            "cursor": "▲ cursor" if m.offset == _done.cursor else "",
        }
        for m in _messages
    ]
    _base = alt.Chart(alt.Data(values=_rows)).encode(
        x=alt.X("event:N", sort=None, title=None)
    )
    (
        _base.mark_square(size=700).encode(
            color=alt.Color(
                "state:N",
                title=None,
                scale=alt.Scale(
                    domain=["applied", "failure recorded", "not reached"],
                    range=["#2e7d32", "#c62828", "#bdbdbd"],
                ),
                legend=alt.Legend(orient="top"),
            )
        )
        + _base.mark_text(dy=30, fontWeight="bold").encode(text="cursor:N")
    ).properties(width=420, height=100, title="After both runs")
    return


@app.cell(hide_code=True)
def _():
    q13 = [
        {
            "q": "Why does the consumer make you choose `skip` or `halt` every time?",
            "options": [
                "Each default would be quietly wrong for one of the two jobs",
                "Older Python versions need it",
                "So the cursor knows where to start",
            ],
            "answer": "Each default would be quietly wrong for one of the two jobs",
            "why": "A live stream should keep running past one bad event. A rebuild that skips one has produced a table that isn't what the log says.",
        },
        {
            "q": "There is no failure record for an event below the cursor. What does that mean?",
            "options": [
                "It was applied cleanly",
                "It might have failed",
                "It was never read",
            ],
            "answer": "It was applied cleanly",
            "why": "Recording is failures-only. The cursor is the record of success, so there is no third state.",
        },
        {
            "q": 'Your code raises `ValueError("bad amount for Maria")`. What goes in the failure record?',
            "options": [
                "The full message",
                "The code `unhandled`, with the type `ValueError` beside it",
                "Nothing: only rakaia's own errors are recorded",
            ],
            "answer": "The code `unhandled`, with the type `ValueError` beside it",
            "why": "Codes can be counted, and leaving the message out keeps field values, like a name, out of the record.",
        },
    ]
    quiz13 = make_quiz(q13)
    mo.vstack([mo.md("### Quiz 13"), quiz13])
    return q13, quiz13


@app.cell(hide_code=True)
def _(q13, quiz13):
    score13, _fb = mark(quiz13, q13)
    _fb
    return (score13,)


# ---------------------------------------------------------------- section 14


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    ## 14. Over the wire, and in Django

    Everything so far was Python calls. `rakaia` also ships a server that speaks
    the **Durable Streams protocol** over HTTP, with no dependencies, so a browser
    or another service can use streams too. Create with `PUT`, append with `POST`,
    read with `GET ?offset=…`. Every response says where to read next in a
    `Stream-Next-Offset` header. A stream can be **closed**, after which appends
    are refused. Live reads (long-poll and server-sent events) follow the same
    offsets.

    The cell below runs that server inside this notebook, with no network
    involved, and talks to it the way a client would.
    """)
    return


@app.cell(hide_code=True)
def _():
    s14_text = mo.ui.text(value="hello", label="Message to append")
    s14_text
    return (s14_text,)


@app.cell
async def _(s14_text):
    _app = create_app(StreamStore())
    _json = {"Content-Type": "application/json"}
    s14_log = []
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=_app), base_url="http://rakaia"
    ) as _client:

        async def _call(method, url, note, **kwargs):
            response = await _client.request(method, url, **kwargs)
            s14_log.append(
                {
                    "request": f"{method} {url}",
                    "what": note,
                    "status": response.status_code,
                    "Stream-Next-Offset": response.headers.get(
                        "stream-next-offset", ""
                    ),
                    "body": response.text,
                }
            )
            return response

        await _call("PUT", "/chat", "create the stream", headers=_json)
        _first = await _call(
            "POST", "/chat", "append", headers=_json, content=b'{"msg": "first"}'
        )
        await _call(
            "POST",
            "/chat",
            "append yours",
            headers=_json,
            content=json.dumps({"msg": s14_text.value}).encode(),
        )
        await _call("GET", "/chat?offset=-1", "read from the start")
        await _call(
            "GET",
            f"/chat?offset={_first.headers['stream-next-offset']}",
            "read after the first append",
        )
        await _call(
            "POST", "/chat", "close", headers={**_json, "Stream-Closed": "true"}
        )
        await _call(
            "POST",
            "/chat",
            "append after closing",
            headers=_json,
            content=b'{"msg": "late"}',
        )
    show(s14_log)
    return (s14_log,)


@app.cell(hide_code=True)
def _():
    mo.md(r"""
    **In Django**, you rarely append by hand. `@stream_model` on a model appends an
    event on every save and delete, to the streams you name:

    ```python
    from django_rakaia import stream_model

    @stream_model(
        stream_paths=lambda obj: [f"area:{obj.area_id}:projects"],
        to_dataclass=lambda obj: ProjectData(id=obj.id, name=obj.name, area_id=obj.area_id),
    )
    class Project(models.Model): ...
    ```

    Saves made with `raw=True`, such as `manage.py loaddata`, are ignored: fixture
    rows are history being restored, not new facts.

    Where the log lives is the `RAKAIA_STORE` setting: in memory, the database, or
    JSON-lines files. **Changing it does not move your log.** The app comes up on
    an empty store, every saved cursor still looks valid, and consumers resume into
    silence rather than failing. Moving a log is a copy, with `migrate_stream` or
    `migrate_all`.
    """)
    return


@app.cell(hide_code=True)
def _(s14_log):
    _lines = [
        "sequenceDiagram",
        "  participant C as client",
        "  participant S as rakaia server",
    ]
    for _row in s14_log:
        _req = _row["request"].replace("?", " ?")
        _lines.append(f"  C->>S: {_req}  ({_row['what']})")
        _back = f"{_row['status']}"
        if _row["Stream-Next-Offset"]:
            _back += f", next offset …{_row['Stream-Next-Offset'][-4:]}"
        _lines.append(f"  S-->>C: {_back}")
    mo.mermaid("\n".join(_lines))
    return


@app.cell(hide_code=True)
def _():
    q14 = [
        {
            "q": "A client appends to a stream that has been closed. What comes back?",
            "options": [
                "201, the stream reopens",
                "409, the append is refused",
                "204, silently ignored",
            ],
            "answer": "409, the append is refused",
            "why": "A closed stream takes no more data, as the last line of the exchange above shows.",
        },
        {
            "q": "You change `RAKAIA_STORE` from the database to files and restart. Where are your events?",
            "options": [
                "Copied into the files on startup",
                "Still in the database; the new store is empty",
                "Lost",
            ],
            "answer": "Still in the database; the new store is empty",
            "why": "Changing the setting moves nothing, and consumers resume into silence. Copy the log with `migrate_stream` or `migrate_all`.",
        },
        {
            "q": "`manage.py loaddata` saves 500 rows of a `@stream_model` model. How many events are appended?",
            "options": ["500", "0", "1"],
            "answer": "0",
            "why": "Raw saves are ignored. Fixture rows are restored history, and appending them would grow the stream on every restore.",
        },
    ]
    quiz14 = make_quiz(q14)
    mo.vstack([mo.md("### Quiz 14"), quiz14])
    return q14, quiz14


@app.cell(hide_code=True)
def _(q14, quiz14):
    score14, _fb = mark(quiz14, q14)
    _fb
    return (score14,)


# ---------------------------------------------------------------- total


@app.cell(hide_code=True)
def _(
    score1,
    score2,
    score3,
    score4,
    score5,
    score6,
    score7,
    score8,
    score9,
    score10,
    score11,
    score12,
    score13,
    score14,
):
    _scores = [
        score1,
        score2,
        score3,
        score4,
        score5,
        score6,
        score7,
        score8,
        score9,
        score10,
        score11,
        score12,
        score13,
        score14,
    ]
    _total = sum(_scores)
    _chart = (
        alt.Chart(
            alt.Data(
                values=[
                    {"section": i + 1, "score": int(s)} for i, s in enumerate(_scores)
                ]
            )
        )
        .mark_bar()
        .encode(
            x=alt.X("section:O", title="Section"),
            y=alt.Y("score:Q", title="Right answers", scale=alt.Scale(domain=[0, 3])),
        )
        .properties(height=180)
    )
    mo.vstack(
        [
            mo.md(f"## Your total: **{_total} / {3 * len(_scores)}**"),
            mo.ui.altair_chart(_chart),
            mo.md(
                "Where next: [the tutorial](../docs/tutorial.md) builds a real Django "
                "table and rebuilds it after a bug, and the "
                "[glossary](../docs/glossary.md) has every term used here."
            ),
        ]
    )
    return


if __name__ == "__main__":
    app.run()
