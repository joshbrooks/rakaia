"""The forms this demo submits, and the reference data they are checked against.

One suku is registered at the start and one is not, and one reporting period is
closed. That is what makes three of the five events take three different paths
through the loop without any of them being contrived: a row out of range is
refused before the log, an event for the unregistered suku fails to apply, and
one for the closed period is deliberately skipped.

`LATE_FORMS` arrive after the first runs, for the contrast between halting below
a failure and skipping past it.
"""

REGISTERED_SUKUS = ["Fatuberliu"]
"""What the administrative import has loaded so far. Maubara arrives later."""

PERIODS = {"2025-12": True, "2026-01": False, "2026-02": False}
"""Reporting period → closed."""

PROGRESS_FORMS: list[dict] = [
    {
        "key": "prog-fatuberliu-2026-01",
        "form_type": "PROGRESS",
        "suku": "Fatuberliu",
        "period": "2026-01",
        "rows": [
            {"output": "WATER", "percent": 40},
            # Refused: a percentage cannot be 140. The siblings are appended.
            {"output": "ROAD", "percent": 140},
            {"output": "SANITATION", "percent": 60},
        ],
    },
    {
        "key": "prog-maubara-2026-01",
        "form_type": "PROGRESS",
        "suku": "Maubara",
        "period": "2026-01",
        # Acceptable to the gate — the row is fine, the village is simply not
        # registered yet, which the gate has no business knowing.
        "rows": [{"output": "ROAD", "percent": 30}],
    },
    {
        "key": "prog-fatuberliu-2025-12",
        "form_type": "PROGRESS",
        "suku": "Fatuberliu",
        "period": "2025-12",
        "rows": [{"output": "WATER", "percent": 80}],
    },
    {
        "key": "prog-fatuberliu-2026-02",
        "form_type": "PROGRESS",
        "suku": "Fatuberliu",
        "period": "2026-02",
        "rows": [{"output": "SANITATION", "percent": 75}],
    },
]

LATE_FORMS: list[dict] = [
    {
        "key": "prog-bobonaro-2026-02",
        "form_type": "PROGRESS",
        "suku": "Bobonaro",
        "period": "2026-02",
        "rows": [{"output": "WATER", "percent": 20}],
    },
    {
        "key": "prog-fatuberliu-2026-02b",
        "form_type": "PROGRESS",
        "suku": "Fatuberliu",
        "period": "2026-02",
        "rows": [{"output": "ROAD", "percent": 10}],
    },
]
