"""The forms the demo saves, in the order they arrive.

One suku, one month, two projects. The order is chosen to show the three things
that go wrong in practice: a report that arrives before its project's
registration, a report resubmitted with a line taken out, and a month that only
becomes ready once every report has been approved.
"""

from __future__ import annotations

from typing import Any

STREAM = "forms/fatuberliu"
"""Every form for the suku goes into one stream, in the order it was saved."""

WATER = "fatuberliu-water-2026-03"
ROAD = "fatuberliu-road-2026-03"
WATER_REGISTRATION = "fatuberliu-water-registration"
ROAD_REGISTRATION = "fatuberliu-road-registration"

FORMS: list[tuple[str, dict[str, Any]]] = [
    (
        "the water report arrives before its project's registration",
        {
            "form_type": "PROGRESS",
            "key": WATER,
            "suku": "Fatuberliu",
            "output": "WATER",
            "period": "2026-03",
            "lines": [
                {"item": "pipes", "cost": 1200},
                {"item": "labour", "cost": 800},
            ],
        },
    ),
    (
        "the road project's registration arrives",
        {
            "form_type": "PROJECT",
            "key": ROAD_REGISTRATION,
            "suku": "Fatuberliu",
            "output": "ROAD",
            "name": "Culverts",
            "budget": 2000,
        },
    ),
    (
        "the road report arrives, over budget",
        {
            "form_type": "PROGRESS",
            "key": ROAD,
            "suku": "Fatuberliu",
            "output": "ROAD",
            "period": "2026-03",
            "lines": [
                {"item": "gravel", "cost": 900},
                {"item": "labour", "cost": 700},
                {"item": "extra culvert", "cost": 600},
            ],
        },
    ),
    (
        "the water project's registration arrives, late",
        {
            "form_type": "PROJECT",
            "key": WATER_REGISTRATION,
            "suku": "Fatuberliu",
            "output": "WATER",
            "name": "Spring intake",
            "budget": 2500,
        },
    ),
    (
        "the road report is resubmitted without the extra culvert",
        {
            "form_type": "PROGRESS",
            "key": ROAD,
            "suku": "Fatuberliu",
            "output": "ROAD",
            "period": "2026-03",
            "lines": [
                {"item": "gravel", "cost": 900},
                {"item": "labour", "cost": 700},
            ],
        },
    ),
    (
        "the water report is approved",
        {"form_type": "STATUS", "report": WATER, "status": "approved"},
    ),
    (
        "the road report is approved",
        {"form_type": "STATUS", "report": ROAD, "status": "approved"},
    ),
]
