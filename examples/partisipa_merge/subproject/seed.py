"""Sample events for the multi-stream merge spike.

Every event carries a ``ts`` — the envelope timestamp from the pghistory spike —
which is the merge order key. The list is authored in the **canonical merged
order** `(ts, stream_path, offset)`, so the single-stream baseline (these events
in one stream) and the three-stream merge must reconstruct the same sequence.

`STREAMS` maps each form type to its own stream, mirroring Partisipa's separate
SF / TF / FF pipelines. Note the deliberate **cross-stream tie** at
``12:30`` — a FINANCE and a MEETING event share a timestamp, so the tiebreak
(`stream_path`: ``forms/finance`` < ``forms/meetings``) decides their order.
"""

STREAMS = {
    "PROGRESS": "forms/progress",
    "MEETING": "forms/meetings",
    "FINANCE": "forms/finance",
}
SINGLE_STREAM = "forms/all"
THREE_STREAMS = ["forms/progress", "forms/meetings", "forms/finance"]

# The cross-stream tie pair (same ts, different streams); finance sorts first.
TIE_KEYS = ("f-mb-1", "m-mb-1")

INITIAL_EVENTS: list[dict] = [
    {
        "form_type": "PROGRESS",
        "key": "p-fb-water",
        "ts": "2026-03-01T09:00:00Z",
        "suku": "Fatuberliu",
        "output": "WATER",
        "percent": 100,
    },
    {
        "form_type": "MEETING",
        "key": "m-fb-1",
        "ts": "2026-03-01T09:30:00Z",
        "suku": "Fatuberliu",
        "meeting_id": "M1",
        "verified": True,
    },
    {
        "form_type": "MEETING",
        "key": "m-fb-2",
        "ts": "2026-03-01T10:00:00Z",
        "suku": "Fatuberliu",
        "meeting_id": "M2",
        "verified": True,
    },
    {
        "form_type": "FINANCE",
        "key": "f-fb-1",
        "ts": "2026-03-01T10:30:00Z",
        "suku": "Fatuberliu",
        "account": "operational",
        "delta": "500.00",
    },
    {
        "form_type": "FINANCE",
        "key": "f-fb-2",
        "ts": "2026-03-01T11:00:00Z",
        "suku": "Fatuberliu",
        "account": "operational",
        "delta": "-200.00",
    },
    {
        "form_type": "FINANCE",
        "key": "f-fb-3",
        "ts": "2026-03-01T11:30:00Z",
        "suku": "Fatuberliu",
        "account": "infrastructure",
        "delta": "1000.00",
    },
    {
        "form_type": "PROGRESS",
        "key": "p-mb-road",
        "ts": "2026-03-01T12:00:00Z",
        "suku": "Maubara",
        "output": "ROAD",
        "percent": 60,
    },
    # ---- cross-stream tie at 12:30: finance (f-mb-1) sorts before meeting ----
    # Both carry the same `slot`, so whichever is LATER in the merged order wins
    # the Claim row — making the tie's resolution observable in the projection.
    {
        "form_type": "FINANCE",
        "key": "f-mb-1",
        "ts": "2026-03-01T12:30:00Z",
        "suku": "Maubara",
        "account": "operational",
        "delta": "100.00",
        "slot": "mb-claim",
    },
    {
        "form_type": "MEETING",
        "key": "m-mb-1",
        "ts": "2026-03-01T12:30:00Z",
        "suku": "Maubara",
        "meeting_id": "M1",
        "verified": True,
        "slot": "mb-claim",
    },
    {
        "form_type": "FINANCE",
        "key": "f-mb-2",
        "ts": "2026-03-01T13:00:00Z",
        "suku": "Maubara",
        "account": "operational",
        "delta": "-150.00",
    },
    {
        "form_type": "FINANCE",
        "key": "f-mb-3",
        "ts": "2026-03-01T13:30:00Z",
        "suku": "Maubara",
        "account": "infrastructure",
        "delta": "200.00",
    },
]

# Maubara's three failures, fixed — each fix arrives on its own pipeline.
HEAL_EVENTS: list[dict] = [
    {
        "form_type": "PROGRESS",
        "key": "p-mb-road",
        "ts": "2026-03-01T14:00:00Z",
        "suku": "Maubara",
        "output": "ROAD",
        "percent": 100,
    },
    {
        "form_type": "MEETING",
        "key": "m-mb-2",
        "ts": "2026-03-01T14:30:00Z",
        "suku": "Maubara",
        "meeting_id": "M2",
        "verified": True,
    },
    {
        "form_type": "FINANCE",
        "key": "f-mb-4",
        "ts": "2026-03-01T15:00:00Z",
        "suku": "Maubara",
        "account": "operational",
        "delta": "100.00",
    },
]

EXPECTED_INITIAL_READINESS = {
    "Fatuberliu": (True, []),
    "Maubara": (
        False,
        [
            "incomplete_projects",
            "insufficient_meetings",
            "negative_operational_balance",
        ],
    ),
}
