"""The source of truth: a sequence of submission *saves*.

Each save is one ``Submission.save()`` in the real system — a full post-change
snapshot plus who saved it and when. Both the "today" pghistory audit table and
the rakaia stream are built from this same list, so any divergence between them
is a real divergence, not a seeding artefact.

The story deliberately includes the failure mode that `repair_blank_save_dataloss`
exists to fix: a truncating "blank save" (here from a ``sync-bot`` actor) wipes
most of a submission's fields, and a human later restores it. The pre-truncation
snapshot is still in the history — which is exactly what recovery relies on.
"""

TRUNCATED_KEY = "sub-water-01"

SAVES: list[dict] = [
    # sub-water-01 is created, then edited to add cost + revise beneficiaries.
    {
        "key": "sub-water-01",
        "op": "create",
        "actor": "aldina@pnds.tl",
        "ts": "2026-03-01T09:00:00Z",
        "fields": {"suku": "Fatuberliu", "output": "WATER", "beneficiaries": 120},
    },
    # A second submission, created by a different officer.
    {
        "key": "sub-road-02",
        "op": "create",
        "actor": "mateus@pnds.tl",
        "ts": "2026-03-01T10:15:00Z",
        "fields": {"suku": "Maubara", "output": "ROAD", "beneficiaries": 80},
    },
    # sub-water-01 edited: beneficiaries revised, cost added (4 keys — the peak).
    {
        "key": "sub-water-01",
        "op": "update",
        "actor": "aldina@pnds.tl",
        "ts": "2026-03-05T11:30:00Z",
        "fields": {
            "suku": "Fatuberliu",
            "output": "WATER",
            "beneficiaries": 135,
            "cost": "1200.00",
        },
    },
    # THE BUG: a blank/partial save truncates sub-water-01 to a single field.
    {
        "key": "sub-water-01",
        "op": "update",
        "actor": "sync-bot@pnds.tl",
        "ts": "2026-03-06T02:00:00Z",
        "fields": {"suku": "Fatuberliu"},
    },
    # A human notices the dataloss and restores the full record.
    {
        "key": "sub-water-01",
        "op": "update",
        "actor": "aldina@pnds.tl",
        "ts": "2026-03-06T09:45:00Z",
        "fields": {
            "suku": "Fatuberliu",
            "output": "WATER",
            "beneficiaries": 135,
            "cost": "1200.00",
        },
    },
    # sub-road-02 is deleted — the row goes away but its history must remain.
    {
        "key": "sub-road-02",
        "op": "delete",
        "actor": "mateus@pnds.tl",
        "ts": "2026-03-07T08:00:00Z",
        "fields": {"suku": "Maubara", "output": "ROAD", "beneficiaries": 80},
    },
]
