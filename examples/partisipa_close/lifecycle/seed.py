"""Sample events for the close-precondition state machine.

Two sukus, each ending with a POM_1 close request:

* **Fatuberliu** satisfies every precondition — project 100 %, two verified
  meetings, both balances positive — so its close is ACCEPTED.
* **Maubara** fails three ways at first — project 60 %, only one verified
  meeting, operational balance net-negative — so its close is REJECTED with
  exactly those reasons. `HEAL_EVENTS` later fix all three.

The POM_1 events stay in the stream throughout; re-replaying re-evaluates them
against the healed state, so the guarded transition self-heals with no bespoke
re-evaluation task.
"""

SAMPLE_EVENTS: list[dict] = [
    # ---- Fatuberliu: will pass ----
    {"schema_version": 1, "form_type": "PROGRESS", "key": "p-fb-water",
     "suku": "Fatuberliu", "output": "WATER", "percent": 100},
    {"schema_version": 1, "form_type": "MEETING", "key": "m-fb-1",
     "suku": "Fatuberliu", "meeting_id": "M1", "verified": True},
    {"schema_version": 1, "form_type": "MEETING", "key": "m-fb-2",
     "suku": "Fatuberliu", "meeting_id": "M2", "verified": True},
    {"schema_version": 1, "form_type": "FINANCE", "key": "f-fb-1",
     "suku": "Fatuberliu", "account": "operational", "delta": "500.00"},
    {"schema_version": 1, "form_type": "FINANCE", "key": "f-fb-2",
     "suku": "Fatuberliu", "account": "operational", "delta": "-200.00"},
    {"schema_version": 1, "form_type": "FINANCE", "key": "f-fb-3",
     "suku": "Fatuberliu", "account": "infrastructure", "delta": "1000.00"},

    # ---- Maubara: will fail all three preconditions ----
    {"schema_version": 1, "form_type": "PROGRESS", "key": "p-mb-road",
     "suku": "Maubara", "output": "ROAD", "percent": 60},
    {"schema_version": 1, "form_type": "MEETING", "key": "m-mb-1",
     "suku": "Maubara", "meeting_id": "M1", "verified": True},
    {"schema_version": 1, "form_type": "FINANCE", "key": "f-mb-1",
     "suku": "Maubara", "account": "operational", "delta": "100.00"},
    {"schema_version": 1, "form_type": "FINANCE", "key": "f-mb-2",
     "suku": "Maubara", "account": "operational", "delta": "-150.00"},
    {"schema_version": 1, "form_type": "FINANCE", "key": "f-mb-3",
     "suku": "Maubara", "account": "infrastructure", "delta": "200.00"},

    # ---- both cycles request close ----
    {"schema_version": 1, "form_type": "POM_1", "key": "close-fb",
     "suku": "Fatuberliu"},
    {"schema_version": 1, "form_type": "POM_1", "key": "close-mb",
     "suku": "Maubara"},
]

# Maubara's three failures, fixed: project finished, a second verified meeting,
# and an operational top-up that lifts the balance back to non-negative.
HEAL_EVENTS: list[dict] = [
    {"schema_version": 1, "form_type": "PROGRESS", "key": "p-mb-road",
     "suku": "Maubara", "output": "ROAD", "percent": 100},
    {"schema_version": 1, "form_type": "MEETING", "key": "m-mb-2",
     "suku": "Maubara", "meeting_id": "M2", "verified": True},
    {"schema_version": 1, "form_type": "FINANCE", "key": "f-mb-4",
     "suku": "Maubara", "account": "operational", "delta": "100.00"},
]

EXPECTED_INITIAL = {
    "Fatuberliu": ("ACCEPTED", []),
    "Maubara": (
        "REJECTED",
        ["incomplete_projects", "insufficient_meetings",
         "negative_operational_balance"],
    ),
}
