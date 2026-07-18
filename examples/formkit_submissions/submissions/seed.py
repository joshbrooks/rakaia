"""Sample FormKit submissions, appended to the `submissions` stream.

Each dict is one submission event, shaped like a FormKit-Ninja `Submission`
payload: a `form_type`, a stable `submission_id` UUID (FormKit assigns these
during its flatten stage), and a nested `fields` object whose `activities` key
is a *repeater*.

Position in this list becomes the stream sequence number (seq), which selects
the handler *version* at replay time:

    seq 0, 1  -> pre-policy  -> visit_summary v1  (COMPLETE at >= 100%)
    seq 2, 3  -> post-policy -> visit_summary v2  (COMPLETE at >= 90%)

Independently, each event declares a `schema_version`:

    schema_version 1  uses the legacy `pct` repeater key
    schema_version 2  uses the current `progress_pct` key

The v1->v2 upcaster (see upcasters.py) renames `pct` -> `progress_pct` on read,
so handlers only ever see `progress_pct`. Note seq 3 is *post-policy* but still
*schema v1* — it exercises the upcaster and the v2 status rule at once.
"""

# The completion-tolerance policy changed at this sequence boundary. Visits
# recorded before it keep the strict 100% rule; from here on, 90% counts as
# COMPLETE. Kept in sync with effective_from/effective_to in handlers.py.
POLICY_CHANGE_SEQ = 2

SAMPLE_SUBMISSIONS: list[dict] = [
    # ---- pre-policy (seq 0..1, visit_summary v1, COMPLETE needs 100%) ----
    {
        "schema_version": 1,  # legacy `pct` key -> upcaster fires
        "form_type": "monitoring_visit",
        "submission_id": "11111111-1111-1111-1111-111111111111",
        "fields": {
            "project_code": "WS-014",
            "suku": "Fatuberliu",
            "monitor": "amaral",
            "visit_date": "2025-02-03",
            "activities": [
                {"name": "Spring intake", "budget": "4000.00", "pct": 100},
                {"name": "Reservoir", "budget": "6000.00", "pct": 100},
            ],
        },
    },
    {
        "schema_version": 1,
        "form_type": "monitoring_visit",
        "submission_id": "22222222-2222-2222-2222-222222222222",
        "fields": {
            "project_code": "RD-227",
            "suku": "Maubara",
            "monitor": "guterres",
            "visit_date": "2025-02-11",
            "activities": [
                # 95% budget-weighted -> COMPLETE under v2, but this is pre-policy
                # so v1's strict rule keeps it IN_PROGRESS. The time-correctness
                # payoff: re-running today never silently promotes it.
                {"name": "Culverts", "budget": "5000.00", "pct": 90},
                {"name": "Surfacing", "budget": "5000.00", "pct": 100},
            ],
        },
    },
    # ---- post-policy (seq 2..3, visit_summary v2, COMPLETE needs 90%) ----
    {
        "schema_version": 2,  # current `progress_pct` key
        "form_type": "monitoring_visit",
        "submission_id": "33333333-3333-3333-3333-333333333333",
        "fields": {
            "project_code": "WS-031",
            "suku": "Liquica",
            "monitor": "amaral",
            "visit_date": "2025-05-20",
            "activities": [
                {"name": "Spring intake", "budget": "3000.00", "progress_pct": 100},
                {"name": "Tank", "budget": "7000.00", "progress_pct": 88},
            ],
        },
    },
    {
        "schema_version": 1,  # post-policy *and* legacy schema -> both fire
        "form_type": "monitoring_visit",
        "submission_id": "44444444-4444-4444-4444-444444444444",
        "fields": {
            "project_code": "IR-108",
            "suku": "Bobonaro",
            "monitor": "soares",
            "visit_date": "2025-06-02",
            "activities": [
                {"name": "Canal lining", "budget": "8000.00", "pct": 95},
                {"name": "Gate", "budget": "2000.00", "pct": 60},
            ],
        },
    },
]
