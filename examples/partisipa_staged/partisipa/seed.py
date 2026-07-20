"""Sample submission events for the staged-replay spike.

The ordering is the whole point: each ``SF_1_2`` appears in the stream **before**
the ``TF_6_1_1`` that defines the Project it belongs to — the late-arrival case
that breaks single-pass processing in Partisipa and drives its backfill tasks.

Each event carries a ``form_type`` discriminator, a stable ``key`` (the
submission UUID), and the natural business keys ``suku`` + ``output`` that link
an SF form to its project.
"""

SAMPLE_SUBMISSIONS: list[dict] = [
    # ---- SF_1_2 forms arrive first; their projects don't exist yet ----
    {
        "schema_version": 1,
        "form_type": "SF_1_2",
        "key": "sf-fatuberliu-water",
        "suku": "Fatuberliu",
        "output": "WATER",
        "cost": "1200.00",
    },
    {
        "schema_version": 1,
        "form_type": "SF_1_2",
        "key": "sf-maubara-road",
        "suku": "Maubara",
        "output": "ROAD",
        "cost": "3400.00",
    },
    # ---- TF_6_1_1 forms arrive later; they define the projects ----
    {
        "schema_version": 1,
        "form_type": "TF_6_1_1",
        "key": "tf-fatuberliu-water",
        "suku": "Fatuberliu",
        "output": "WATER",
        "project_name": "Spring intake WS-014",
    },
    {
        "schema_version": 1,
        "form_type": "TF_6_1_1",
        "key": "tf-maubara-road",
        "suku": "Maubara",
        "output": "ROAD",
        "project_name": "Culverts RD-227",
    },
]

# A later correction used by the self-heal demo: an SF_1_2 whose project has not
# been submitted yet, followed (later) by the TF_6_1_1 that defines it.
LATE_SF = {
    "schema_version": 1,
    "form_type": "SF_1_2",
    "key": "sf-liquica-tank",
    "suku": "Liquica",
    "output": "TANK",
    "cost": "900.00",
}
LATE_TF = {
    "schema_version": 1,
    "form_type": "TF_6_1_1",
    "key": "tf-liquica-tank",
    "suku": "Liquica",
    "output": "TANK",
    "project_name": "Storage tank WS-031",
}
