"""Sample events — deliberately **out of order**.

The two tasks for project ``P-100`` arrive *before* the ``PROJECT`` event that
creates it. Staged replay links them anyway; a naive single-pass projection
would leave them orphaned.
"""

from __future__ import annotations

SAMPLE_EVENTS: list[dict] = [
    # Tasks arrive first, referencing a project that does not exist yet.
    {
        "form_type": "TASK",
        "task_id": "T-1",
        "title": "Survey the site",
        "project_code": "P-100",
    },
    {
        "form_type": "TASK",
        "task_id": "T-2",
        "title": "Draft the budget",
        "project_code": "P-100",
    },
    # ...then the project both tasks belong to.
    {"form_type": "PROJECT", "code": "P-100", "name": "Water supply"},
    # A second project, this time in order.
    {"form_type": "PROJECT", "code": "P-200", "name": "Road repair"},
    {
        "form_type": "TASK",
        "task_id": "T-3",
        "title": "Order gravel",
        "project_code": "P-200",
    },
]
