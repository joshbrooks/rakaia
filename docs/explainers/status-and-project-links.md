# Status and project links

Two steps here connect a report to the rest of the picture: its review status,
and the project it belongs to.

## Step 2: status

When a coordinator approves a report, that decision is a form of its own, and it
goes into the log like any other. The status step turns it into the report's
current status. Until a decision arrives, a report counts as *submitted*.

## Step 3: project links

A project does not exist until its registration form arrives: the registration
is what creates it. A progress report names its suku and what it is building,
such as "Fatuberliu, water", but not its project. So someone, or something, has
to decide which project a report belongs to.

Here a save decides, by finding the registration for the report's suku and
output. It then writes the decision into the log as an event of its own, with the
reason: *the water report is assigned to the water registration, because it
matched suku Fatuberliu and output WATER when its registration arrived*. The link step turns that event into
the report's link.

The hard case is a report that arrives **before** its project's registration. In
the story, the water report is the first form of the month, and the water
registration does not arrive until save 4. At save 1 nothing matches, so nothing
is assigned, and the month cannot close. At save 4, the save that brings the
registration also looks for reports waiting for it, and records the assignment
then.

## Why it is an event

A rebuild does not decide anything again. It replays the assignments the saves
recorded. So if the way projects are matched changes next year, a rebuild still
gives every old report the project it was given at the time, and the log says
why. Anyone reading a report's project later can see when it was assigned and
for what reason, instead of having to work out which rule was in force.

## Try it

```sh
just pipeline-demo --until project_links
```

Watch for the `recorded:` lines at saves 3 and 4. The water report shows
`project (none)` after saves 1 to 3, then `project Spring intake` from save 4.

## Appendix

```python
def assignments(form, reader):
    if form["form_type"] == "PROGRESS":
        waiting = [] if reader.get(LINK, report_key=form["key"]) else [form["key"]]
        registration = reader.get(PROJECT, suku=form["suku"], output=form["output"])
    elif form["form_type"] == "PROJECT":
        waiting = [
            r.key
            for r in reader.filter(REPORT, suku=form["suku"], output=form["output"])
            if reader.get(LINK, report_key=r.key) is None
        ]
        registration = reader.get(PROJECT, registration=form["key"])
    ...
    return [
        {
            "form_type": "ASSIGNED",
            "report": key,
            "registration": registration.registration,
            "because": because,
        }
        for key in waiting
    ]
```

- The step has two parts. Deciding (`assignments`) runs only on a save: after
  the form is saved, `save_form` appends each event it returns and replays it.
  Applying (`project_link`, a stage-0 handler on `ASSIGNED` events) runs on both
  routes and writes a `ProjectLink` row with the registration and the reason.
- On the step list, the deciding part carries
  `Skip("not_needed", "a rebuild replays the assignments the saves recorded; it decides nothing")`.
  `Step` refuses a step that decides on a save without saying why a rebuild does not.
- A report that already has a project keeps it; the example never moves an
  assignment. So a report resubmitted with a different suku or output keeps its
  first project, and its budget is still checked against that project. Moving
  it would take a new assignment, which the example does not make. A project is keyed by its registration form's key, and so is the
  link, never a project number or a database id. The keys are made up to be
  readable, such as `fatuberliu-water-registration`.
- `test_a_rebuild_keeps_an_assignment_the_rule_would_not_make` records an
  assignment by hand, one the matching rule would never choose, and checks that
  a rebuild reproduces it.
  `test_the_save_that_brings_a_late_registration_assigns_the_waiting_report`
  checks the late case and the recorded reason.
- Status is a stage-0 handler that upserts one `ReportStatus` row per report,
  keyed by the report's key.

### How Partisipa compares

Read on partisipa-import `develop` (bdaf1a712) on 2026-11-13.

- A save decides each automatic link once (`ida_forms/streams/produce.py`,
  `bind_project_edge` and `resolve_auto_target`). It reads the form's own
  project field first, then matches a registration (TF 6.1.1) on suku and
  output, or on activity when there is no output.
- It records the link as an event on a `relations/project` stream naming that
  registration. The event names the form and the registration, but not why that
  registration was chosen (`ida_forms/streams/relations.py`, `project_edge`).
- When nothing matches, the save leaves the form unlinked.
- The pass that decides links again, `rebind_auto_project_links`, works only on
  links that already exist. It runs after a registration is rejected or deleted
  (`ida_forms/derivations.py`, `rebind_links_to_project`), and to move SF 2.3
  priorities onto their own aldeia's project.
