# Triage Labels

The skills speak in terms of five canonical triage roles. This file maps those roles to the actual label strings used in this repo's issue tracker.

| Label in mattpocock/skills | Label in our tracker | Meaning                                  |
| -------------------------- | -------------------- | ---------------------------------------- |
| `needs-triage`             | `needs-triage`       | Maintainer needs to evaluate this issue  |
| `needs-info`               | `needs-info`         | Waiting on reporter for more information |
| `ready-for-agent`          | `ready-for-agent`    | Fully specified, ready for an AFK agent  |
| `ready-for-human`          | `ready-for-human`    | Requires human implementation            |
| `wontfix`                  | `wontfix`            | Will not be actioned                     |

When a skill mentions a role (e.g. "apply the AFK-ready triage label"), use the corresponding label string from this table.

Edit the right-hand column to match whatever vocabulary you actually use.

## Provenance

`wontfix` is a GitHub default that already existed on this repo. The other four
labels were created as part of agent-skills setup and exist solely to drive the
triage state machine.

The repo's other labels (`bug`, `enhancement`, `documentation`, `question`,
`duplicate`, `invalid`, `help wanted`, `good first issue`, `deferred`) are
**not** triage states. They classify an issue's kind, not its position in the
workflow, and an issue normally carries one of each. In particular `question`
means "further information is requested" of the *maintainers*, which is not the
same as `needs-info` ("waiting on the reporter") — don't substitute one for the
other.
