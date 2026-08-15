# Issue tracker: GitHub

Issues and PRDs for this repo live as GitHub issues. Use the `gh` CLI for all operations.

## Conventions

- **Create an issue**: `gh issue create --title "..." --body "..."`. Use a heredoc for multi-line bodies.
- **Read an issue**: `gh issue view <number> --comments`, filtering comments by `jq` and also fetching labels.
- **List issues**: `gh issue list --state open --json number,title,body,labels,comments --jq '[.[] | {number, title, body, labels: [.labels[].name], comments: [.comments[].body]}]'` with appropriate `--label` and `--state` filters.
- **Comment on an issue**: `gh issue comment <number> --body "..."`
- **Apply / remove labels**: `gh issue edit <number> --add-label "..."` / `--remove-label "..."`
- **Close**: `gh issue close <number> --comment "..."`

Infer the repo from `git remote -v` — `gh` does this automatically when run inside a clone.

## When a skill says "publish to the issue tracker"

Create a GitHub issue.

## When a skill says "fetch the relevant ticket"

Run `gh issue view <number> --comments`.

## Writing issues and PRs

Applies to both issue bodies and pull request descriptions.

- **Title**: short and jargon-free. It should tell a reader who doesn't know the
  internals what changed or what's wrong. Prefer "bulk appends never reached live
  subscribers" over "fan-out cursor advance skips `_notify` on batch path".
- **Body**: at most two short paragraphs, plain language, no jargon. Say what the
  problem or change is and why it matters. Don't paste stack traces, diffs, file
  inventories, or design rationale into the body.
- **Detail goes in a comment.** If technical content is genuinely needed —
  reproduction steps, a trace, benchmark numbers, an implementation sketch —
  append it as a follow-up comment (`gh issue comment` / `gh pr comment`), not in
  the body. This keeps the body readable to someone skimming a list.

Length is a constraint, not a target: if one paragraph says it, write one.

## Repo specifics

The remote is `git@github.com:joshbrooks/rakaia.git`; `gh` infers it automatically
inside any clone or worktree of this repo.

This repo also uses a `deferred` label — "intentionally deferred; action only when
its un-defer trigger fires". It is orthogonal to the triage state machine: an issue
can be `deferred` and still carry a triage label. Don't strip it during triage.
