# CLAUDE.md

## Development

- **Running tests:** the test deps live in extras. In a fresh checkout **or a new
  git worktree**, run `uv sync --extra dev --extra django` before `pytest` —
  plain `uv run` bootstraps only the zero-dependency core package, so tests fail
  with `No module named pytest` until the extras are synced.
- CI (`.github/workflows/ci.yml`) runs `ruff check`, `ruff format --check`,
  `pyright src/`, `pytest`, and `zensical build`; run those before pushing.
- **Postgres leg.** The default `pytest` run is SQLite, where every
  `select_for_update()` in `django_rakaia` is a no-op — Django emits
  `FOR UPDATE` only when the backend reports `has_select_for_update`, and the
  SQLite backend does not. A second CI job (`test-postgres`) runs the same
  suite against Postgres 16. Locally: `just test-pg` (starts a podman
  container and sets `RAKAIA_TEST_DB=postgres`), or set `RAKAIA_TEST_DB` and
  the `PG*` variables yourself. Needs `uv sync --extra postgres`.
  Anything that asserts locking or concurrency must also be marked
  `django_db(transaction=True)`; a plain `django_db` test runs inside a
  transaction pytest-django rolls back, so a lock taken outside the code's own
  `atomic()` still looks fine and a second connection can never see the rows.
- **A plain `django_db` marker also hides a missing `using=` on
  `transaction.atomic()`, and that failure is worse.** `transaction.atomic()`
  with no argument binds to `default`, so code pointed at another alias writes in
  autocommit while opening an empty transaction somewhere else — on Postgres
  `select_for_update` checks the *target* connection and raises, and on SQLite a
  failed write stays committed. Under `django_db(databases=[...])` pytest-django
  has already opened a transaction on **every declared alias**, which silently
  supplies both the missing `atomic()` and the absent `BEGIN`, so the whole
  feature tests green while being unusable in production. That is how #180
  shipped a broken alias past a full suite.
  **Anything alias-aware wants `django_db(transaction=True, databases=[...])`**,
  and is worth checking per site: strip `using=` from one `atomic()` at a time and
  confirm the test named for it goes red. Aggregate mutation only proves that
  *something* was covered.
- **Apply `requires_row_locks` per test, not per class.** `test_locking.py` skips
  wholesale on SQLite, so a class-level marker also skips failures that *are*
  visible there — a non-rollback is, a `select_for_update` raise is not. Mark the
  cases whose failure mode is the lock; leave the rest to run on both legs.
- **Row locking is covered deliberately, not incidentally.** There are three
  `select_for_update()` sites (`models.py` offset watermark, `django_store.py`
  stream row, `effect_executor.py` retire capture). Around 290 tests touch one,
  but nearly all reach it in passing — an append allocates an offset, and
  allocating locks. The cover is `test_concurrent_appends.py` and
  `test_locking.py`; converting the incidental ~227 to `transaction=True` was
  considered and rejected (#148), because it buys nothing those files do not
  and makes every run pay truncation teardown. **A new test that exercises a
  lock belongs in one of those two files, marked `transaction=True`, and must
  be shown to fail with the lock removed** — two earlier attempts passed with
  and without it, which is the failure mode these tests exist to avoid.
- **pyright is a hard gate** and `src/` is expected at zero errors. Run
  `just typecheck`, which pins `PYRIGHT_PYTHON_FORCE_VERSION` to the version in
  the lockfile. Do **not** set it to `latest`, which this file used to advise:
  pyright nags that a newer release exists, and taking its suggestion typechecks
  against a different pyright than CI — the nag is cosmetic, the divergence is
  not. The recipe silences the nag; a bare `uv run pyright` still prints it.
  Upgrading pyright is its own change: bump the pin in `pyproject.toml` and the
  `justfile` together. Django's synthesised attributes are declared explicitly (see the
  `if TYPE_CHECKING` blocks in `django_rakaia/models.py`) rather than waved
  through with ignores.
- **Lint and format take no path arguments.** `[tool.ruff]` in `pyproject.toml`
  decides what is checked, so `just lint`, CI and your editor all see the same
  tree. Passing paths reintroduces the split that let `just check` pass on a diff
  CI rejected. `just lint` / `just fmt`; the rule set and every deliberate
  `ignore` carries its reason inline in `pyproject.toml`.
- **`docs/api-reference.md` is gated in CI** (`just api-reference-check`), not
  only by `just check`. If you add or remove an exported name, run
  `just api-reference` and commit the result — and rebase before merging, since
  the count line at the bottom is a single line two branches will both rewrite.
  The check compares against the last commit, so run it *after* committing;
  before that, a correctly regenerated file still reads as out of date.
- **Adding a public name touches five places.** `_EXPORTS` in the package
  `__init__` (and, for `rakaia`, its `if TYPE_CHECKING` import block), the
  expected set in `tests/test_rakaia/test_public_api.py`, `GROUPS` in
  `scripts/gen_api_reference.py`, then `just api-reference`. Two of those fail
  nothing when missed: a name absent from `GROUPS` lands under *Everything else*,
  and one absent from the `TYPE_CHECKING` block is invisible to type checkers.
- To reproduce the **full** CI gate locally you also need the docs extra
  (`zensical` isn't in `dev`/`django`): `uv sync --extra dev --extra django --extra docs`,
  then `uv run zensical build`. Without `--extra docs` that step fails with
  `Failed to spawn: zensical`.

## Documentation

- **`docs/api-reference.md` is generated. Do not hand-edit it.** It is produced
  from `rakaia.__all__` / `django_rakaia.__all__` by
  `scripts/gen_api_reference.py`. Run `just api-reference` and commit the result;
  `just api-reference-check` fails if it has drifted. To move a name into a
  different section, edit `GROUPS` in that script, not the Markdown.
- **The nav in `zensical.toml` is grouped by reader intent** — *Start here*,
  *How do I…*, *How it works*, *Look it up*, *Experiments* — not by subsystem.
  The four Diátaxis modes were used to work out what each page is *for*; the
  section names are the plain-English version. When adding a page, decide which
  question the reader is asking and put it there.
- **Main pages stay plain-language; technical detail goes in a trailing
  `## Appendix` section on the same page.** Keeping the detail next to what it
  qualifies is deliberate — it is what stops the caveats rotting away from the
  claims they modify.
- Research notes live in `docs/research/` and are deliberately **not** in the
  nav. They are dated and are not decisions; decisions go in `docs/adr/`.
- **A release updates five files, and a PR updates the first of them.** These
  drifted apart across `0.3.0`/`0.3.1`, which is what this rule exists to stop:
  - `CHANGELOG.md` — for anything a consumer of the library could notice, under
    `[Unreleased]`, in the PR that makes the change. Repo tooling is out of scope
    by long precedent: the uv pin, the lint config and the CI matrix have never
    been changelogged, because the file documents the library, not the workshop.
    Never leave `[Unreleased]` absent: the release-prep PR renames it and
    immediately adds an empty one back. Reconstructing a release from the git log
    at tag time is how `whats-new.md` got skipped for two releases.
  - `UPGRADING.md` — if anything breaks, *or* if something works differently in a
    way that fails quietly. A trap that raises nothing (changing `RAKAIA_STORE`
    moves no data) belongs here even though it breaks no signature.
  - `docs/whats-new.md` — if a headline capability landed. It is a **cumulative**
    tour: append a numbered section, never rewrite the earlier ones. Every
    section owes the reader a problem, a snippet, and a one-command demo — and
    when no example covers the capability yet, name the tests that prove it
    instead of inventing a command, the way section 18 does.
  - `docs/examples.md` — if an example changed, and always check *Known gaps*.
    A new public API with no example is a gap; say so there rather than letting
    the matrix imply coverage that does not exist.
  - `okf/` — the machine-readable bundle. A new name, setting or command goes in
    the concept page that owns it, and every such edit gets a dated entry in
    `okf/log.md`. Only *examples* are gated (`test_docs_names_resolve.py` fails
    when an `examples/` directory has no bundle page), so a change that ships a
    setting or a command without shipping an example walks straight past the
    suite.
- **A new setting or management command is a feature, not plumbing.** The tour's
  *headline capability* test reads as an invitation to skip anything that sounds
  internal, and that is how `0.7.0` shipped: paged catch-up reads, the event
  stamp that can no longer contradict an event's position, and
  `RAKAIA_PERMANENT_STREAMS` with `manage.py prune_orphan_events` all reached
  `CHANGELOG.md` and none of them reached `docs/whats-new.md` or `okf/` until
  after the tag. If a consumer would change a setting, run a command, or get a
  different answer from the same call, it belongs in the tour and the bundle.
  Nothing checks either of those two files, so this is on whoever writes the PR;
  the repair costs an afternoon of reading eleven commits back, which is what it
  cost here.
- **Tag a release `v<version>`, with the `v`.** `publish.yml` triggers on `v*`
  (plus a manual `workflow_dispatch`), so a tag named `0.4.0` publishes nothing — no failure, no output,
  just an absent release that looks like a slow PyPI. Worth stating because the
  sibling repos disagree: `formkit-ninja` tags `4.1.0` without a prefix, and
  moving between the two in one sitting is how the wrong one gets typed.

- **Doc code samples are checked against the real signature, not from memory.**
  Every name in a snippet must resolve and every keyword must exist —
  `rebuild_and_verify` was documented with three wrong arguments on the first
  pass here. `docs/api-reference.md` is generated and is the cheapest place to
  confirm a signature.
- **`just demos` does not cover `chat` or `polyglot`.** They need a running
  server, so CI never touches them and only a manual run finds a break. If you
  change either — or the store, SSE or URL wiring underneath them — start the
  server and exercise the real endpoint before claiming they work.

## Writing it up

- **Two plain paragraphs.** A commit message body, an issue body and a PR body are each
  at most two paragraphs, written so someone who only reads the notification email gets
  the whole point. Say what changed and what it means for whoever uses this. That is the
  deliverable.
- **Everything else goes in a comment.** Tables, measured counts, SHAs, query output,
  repro steps, mutation records, per-file reasoning — post them below the body, on the PR.
  They are evidence for whoever verifies the work, not the summary for whoever reads it. A
  commit message has nowhere to put them, so they belong on its PR instead.
- **No machinery in the body.** This library's own words — stream, event, consumer,
  cursor, offset, replay, backend — are the vocabulary and are fine. Identifiers are not:
  `DjangoExecutor.apply` is "the part that writes a batch", `_StageBuffer` is "batching a
  pass together". Save the precise names for the comment, where precision is the job.
- The test: read the body alone. If it needs the source open, or the point arrives after
  the evidence, rewrite it.
- **Docstrings too: two paragraphs**, with one difference — a docstring's reader is
  looking at the code, so identifiers *are* their vocabulary. What does not belong is the
  essay: the history of a bug, what an earlier cut did, or the same point twice. A
  non-obvious constraint earns its own short paragraph; if it took a mutation to find,
  write it down.

## Cross-repo claims are measured, not remembered

This repository is one of three that make one product — partisipa-import,
formkit-ninja and rakaia — plus `catalpainternational/ansible`, which alone
records what each host actually runs. Any claim about what another one of them
pins, requires or deploys is a **measurement with a date on it**, and it goes
stale within a day or two. Your memory of it, and anything you read about it
earlier in a session, are both worse than stale: they read exactly like
something current.

So before you assert cross-repo state — in an issue, a comment, a commit
message, a plan, or a decision about what to work on — fetch and read it:

```bash
git -C <repo> fetch --all -q
git -C <repo> show origin/main:backend/pyproject.toml   # what partisipa pins
git -C <repo> show origin/main:backend/uv.lock          # what it resolves
git -C ~/github/catalpainternational/ansible show \
    origin/partisipa-staging:host_vars/partisipa-production.yaml   # what a host runs
```

Read from `origin/<branch>`, never from a working tree — yours or a sibling's —
and never from a document that quotes it. A tag is not a release, a release is
not a merge, and a merge is not a deploy; each of those gaps has been the answer
to "why is this not working" at least once.

**And never turn a release note into a consequence for another repository
without opening that repository's code** — in either direction, whether you are
reading their changelog or writing your own. A changelog says what changed in
the package. It does not say what that means for a consumer, who may already do
the thing, may not reach the code at all, or may hold a pin that makes it moot.

Both halves cost real work on 2026-09-23. An agent reported partisipa-import as
pinning `rakaia-streams==0.5.*`, from a checkout 134 commits behind
`origin/main`, when that repo's own `main` had said `==0.6.*` for a day — and a
second repo planned a version crossing against that premise before a third
re-measured and caught it. Note where the damage is: not in the stale checkout,
which is ordinary, but in the claim leaving the repo that could have checked it.
The other half, the same day: a note that rakaia 0.7.0 "changes what a `--reset`
backfill destroys" was carried into six pages before anyone opened
partisipa-import's backfill commands and found they had been doing that delete
themselves all along.

This library is consumed and pre-1.0, and `docs/public-api.md` is the promise.
Its bounds are conditional, which is the part that tells you what to check:
`>=0.X,<0.X+1` is right for a consumer living inside Tier 1, but a consumer that
queries the ORM models or imports a submodule path directly pins to an exact
minor, because the table shape and the module layout are both allowed to move
within a minor. partisipa-import is squarely in that second case —
`django_rakaia.models.StreamEntry`/`StreamEvent` across seven backfill commands,
and `rakaia.replay._reducer_wants_touched` in one test — so it pins `==0.6.*`.

**So work out which tier you changed before writing that it reaches them.** A
Tier 1 change is not supposed to arrive until a minor. A Tier 2 change — an ORM
column, a module path — reaches anyone on an exact-minor pin the moment they
move it, and nobody at all before, whatever the changelog says. That is a
question you can answer here, in this repo, before you write the sentence.

The `--reset` example above is not a mistake this repo made: it was a claim
*about* rakaia propagating through other people's documents while nobody opened
the consumer's code. It sits here as the worked example of the rule, because the
answer was in partisipa-import's seven `_prepare_stream` sites and no amount of
care with our own changelog would have produced it.

The cross-repo state itself — version table, what each host runs, the shared
ADRs — lives in `joshbrooks/shared` and is gated by `just check` there. Treat it
the same way: a starting point to re-measure from, never an answer to quote.

## Agent skills

### Issue tracker

Issues live as GitHub issues in `joshbrooks/rakaia`, via the `gh` CLI. Issue and PR
titles and bodies must be short and jargon-free — two paragraphs at most, with any
technical detail appended as a comment instead. See `docs/agents/issue-tracker.md`.

### Triage labels

Canonical five-label vocabulary, unmapped. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context. Domain language lives in `docs/glossary.md` (not `CONTEXT.md`); decisions in `docs/adr/`. See `docs/agents/domain.md`.

## gstack

Use the `/browse` skill from gstack for all web browsing. Never use `mcp__claude-in-chrome__*` tools.

### Available skills

- `/office-hours`
- `/plan-ceo-review`
- `/plan-eng-review`
- `/plan-design-review`
- `/design-consultation`
- `/design-shotgun`
- `/design-html`
- `/review`
- `/ship`
- `/land-and-deploy`
- `/canary`
- `/benchmark`
- `/browse`
- `/connect-chrome`
- `/qa`
- `/qa-only`
- `/design-review`
- `/setup-browser-cookies`
- `/setup-deploy`
- `/retro`
- `/investigate`
- `/document-release`
- `/codex`
- `/cso`
- `/autoplan`
- `/plan-devex-review`
- `/devex-review`
- `/careful`
- `/freeze`
- `/guard`
- `/unfreeze`
- `/gstack-upgrade`
- `/learn`
