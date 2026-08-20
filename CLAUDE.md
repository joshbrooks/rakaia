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
- **pyright is a hard gate** and `src/` is expected at zero errors. Run it as
  `PYRIGHT_PYTHON_FORCE_VERSION=latest uv run pyright src/` locally — plain
  `uv run pyright` can object to its own pinned version. Django's synthesised
  attributes are declared explicitly (see the `if TYPE_CHECKING` blocks in
  `django_rakaia/models.py`) rather than waved through with ignores.
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
