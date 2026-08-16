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
