# CLAUDE.md

## Development

- **Running tests:** the test deps live in extras. In a fresh checkout **or a new
  git worktree**, run `uv sync --extra dev --extra django` before `pytest` —
  plain `uv run` bootstraps only the zero-dependency core package, so tests fail
  with `No module named pytest` until the extras are synced.
- CI (`.github/workflows/ci.yml`) runs `ruff check`, `ruff format --check`,
  `pyright src/`, `pytest`, and `zensical build`; run those before pushing.
- **pyright is a hard gate** and `src/` is expected at zero errors. Run it as
  `PYRIGHT_PYTHON_FORCE_VERSION=latest uv run pyright src/` locally — plain
  `uv run pyright` can object to its own pinned version. Django's synthesised
  attributes are declared explicitly (see the `if TYPE_CHECKING` blocks in
  `django_rakaia/models.py`) rather than waved through with ignores.
- To reproduce the **full** CI gate locally you also need the docs extra
  (`zensical` isn't in `dev`/`django`): `uv sync --extra dev --extra django --extra docs`,
  then `uv run zensical build`. Without `--extra docs` that step fails with
  `Failed to spawn: zensical`.

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
