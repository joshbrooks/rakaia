# CLAUDE.md

## Development

- **Running tests:** the test deps live in extras. In a fresh checkout **or a new
  git worktree**, run `uv sync --extra dev --extra django` before `pytest` —
  plain `uv run` bootstraps only the zero-dependency core package, so tests fail
  with `No module named pytest` until the extras are synced.
- CI (`.github/workflows/ci.yml`) runs `ruff check`, `ruff format --check`,
  `pytest`, and `zensical build`; run those before pushing.
- To reproduce the **full** CI gate locally you also need the docs extra
  (`zensical` isn't in `dev`/`django`): `uv sync --extra dev --extra django --extra docs`,
  then `uv run zensical build`. Without `--extra docs` that step fails with
  `Failed to spawn: zensical`.

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
