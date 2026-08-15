# Rakaia justfile — convenience recipes for development and a
# production-style local run with Redis + multiple worker processes.
#
# Install just:    https://github.com/casey/just
#   Fedora:        sudo dnf install just
#   macOS:         brew install just
#   Arch:          pacman -S just
#   cargo:         cargo install just
#
# Then list available recipes with:
#   just            # or `just --list`

set dotenv-load := true

# ---------------------------------------------------------------------------
# Configuration (override on the command line: `just serve workers=8`)
# ---------------------------------------------------------------------------

REDIS_CONTAINER := env_var_or_default("REDIS_CONTAINER", "rakaia-redis")
REDIS_IMAGE     := env_var_or_default("REDIS_IMAGE", "docker.io/redis:7-alpine")
REDIS_PORT      := env_var_or_default("REDIS_PORT", "6379")
REDIS_URL       := env_var_or_default("REDIS_URL", "redis://localhost:" + REDIS_PORT + "/0")

# Where the chat sample lives. The justfile recipes change into this dir
# so manage.py / hypercorn / chat_project.* imports all resolve.
CHAT_DIR        := "examples/chat"
POLYGLOT_DIR    := "examples/polyglot"
ORDERS_DIR      := "examples/orders"
FORMKIT_DIR     := "examples/formkit_submissions"
HISTORY_DIR     := "examples/partisipa_history"
PARTISIPA_DIR   := "examples/partisipa_staged"
COOKBOOK_DIR    := "examples/projection_cookbook"
CLOSE_DIR       := "examples/partisipa_close"
MERGE_DIR       := "examples/partisipa_merge"
REPEATERS_DIR   := "examples/partisipa_repeaters"

# Note: we deliberately do NOT export DJANGO_SETTINGS_MODULE at the top
# level. Doing so leaks into `just test`, overriding the value pytest
# reads from pyproject.toml. Recipes that need a specific settings
# module set it inline.

# ---------------------------------------------------------------------------
# Help
# ---------------------------------------------------------------------------

default:
    @just --list

# ---------------------------------------------------------------------------
# Setup
# ---------------------------------------------------------------------------

# Install all dependency groups (dev + django + docs + prod)
install:
    uv sync --extra dev --extra django --extra docs --extra prod

# ---------------------------------------------------------------------------
# Guided feature tour
# ---------------------------------------------------------------------------

# Run the scripted feature demos end-to-end with narration (see docs/whats-new.md)
demo:
    @echo "==========================================================="
    @echo "  Rakaia feature tour  —  docs/whats-new.md"
    @echo "==========================================================="
    @echo ""
    @echo ">> 1/2  Versioned handlers, upcasters, replay & dry-run   (examples/orders)"
    @echo ""
    @just orders-demo
    @echo ""
    @echo ">> 2/2  Projections, fan-out & migration parity   (examples/formkit_submissions)"
    @echo ""
    @just formkit-demo
    @echo ""
    @echo "==========================================================="
    @echo "  Live SSE demos (web) — run either in a separate terminal:"
    @echo "    just dev           # chat      -> http://localhost:8000"
    @echo "    just polyglot-dev  # polyglot  -> http://localhost:8001"
    @echo "==========================================================="

# Run EVERY demo and fail on the first one that breaks.
#
# `just demo` is the narrated two-part tour for a human reader. This is the
# regression gate: all eleven demos, no narration budget, non-zero exit on the
# first failure. CI runs it, which is what makes the examples a tested surface
# rather than a directory of prose that happened to compile.
#
# Each demo asserts its own claims and raises CommandError when one does not
# hold, so "it ran" and "it was right" are the same outcome.
demos:
    @echo ">> protocol layer (no Django)"
    @just protocol-demo
    @echo ">> effect primitives (no Django)"
    @just multi-owner-demo
    @echo ">> orders — versioned handlers, upcasters, replay"
    @just orders-demo
    @echo ">> formkit — projections, fan-out, migration parity"
    @just formkit-demo
    @echo ">> formkit stream — append log as source of truth"
    @just formkit-stream-demo
    @echo ">> projection cookbook — staged replay + verification"
    @just cookbook-demo
    @echo ">> partisipa history — pghistory retirement"
    @just partisipa-history-demo
    @echo ">> partisipa staged — late-arriving cross-form links"
    @just partisipa-demo
    @echo ">> partisipa close — guarded transition"
    @just partisipa-close-demo
    @echo ">> partisipa merge — multi-stream deterministic replay"
    @just partisipa-merge-demo
    @echo ">> partisipa repeaters — tree reconcile, no orphans"
    @just partisipa-tree-demo
    @echo ""
    @echo "All demos passed."

# ---------------------------------------------------------------------------
# Redis (podman)
# ---------------------------------------------------------------------------

# Start Redis in a podman container (idempotent — reuses an existing one)
redis-up:
    @podman start {{REDIS_CONTAINER}} 2>/dev/null \
        || podman run -d --name {{REDIS_CONTAINER}} \
                      -p {{REDIS_PORT}}:6379 \
                      {{REDIS_IMAGE}}
    @podman ps --filter name={{REDIS_CONTAINER}}

# Stop the Redis container
redis-down:
    -podman stop {{REDIS_CONTAINER}}

# Stop and remove the Redis container
redis-destroy: redis-down
    -podman rm {{REDIS_CONTAINER}}

# Tail Redis container logs
redis-logs:
    podman logs -f {{REDIS_CONTAINER}}

# Open a redis-cli session against the running container
redis-cli:
    podman exec -it {{REDIS_CONTAINER}} redis-cli

# ---------------------------------------------------------------------------
# Django management commands (chat sample)
# ---------------------------------------------------------------------------

# Run database migrations against the chat sample
migrate:
    cd {{CHAT_DIR}} && uv run python manage.py migrate

# Collect static files into chat's STATIC_ROOT (needed by WhiteNoise in prod)
collectstatic:
    cd {{CHAT_DIR}} && \
        DJANGO_SETTINGS_MODULE=chat_project.settings_prod \
        uv run python manage.py collectstatic --noinput

# Create a Django superuser interactively
createsuperuser:
    cd {{CHAT_DIR}} && uv run python manage.py createsuperuser

# Defaults to dev settings. For the prod path:
#   just demo-user chat_project.settings_prod
#
# Create or reset the chat sample's demo superuser (admin / admin) — idempotent
demo-user settings="chat_project.settings":
    @cd {{CHAT_DIR}} && DJANGO_SETTINGS_MODULE={{settings}} uv run python manage.py shell -c "from django.contrib.auth import get_user_model as _G; _u, _c = _G().objects.get_or_create(username='admin', defaults={'email': 'admin@example.com'}); _u.is_staff = True; _u.is_superuser = True; _u.set_password('admin'); _u.save(); print(('created' if _c else 'updated') + ' demo superuser: admin / admin')"

# Drop into the Django shell with the chat models loaded
shell:
    cd {{CHAT_DIR}} && uv run python manage.py shell

# ---------------------------------------------------------------------------
# Servers
# ---------------------------------------------------------------------------

# Single-worker dev server (in-memory channel layer, autoreload)
dev: migrate
    cd {{CHAT_DIR}} && uv run python manage.py runserver

# Defaults to 4 workers on 0.0.0.0:8000. Recipe parameters are
# positional, so override like:
#   just serve 8                       # 8 workers
#   just serve 2 127.0.0.1 9000        # 2 workers, custom host/port
#
# Production-style multi-worker run (hypercorn + Redis channel layer)
serve workers="4" host="0.0.0.0" port="8000": redis-up
    @echo "Starting hypercorn with {{workers}} worker(s) on http://{{host}}:{{port}}"
    @echo "Settings: chat_project.settings_prod | Redis: {{REDIS_URL}}"
    cd {{CHAT_DIR}} && \
        DJANGO_SETTINGS_MODULE=chat_project.settings_prod \
        REDIS_URL={{REDIS_URL}} \
        uv run python manage.py migrate --noinput
    cd {{CHAT_DIR}} && \
        DJANGO_SETTINGS_MODULE=chat_project.settings_prod \
        REDIS_URL={{REDIS_URL}} \
        uv run python manage.py collectstatic --noinput
    cd {{CHAT_DIR}} && \
        DJANGO_SETTINGS_MODULE=chat_project.settings_prod \
        REDIS_URL={{REDIS_URL}} \
        uv run hypercorn \
            --bind {{host}}:{{port}} \
            --workers {{workers}} \
            --access-logfile - \
            chat_project.asgi:application

# Smoke-test a running prod server (requires `curl`)
smoke host="http://localhost:8000":
    @curl -fsS {{host}}/ > /dev/null && echo "OK: {{host}}/"

# ---------------------------------------------------------------------------
# Polyglot sample (live-editable translations demo)
# ---------------------------------------------------------------------------

# Single-worker dev server for the polyglot sample
polyglot-dev:
    cd {{POLYGLOT_DIR}} && uv run python manage.py migrate
    cd {{POLYGLOT_DIR}} && uv run python manage.py runserver 0.0.0.0:8001

# Multi-worker hypercorn run for the polyglot sample
polyglot-serve workers="4" host="0.0.0.0" port="8001": redis-up
    @echo "Starting polyglot on http://{{host}}:{{port}} ({{workers}} worker(s))"
    cd {{POLYGLOT_DIR}} && \
        DJANGO_SETTINGS_MODULE=polyglot_project.settings_prod \
        REDIS_URL={{REDIS_URL}} \
        uv run python manage.py migrate --noinput
    cd {{POLYGLOT_DIR}} && \
        DJANGO_SETTINGS_MODULE=polyglot_project.settings_prod \
        REDIS_URL={{REDIS_URL}} \
        uv run python manage.py collectstatic --noinput
    cd {{POLYGLOT_DIR}} && \
        DJANGO_SETTINGS_MODULE=polyglot_project.settings_prod \
        REDIS_URL={{REDIS_URL}} \
        uv run hypercorn \
            --bind {{host}}:{{port}} \
            --workers {{workers}} \
            --access-logfile - \
            polyglot_project.asgi:application

# ---------------------------------------------------------------------------
# Orders sample (versioned handlers / replay demo)
# ---------------------------------------------------------------------------

# Seed the orders stream and replay it through versioned handlers
orders-demo:
    cd {{ORDERS_DIR}} && uv run python manage.py migrate
    cd {{ORDERS_DIR}} && uv run python manage.py demo_orders --twice

# Projection cookbook: staged replay + reader + executor + diff verification
cookbook-demo:
    cd {{COOKBOOK_DIR}} && uv run python manage.py migrate
    cd {{COOKBOOK_DIR}} && uv run python manage.py demo_cookbook

# Dev server showing the materialized orders projection
orders-dev:
    cd {{ORDERS_DIR}} && uv run python manage.py migrate
    cd {{ORDERS_DIR}} && uv run python manage.py runserver 0.0.0.0:8002

# Live stream demo: random orders pushed ~1/s + a submit form (open /live/)
orders-live:
    cd {{ORDERS_DIR}} && uv run python manage.py migrate
    @echo "Open http://localhost:8002/live/ — orders stream in live via op=\"update\""
    cd {{ORDERS_DIR}} && uv run python manage.py runserver 0.0.0.0:8002

# ---------------------------------------------------------------------------
# FormKit-submissions prototype (adoption spike for formkit-ninja)
# ---------------------------------------------------------------------------

# Seed submissions, replay, and assert replay == direct to_model()
formkit-demo:
    cd {{FORMKIT_DIR}} && uv run python manage.py migrate
    cd {{FORMKIT_DIR}} && uv run python manage.py demo_submissions --twice

# Arrow-flip: SubmissionEvent (append log = source of truth) -> Submission projection
formkit-stream-demo:
    cd {{FORMKIT_DIR}} && uv run python manage.py migrate
    cd {{FORMKIT_DIR}} && uv run python manage.py demo_submission_stream

# Dev server showing the materialized submission projection
formkit-dev:
    cd {{FORMKIT_DIR}} && uv run python manage.py migrate
    cd {{FORMKIT_DIR}} && uv run python manage.py runserver 0.0.0.0:8003

# ---------------------------------------------------------------------------
# pghistory-retirement spike (reproduce audit + recovery from a stream)
# ---------------------------------------------------------------------------

# Assert a stream w/ event envelope reproduces pghistory's audit + recovery
partisipa-history-demo:
    cd {{HISTORY_DIR}} && uv run python manage.py migrate
    cd {{HISTORY_DIR}} && uv run python manage.py demo_history
# Staged-replay spike (late-arriving cross-form links — issue #7)
# ---------------------------------------------------------------------------

# Reproduce the unlinked bug, then resolve it with staged replay + self-heal
partisipa-demo:
    cd {{PARTISIPA_DIR}} && uv run python manage.py migrate
    cd {{PARTISIPA_DIR}} && uv run python manage.py demo_staged

# ---------------------------------------------------------------------------
# Close-precondition state machine spike (guarded transition — issue #7)
# ---------------------------------------------------------------------------

# Decide a POM_1 cycle close from cross-form preconditions; reject then self-heal
partisipa-close-demo:
    cd {{CLOSE_DIR}} && uv run python manage.py migrate
    cd {{CLOSE_DIR}} && uv run python manage.py demo_close

# ---------------------------------------------------------------------------
# Multi-stream merge spike (replay N form pipelines as one — issue #7)
# ---------------------------------------------------------------------------

# Merge three form streams into one deterministic replay; assert parity + ties
partisipa-merge-demo:
    cd {{MERGE_DIR}} && uv run python manage.py migrate
    cd {{MERGE_DIR}} && uv run python manage.py demo_merge

# ---------------------------------------------------------------------------
# Tree-reconcile spike (unbounded nested repeaters, no orphans — issue #7)
# ---------------------------------------------------------------------------

# Resubmit a pruned repeater tree; assert no deep orphans, no double-count
partisipa-tree-demo:
    cd {{REPEATERS_DIR}} && uv run python manage.py migrate
    cd {{REPEATERS_DIR}} && uv run python manage.py demo_repeaters

# ---------------------------------------------------------------------------
# Standalone Rakaia protocol server (no Django)
# ---------------------------------------------------------------------------

# Run the zero-dep ASGI rakaia app under uvicorn
rakaia port="4437":
    uv run uvicorn rakaia:app --host 0.0.0.0 --port {{port}}

# Protocol layer (no Django): append/read, producer fencing, close, poll cursors
protocol-demo:
    uv run python examples/protocol_streams/demo.py

# Effect primitives (no Django): Ref, reconcile_aggregate(owns=), reconcile_by_key
multi-owner-demo:
    cd examples/multi_owner && uv run python demo.py

# ---------------------------------------------------------------------------
# Durable Streams conformance suite
# ---------------------------------------------------------------------------

# Start rakaia, run the upstream conformance suite against it, tear it down.
# Requires node/npm. See conformance/README.md.
conformance port="4437":
    ./conformance/run.sh {{port}}

# Regenerate conformance/expected-failures.txt from a fresh run. Use when the
# accepted protocol gap changes (fork lands, or the suite version bumps); review
# the diff before committing.
conformance-baseline port="4437":
    ./conformance/run.sh {{port}}
    node conformance/check-regressions.mjs --write-baseline

# ---------------------------------------------------------------------------
# Quality gates
# ---------------------------------------------------------------------------

# Run the test suite
test:
    uv run pytest

# Run the test suite with coverage
test-cov:
    uv run pytest --cov=src/rakaia --cov=src/django_rakaia --cov-report=term-missing

# Lint
lint:
    uv run ruff check src/ tests/

# Format check (no writes)
fmt-check:
    uv run ruff format --check src/ tests/

# Format in place
fmt:
    uv run ruff format src/ tests/

# Type check
typecheck:
    uv run pyright src/

# Build the documentation site
docs:
    uv run zensical build

# Serve the documentation locally
docs-serve:
    uv run zensical serve

# Run the full quality gate (lint + format + tests + docs build).
# Note: `typecheck` is a separate recipe — pyright currently surfaces a
# bunch of pre-existing Django ORM dynamism warnings on this codebase
# (queryset annotations, related managers, implicit `id` fields). Run
# `just typecheck` if you want to see them; fixing them is tracked
# separately and would benefit from migrating to `django-stubs`.
check: lint fmt-check test docs

# ---------------------------------------------------------------------------
# Release / publishing (dist name: rakaia-streams)
# ---------------------------------------------------------------------------

# Build the sdist + wheel into dist/ (clears stale artifacts first)
build:
    rm -rf dist/
    uv build
    uvx twine check dist/*

# Tag the current pyproject version and push it — this triggers .github/workflows/publish.yml
release: check build
    #!/usr/bin/env bash
    set -euo pipefail
    version="$(uv run --no-project python -c \
        'import tomllib,pathlib; print(tomllib.loads(pathlib.Path("pyproject.toml").read_text())["project"]["version"])')"
    if [ -n "$(git status --porcelain)" ]; then
        echo "Working tree is dirty — commit before releasing." >&2
        exit 1
    fi
    echo "Tagging v${version} and pushing (CI publishes to PyPI)."
    git tag "v${version}"
    git push origin "v${version}"

# Upload dist/ to TestPyPI by hand (needs a TestPyPI token; CI is the normal path)
publish-test: build
    uvx twine upload --repository testpypi dist/*

# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

# Remove Python caches, the chat sample's SQLite DB, and collected statics
clean:
    -find . -type d -name __pycache__ -prune -exec rm -rf {} +
    -find . -type d -name .pytest_cache -prune -exec rm -rf {} +
    -find . -type f -name "*.pyc" -delete
    -rm -f {{CHAT_DIR}}/db.sqlite3
    -rm -rf {{CHAT_DIR}}/staticfiles/
    -rm -f {{POLYGLOT_DIR}}/db.sqlite3
    -rm -rf {{POLYGLOT_DIR}}/staticfiles/
    -rm -rf site/

# Tear down everything: stop Redis container and clean caches
nuke: redis-destroy clean
