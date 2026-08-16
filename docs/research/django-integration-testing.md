# How can rakaia's Django integration be tested and improved?

**Status:** research notes, 2026-08-15. Not a decision — no ADR is implied by
this file, and nothing here changes source.

**Where this lives and why.** Same reasoning as
[`handler-types.md`](handler-types.md): `docs/` holds prose docs listed in
`zensical.toml`'s explicit `nav`, `docs/adr/` holds decisions, `okf/` holds the
machine-readable bundle. A research note is none of those. It sits in
`docs/research/` — inside the docs tree, searchable, versioned with the code it
argues about — and is deliberately **not** added to `zensical.toml`'s `nav`.

---

## The question, and why now

`django_rakaia` is 5,058 lines — 25 top-level modules, 9 migrations, one
management command. It has 521 tests and **94% line coverage** (measured
2026-08-15,
`uv run pytest tests/test_django_rakaia --cov=src/django_rakaia`). By the usual
metric it is well tested.

The question is whether that number means what it looks like it means. The
answer is no, and the reason is stated in the source itself, four times:

| Where | What it says |
|---|---|
| `src/django_rakaia/django_store.py:248` | "a no-op on backends without row locks (SQLite)" |
| `src/django_rakaia/django_store.py:311` | "SQLite has no row locks, so a missing transaction here is **invisible to the test suite** but a guaranteed 500 in production" |
| `src/django_rakaia/effect_executor.py:206` | "No-op on SQLite, which serialises writers anyway" |
| `src/django_rakaia/django_store.py:752` | "backends with `INSERT ... RETURNING` (Postgres, and the modern SQLite used in CI)" |

And once in the test suite, as a test written for a CI leg that does not exist
(`tests/server_store_contract.py:205`):

> This is also the durable store's only write path that ever ran outside a
> transaction — invisible on SQLite (no row locks), a guaranteed
> `TransactionManagementError` 500 on Postgres. **The case exists so a Postgres
> CI leg fails** if the `transaction.atomic()` around create is ever removed.

There is no Postgres CI leg (`.github/workflows/ci.yml`, `conformance.yml`,
`publish.yml` — the only three workflows). So the codebase has already written
down, in five places, that its most safety-critical behaviour is not exercised.
This note takes that seriously and asks what else is in the same category.

---

## What the Django surface actually is

`django_rakaia` is not a thin wrapper. It touches most of Django's stateful
machinery. Enumerated from source:

| Django facility | Where | What depends on it |
|---|---|---|
| **Models / ORM** | `models.py` — 6 models (`Stream:27`, `StreamOffsetWatermark:175`, `StreamProducer:196`, `StreamEvent:228`, `StreamEntry:305`, `ConsumerCursor:348`) | The whole durable log |
| **`transaction.atomic()`** | `django_store.py:317,388,466,681,779`; `decorators.py:133`; `effect_executor.py:90` | Offset allocation, producer fencing, batch append, effect application |
| **`select_for_update()`** | `models.py:161` (offset watermark), `django_store.py:250` (stream row), `effect_executor.py:211` (retire transition capture) | Concurrent-writer serialization; the entire correctness argument for offsets and fencing |
| **`post_save` / `post_delete` signals** | `decorators.py:236,259` (`@stream_model`); `channels_signals.py:95` (SSE fan-out) | Model-driven event emission; live broadcast |
| **`raw=True` signal guard** | `decorators.py:247`, `channels_signals.py:110` | `loaddata` / `serialized_rollback` must not append phantom events |
| **Migrations** | `migrations/0001`–`0009` | Schema; `0007` has a data migration (2 uncovered lines) |
| **Multi-DB / `using=`** | `effect_executor.py:73,81,90`; `projection_reader.py`; `verification.py:480` | Disposable-DB rebuild verification (ADR 0003) |
| **`connection.execute_wrapper`** | `hermeticity.py:120` | `deny_database_access` — the read-side hermeticity gate |
| **System checks** | `checks.py:26` (`rakaia.E001`, `rakaia.W001`) | Store misconfiguration caught at `manage.py check` |
| **`AppConfig.ready()`** | `apps.py:10` | Handler autodiscovery, conditional Channels wiring |
| **Middleware** | `middleware.py:32` — sync-only, `ContextVar`-based provenance | Envelope `metadata` stamping |
| **Async ORM bridge** | `django_store.py:959` (`sync_to_async(thread_sensitive=True)`), `:1043`, `:1053`, `:1058` | Serving the Durable Streams protocol over the durable store |
| **Async querysets** | `channels_views.py:64` (`async for entry in entries_qs`) | SSE catch-up |
| **`StreamingHttpResponse`** | `channels_views.py:105` | SSE |
| **Channels channel layer** | `channels_signals.py:85,90`; `channels_views.py:51-55,80` | Cross-process broadcast |
| **Management command** | `management/commands/replay.py` | Operator-facing replay |
| **Admin** | `admin.py:18,38,96` (3 registrations) + `register_stream_event_admin:140` | Browsing |
| **Settings** | `RAKAIA_STORE`, `RAKAIA_ENABLE_SSE` (`store.py:61`, `apps.py:41`) | Backend selection, optional-extra gating |

Two structural facts matter for testing:

1. **The store is a process-wide singleton behind a module-level dict**
   (`store.py:19-20`, `_stores` + `_store_lock`). It is not reset by any Django
   or pytest-django fixture, because it holds no DB state — the in-memory
   variant holds *all* its state.
2. **Every offset the durable store issues goes through one locked
   read-modify-write** (`models.py:131-172`,
   `Stream.get_next_offset_block`). The docstring says "Must be called inside a
   transaction". Nothing in the test suite can check that claim (see below).

---

## What is currently tested

521 tests in `tests/test_django_rakaia/` (36 `test_*.py` modules), plus four shared contract
suites at `tests/` root (`store_contract.py`, `server_store_contract.py`,
`executor_contract.py`, `projection_reader_contract.py`) that are run against
**both** the in-memory and the Django store — a genuinely good pattern, and the
strongest thing in the harness. `tests/test_django_rakaia/test_store_contract.py`
and `test_server_store_contract.py` are three-line adapters that bind the shared
suite to `DjangoStreamStore`.

Per-module line coverage (same run):

| Module | Cov | Uncovered |
|---|---|---|
| `integration.py` | **0%** | all of it — the documented ASGI mount |
| `admin.py` | 84% | 79-80, 160, 178-184, 193-199, 203, 207-210 |
| `management/commands/replay.py` | 87% | 77, 79-80, 82 |
| `channels_signals.py` | 89% | 83, 87, 113 |
| `django_store.py` | 92% | 25 lines |
| `verification.py` | 94% | 15 lines |
| `models.py` | 95% | 6 lines |
| everything else | 98–100% | |

Transaction-mode usage is deliberate and documented where used. Only 6 of 36
test modules opt into `pytest.mark.django_db(transaction=True)`:
`test_channels.py:48,117`, `test_protocol_server.py:26`,
`test_publish_on_append.py:35`, `test_server_store_contract.py:21`,
`test_django_store.py:442`. `test_server_store_contract.py:7` explains why:
"because the async cases reach the database through …". The other ~59
`django_db` marks are the default (transaction-wrapped) mode.

Two test modules declare a second database:
`pytest.mark.django_db(databases=["default", "overlay"])` —
`test_using_seam.py:24` and `test_hermeticity.py:37`, against an `overlay`
alias defined at `tests/test_django_rakaia/settings.py:6`.

---

## The gaps

### Gap 1 — one backend, and it is the one that cannot express the invariants

`tests/test_django_rakaia/settings.py:1-7` configures two SQLite `:memory:`
aliases and nothing else. Every one of the eleven `examples/*/settings.py`
files is also SQLite (`grep -rn ENGINE examples/`). There is no
`psycopg`/`postgres` string anywhere in `.github/workflows/`.

This is not a coverage quibble — it silently disarms three mechanisms. From
Django's own source (6.0.3, as installed in `.venv`;
`django/db/models/sql/compiler.py:840`):

```python
if self.query.select_for_update and features.has_select_for_update:
    if (
        self.connection.get_autocommit()
        # Don't raise an exception when database doesn't
        # support transactions, as it's a noop.
        and features.supports_transactions
    ):
        raise TransactionManagementError(
            "select_for_update cannot be used outside of a transaction."
        )
```

`has_select_for_update` defaults to `False`
(`django/db/backends/base/features.py:50`) and the SQLite backend does not
override it. So on SQLite the *entire* branch is skipped. Two consequences,
both load-bearing here:

- **No lock is taken.** `models.py:161`, `django_store.py:250` and
  `effect_executor.py:211` are no-ops. The concurrency argument for
  monotonic offsets, for producer fencing ("the row lock is what makes fencing
  fence", `django_store.py:682`), and for the retire-transition capture
  ("without the lock, under READ COMMITTED the reported flip set could
  diverge", `effect_executor.py:205`) is **entirely unexercised**.
- **The "must be inside a transaction" contract is unenforced.** The
  `TransactionManagementError` that would catch a missing `atomic()` is inside
  the skipped branch. This is precisely the failure mode
  `django_store.py:309-314` and `tests/server_store_contract.py:205` warn about
  in prose, having no way to assert it.

Django's QuerySet reference states the same thing from the outside:

> Using `select_for_update()` on backends which do not support `SELECT ... FOR
> UPDATE` (such as SQLite) will have no effect. `SELECT ... FOR UPDATE` will not
> be added to the query, and an error isn't raised if `select_for_update()` is
> used in autocommit mode.

and lists the backends that do: "The `postgresql`, `oracle`, and `mysql`
database backends support `select_for_update()`"
(<https://docs.djangoproject.com/en/5.2/ref/models/querysets/#select-for-update>).

**And a Postgres CI leg alone would not be enough.** The same page carries the
warning that closes the loop with Gap 3:

> Although `select_for_update()` normally fails in autocommit mode, since
> `TestCase` automatically wraps each test in a transaction, calling
> `select_for_update()` in a `TestCase` even outside an `atomic()` block will
> (perhaps unexpectedly) pass without raising a `TransactionManagementError`.
> **To properly test `select_for_update()` you should use
> `TransactionTestCase`.**

So the guard is disarmed twice over, independently: once by the backend, once by
the test-case class. Switching to Postgres without also switching those tests to
`transaction=True` would move the needle less than it looks.

The library's own docs point deployments at Postgres
(`django_store.py:753`: "The durable store targets Postgres"). The gap between
"targets Postgres" and "tested only on SQLite" is the single widest one here.

### Gap 2 — one Django version, one Python version

`pyproject.toml` declares `requires-python = ">=3.10"`, `django>=4.2.0`, and
classifiers for Python 3.10/3.11/3.12/3.13. CI installs exactly one
combination: `uv python install 3.12` (`ci.yml:26`, `conformance.yml:43`,
`demos` job) resolved through `uv.lock`, which pins **Django 5.2.12** for
`python_full_version < '3.12'` and **Django 6.0.3** for `>= 3.12`
(`uv.lock:526-547`). On Python 3.12 that is Django 6.0.3.

So of the declared support matrix, exactly one cell is ever built, and
**Django 4.2 is never installed by anything** — not by CI, not by the lockfile
on any Python the project supports. The `django>=4.2.0` floor is an untested
assertion.

Worse, it is an assertion about a version Django itself no longer supports. The
Django download page currently lists **5.2 LTS** ("End of mainstream support:
December 3, 2025", "End of extended support: April 2028"), **6.0** ("End of
mainstream support: August 4, 2026") and **6.1** as the supported series, with
6.1 as "the latest official version"
(<https://www.djangoproject.com/download/>). 4.2 is not on that list. Meanwhile
the lockfile's own split is forced by Django's Python floor: per Django's
install FAQ, 5.2 runs on Python 3.10–3.14 and **6.0 requires Python 3.12+**
(<https://docs.djangoproject.com/en/dev/faq/install/#what-python-version-can-i-use-with-django>).
That is exactly why `uv.lock` carries two Django versions.

Net: the project claims a floor Django has retired, and tests a ceiling one
minor behind Django's latest. There is also no `--resolution lowest-direct`
job, which is the packaging-level way to test a declared floor at all.

One smaller inconsistency falls out of the same table. Django's install FAQ
gives 4.2 → Python 3.8–3.12 and 5.2 → 3.10–3.14, so the intersection of
`django>=4.2` with `requires-python = ">=3.10"` is Python **3.10–3.12**. The
`Programming Language :: Python :: 3.13` classifier (`pyproject.toml:21`)
therefore advertises a combination that cannot include the declared floor. It is
harmless in practice — a 3.13 resolve simply picks a newer Django — but it is
another line in the metadata that nothing checks.

### Gap 3 — `TestCase` vs `TransactionTestCase` semantics are load-bearing here and mostly unstated

pytest-django's plain `django_db` mark maps to Django's `TestCase`;
`transaction=True` maps to `TransactionTestCase`, and "will allow the test to use
real transactions" (<https://pytest-django.readthedocs.io/en/latest/helpers.html>).
Django's own testing docs draw the line precisely:

> A `TransactionTestCase` resets the database after the test runs by truncating
> all tables. A `TransactionTestCase` may call commit and rollback and observe
> the effects of these calls on the database. A `TestCase`, on the other hand,
> does not truncate tables after a test. Instead, it encloses the test code in a
> database transaction that is rolled back at the end of the test.

and, decisively for this codebase:

> A consequence of this, however, is that some database behaviors cannot be
> tested within a Django `TestCase` class. For instance, **you cannot test that a
> block of code is executing within a transaction, as is required when using
> `select_for_update()`**. In those cases, you should use
> `TransactionTestCase`.

(<https://docs.djangoproject.com/en/5.2/topics/testing/tools/>, emphasis added.)

So the 30 or so `django_db` marks covering the durable store's write paths are
running in the one mode Django's docs name as unable to test the thing those
write paths are built around — on top of the backend (Gap 1) that removes the
lock entirely. The two effects compound.

Three behaviours in this codebase turn on that distinction and are only tested
in one mode:

- **`transaction.on_commit` is never used** (`grep -rn "on_commit" src/` — zero
  hits), even though `django_store.py:849` broadcasts to the channel layer
  **inside** `transaction.atomic()`, with the comment "Inside the transaction,
  matching the receiver's existing timing on the `append` path." That means a
  transaction that rolls back after an append has already told every SSE
  subscriber about an event that does not exist. Under the default `django_db`
  mode *every* test rolls back, so the suite has been running the phantom-frame
  scenario continuously without an assertion pointed at it. Django ships the
  tool for testing exactly this without paying for `TransactionTestCase` —
  `TestCase.captureOnCommitCallbacks()`, which "[r]eturns a context manager that
  captures `transaction.on_commit()` callbacks for the given database
  connection" and with `execute=True` "emulates a commit after the wrapped block
  of code" (<https://docs.djangoproject.com/en/5.2/topics/testing/tools/>) — and
  it is inapplicable here, because there are no `on_commit` callbacks to
  capture.
- **`select_for_update()` inside `django_db` (non-transactional)** is inside the
  outer atomic block Django opened, so even on a locking backend it would not
  reproduce cross-connection contention. Concurrency needs
  `transaction=True` *and* real threads/connections *and* a locking backend.
- **`raw=True` / `serialized_rollback`.** `decorators.py:244-248` and
  `channels_signals.py:110` guard against fixture loads, and
  `test_decorators_consumer_contract.py:6` names `serialized_rollback` as one of
  the two paths that produce them — but nothing in the suite actually runs with
  `serialized_rollback=True`. The guard is tested via `loaddata`-shaped calls,
  not via the mechanism it names. (pytest-django's own docs are blunt about the
  price: "Note that this will slow down that test suite by approximately 3x" —
  <https://pytest-django.readthedocs.io/en/latest/helpers.html>. One targeted
  test, not a suite-wide flag.)

### Gap 4 — the broadcast/commit ordering has no test either way

Related to Gap 3 but worth separating, because it is an *integration* defect
candidate rather than a harness one. Both broadcast paths fire pre-commit:

- `channels_signals.py:95` — a `post_save` receiver on `StreamEntry`;
- `django_store.py:849` — an explicit `self._publish(...)` inside
  `transaction.atomic()`, added because `bulk_create` does not fire `post_save`
  (issue #82).

The second deliberately copies the first's timing. Neither has a test asserting
what a subscriber sees when the enclosing transaction rolls back. The
`ProvenanceMiddleware` case makes this concrete: a view that appends and then
raises broadcasts an event whose row is gone.

### Gap 5 — multi-DB is half-wired, and the untested half is the `@stream_model` door

The `using=` seam is real and well tested for the **replay** path
(`test_using_seam.py`, `test_hermeticity.py`, both with
`databases=["default", "overlay"]`). It is absent from the **write** path:

- `decorators.py:133` opens `transaction.atomic()` with **no `using=`**, and
  `decorators.py:250` calls `create_stream_event(...)` without forwarding
  `kwargs["using"]` — which Django's `post_save` always supplies. So a model
  saved on a non-default alias emits its stream event onto `default`, in a
  transaction on `default`, splitting the write.
- `subscription.py:67,78` (`load_cursor`, `commit_cursor`) and
  `store.py` (`get_store()`) take no alias at all.
- `management/commands/replay.py:19-46` exposes `stream`, `--from`, `--to`,
  `--strict-drift`, `--dry-run` — **no `--database`/`--using`**, despite
  `replay_stream` being the documented entry point for the disposable-DB
  rebuild that the `using=` seam exists to enable.

There is also no `DATABASE_ROUTERS` anywhere in `src/`, `tests/` or `examples/`
(`grep -rn "DATABASE_ROUTERS"` → zero hits), so router interaction — the normal
way a real deployment splits reads and writes — is untested. Django's multi-DB
docs note that routers reach into migrations too: "`makemigrations` always
creates migrations for model changes, but if `allow_migrate()` returns `False`,
any migration operations for the `model_name` will be silently skipped when
running `migrate` on the `db`"
(<https://docs.djangoproject.com/en/5.2/topics/db/multi-db/#allow_migrate>).
For a library that ships its own migrations, "silently skipped" is the operative
phrase.

Worth noting that the two-alias tests are correctly written: Django's docs
warn that "By default, only the `default` database will be wrapped in a
transaction during a `TestCase`'s execution and attempts to query other
databases will result in assertion errors to prevent state leaking between
tests" (<https://docs.djangoproject.com/en/5.2/topics/testing/tools/>), which is
why `test_using_seam.py:24` and `test_hermeticity.py:37` declare
`databases=["default", "overlay"]`. The gap is coverage, not correctness.

### Gap 6 — no migration-drift check

`grep -rn "makemigrations" .github/ tests/ justfile` returns only
`ruff format --check` hits. Nothing runs `makemigrations --check`, and nothing
tests that the 9 migrations actually build the schema the models describe.
Django's own answer is one flag: `--check` "[m]akes `makemigrations` exit with a
non-zero status when model changes without migrations are detected. Implies
`--dry-run`"
(<https://docs.djangoproject.com/en/5.2/ref/django-admin/#cmdoption-makemigrations-check>).
The `--dry-run` implication is a 4.2-and-later guarantee — 4.2's own release
note says "In older versions, the missing migrations were also created when
using the `--check` option" — so it is safe for this project's declared floor,
which is one of the few places that floor helps rather than costs.

Coverage reports 100% on most migration files only because they are
*imported* during test-database creation, not because their forward/backward
behaviour is asserted. `0007_stream_closed_stream_closed_at_and_more.py` is at
80% with lines 15-16 uncovered — those are inside its data-migration function.

For a library that ships migrations into other people's projects, a model change
landing without its migration is a defect that reaches consumers as a runtime
`ProgrammingError`, and it is the single cheapest gate to add.

A live instance of the drift this invites: `0008_alter_translatable_unique_together_and_more.py`
drops the `Translatable` model ("demo domain, not library surface … **every**
consumer got the table whether or not they ever used it"), and `grep -rn "class
Translatable" src/` now returns nothing — but `docs/django-integration.md`'s
Admin section still tells readers that `django_rakaia.admin` "registers
`Stream`, `StreamEvent`, `StreamEntry`, and `Translatable`". `admin.py` has
three registrations (`:18`, `:38`, `:96`). Nothing checks the docs against the
code, so the manual describes a model that was deleted on the same day this note
was written.

### Gap 7 — async/ASGI is tested at the protocol layer but not at the connection layer

What *is* tested: `test_protocol_server.py` drives `create_app(store=DjangoStreamStore())`
over `httpx.ASGITransport` under `django_db(transaction=True)`, and
`test_channels.py` exercises the SSE view including `Last-Event-ID` handling and
group-mate filtering. That is real async coverage.

What is not:

- **The documented composition.** `integration.py` is at **0% coverage**. Its
  docstring (`integration.py:15-27`) and `docs/django-integration.md`
  ("Protocol HTTP API") both give a prefix-stripping `asgi.py` recipe, and
  `tests/test_django_rakaia/asgi.py` is a bare `get_asgi_application()` — the
  composed app the docs tell users to build is never assembled, so the
  prefix-stripping instruction is unverified.
- **Connection lifetime in the long-lived generator.**
  `channels_views.py:50-103` opens a queryset (`async for entry in entries_qs`,
  line 64), then loops on `channel_layer.receive()` forever. Nothing closes the
  DB connection for the life of the SSE response. Django's databases reference
  states the rule directly:

  > If a connection is created in a long-running process, outside of Django's
  > request-response cycle, the connection will remain open until explicitly
  > closed, or timeout occurs. You can use `django.db.close_old_connections()`
  > to close all old or unusable connections.

  and, for this exact deployment shape: "**When using ASGI, persistent
  connections should be disabled.** Instead, use your database backend's
  built-in connection pooling if available"
  (<https://docs.djangoproject.com/en/5.2/ref/databases/#persistent-connections>).
  `grep -rn "close_old_connections\|CONN_MAX_AGE" src/ docs/` returns nothing —
  neither the code nor the deployment docs mention either. *(Inference: I traced
  the code path; whether a connection is actually held for the life of the
  generator depends on the server and on `CONN_MAX_AGE`, and I did not measure
  it.)*
- **Transactions in async code.** Django's async topic guide says plainly:
  "**Transactions do not yet work in async mode.** If you have a piece of code
  that needs transactions behavior, we recommend you write that piece as a
  single synchronous function and call it using `sync_to_async()`"
  (<https://docs.djangoproject.com/en/5.2/topics/async/>). `django_store.py` does
  exactly that — `_append_with_producer_sync`, `_close_with_producer_sync`, and
  `run_sync` with `thread_sensitive=True` (`:959-967`) — so the design is
  right. What is missing is a test that *pins* it: nothing fails if a future
  change opens an `atomic()` on the async side of that boundary.
- **Channel layer semantics.** The test settings use
  `channels.layers.InMemoryChannelLayer` (`settings.py:27-31`). Channels' own
  docs: "In-memory channel layers operate with each process as a separate layer.
  This means that **no cross-process messaging is possible**"
  (<https://channels.readthedocs.io/en/latest/topics/channel_layers.html>).
  Cross-process delivery is the reason `DjangoStreamStore` exists at all
  (`django_store.py:190-195` polls rather than waiting on an in-process event
  "because a durable stream can be appended to by another process entirely"), so
  the broadcast half of that story is tested only in the mode that cannot
  exhibit it. Channels' `ChannelsLiveServerTestCase` is the documented way up a
  level, with its own constraint: "You can't use an in-memory database for your
  live tests" (<https://channels.readthedocs.io/en/latest/topics/testing.html>)
  — which the current `:memory:` settings would have to change to satisfy.
- **Sync middleware under ASGI.** `middleware.py:32` defines neither
  `async_capable` nor `sync_capable`, and stamps a `ContextVar`
  (`rakaia/context.py:29`). Whether the `ContextVar` set inside Django's
  sync-middleware adaptation is visible to an append made later in the same
  request under ASGI is *not* asserted anywhere — `test_middleware.py` has three
  cases, all calling the middleware directly as a plain callable. **This is the
  gap I am least certain about and the one most worth an experiment.**

### Gap 8 — the singleton store leaks between tests, per-module

`store.py:19` caches store instances in a module-level dict for the process
lifetime. The in-memory store holds *all* stream state there. Exactly one test
module clears it (`test_store_selection.py:31,33`) and exactly one adds an
autouse reset fixture (`test_replay_command.py:26-31`, "Reset the singleton
store between tests so streams don't leak"). There is **no
`tests/conftest.py` and no `tests/test_django_rakaia/conftest.py`** at all, so
the fix is copy-pasted per module rather than applied once.

Note also that `RAKAIA_STORE` is *unset* in the test settings, so the suite's
default store is the **in-memory** one. `test_replay_command.py`'s fixture calls
`get_store().clear()` — a method the durable store does not need and the
selection is not parameterized over, so flipping the suite to `durable` would
not just change a setting.

### Gap 9 — no query-count or performance assertions

`grep -rn "assert_num_queries" tests/` — zero hits. Several docstrings make
explicit query-count claims that nothing checks:

- `django_store.py:744-748`: `append_many` is "a handful of INSERTs regardless
  of N (not the 2N a loop of `append` issues)".
- `effect_executor.py:19-23`: `skip_unchanged=True` "trades one UPDATE per row
  for one SELECT per row".

These are exactly the claims `django_assert_num_queries` exists to pin, and they
are exactly the claims that regress silently. (Caveat worth knowing before
adopting it: pytest-django documents that the fixture "wraps
`django.test.utils.CaptureQueriesContext` and yields the wrapped
`CaptureQueriesContext` instance"
(<https://pytest-django.readthedocs.io/en/latest/helpers.html>) — and
`CaptureQueriesContext` appears nowhere in Django's own documentation; it is an
internal in `django/test/utils.py`. Django's documented public equivalent is
`assertNumQueries`. Either is fine; the dependency is just worth naming.)

### Gap 10 — the hermeticity guard is thread-local, and the store crosses threads

`deny_database_access` (`hermeticity.py:79-123`, ADR 0003) is built on
`connection.execute_wrapper`. Django's instrumentation docs say what that
installs:

> Returns a context manager which, when entered, installs a wrapper around
> database query executions, and when exited, removes the wrapper. **The wrapper
> is installed on the thread-local connection object.**

(<https://docs.djangoproject.com/en/5.2/topics/db/instrumentation/>)

Meanwhile `DjangoStreamStore.run_sync` (`django_store.py:959-967`) deliberately
moves every ORM call into a worker thread via
`sync_to_async(fn, thread_sensitive=True)`, with the docstring "`thread_sensitive=True`
keeps them all on the same one, which is what makes them share a transaction and
a connection."

The two facts sit next to each other and nothing in the suite tests their
interaction. If a rebuild is ever driven from an async context — which is the
whole reason `run_sync` exists — the deny wrapper installed on the calling
thread's connection object is not obviously the one the store's thread uses.
`test_hermeticity.py` and `test_rebuild_isolation.py` are entirely synchronous,
so the question has never been asked.

*(Inference, flagged as such: I have not run this. `thread_sensitive=True` runs
work in asgiref's shared executor thread, and `connections` is thread-local, so
a wrapper installed on the main thread would not be present there — but asgiref
does propagate `contextvars`, and Django's `connections` handler behaviour under
`sync_to_async` is subtle enough that this needs an experiment rather than an
argument. It is a ten-line test either way, and the answer matters: a
silently-inert hermeticity guard reports green for exactly the leak ADR 0003
exists to catch.)*

### Gap 11 — coverage is reported but not gated

`ci.yml:48` runs `pytest --cov=... --cov-report=term-missing`. There is no
`fail_under`, no `[tool.coverage]` section in `pyproject.toml`, and `just check`
(`justfile:401`) is `lint fmt-check test docs` — it omits `typecheck`, which CI
does gate (`ci.yml:40`).

---

## Improvement options

Deliberately given as options with costs, not a single recommendation.

### For the harness

| Option | What it buys | What it costs |
|---|---|---|
| **A. Postgres CI leg** — a `services: postgres` matrix axis, env-var-driven `DATABASES`, run the same suite | Arms `select_for_update` (Gap 1) and the `TransactionManagementError` guard; makes `tests/server_store_contract.py:205` mean what it says; validates `bulk_create` PK population on the backend that is actually targeted | ~2–4 min CI; a settings split; the `:memory:` `overlay` alias has no Postgres equivalent. **Must be paired with `transaction=True` on the locking tests** — Django's docs say a `TestCase` "will (perhaps unexpectedly) pass without raising a `TransactionManagementError`" even on Postgres, so a Postgres leg alone still does not test the lock |
| **B. Django/Python version matrix** — `4.2` (on py3.10/3.11), `5.2`, `6.0` | Backs the `django>=4.2` claim, or refutes it | `uv.lock` is single-resolution; needs `--resolution lowest-direct` or per-version constraint files, which cuts against the lockfile discipline `pyproject.toml:78-86` was written to protect (#118). Realistic middle path: one extra "oldest declared" job, not a full cross-product |
| **C. Concurrency tests** — two real connections contending on `get_next_offset_block` under `transaction=True` | The only way to test the offset/fencing invariants at all; would have to be Postgres-gated | Threaded DB tests are the flakiest thing in a suite; needs `transaction=True`, careful connection teardown, and a skip marker for SQLite |
| **D. `manage.py makemigrations --check django_rakaia` in CI** (`--check` implies `--dry-run`) | Catches model/migration drift before it reaches consumers | Near-zero. Cheapest item on this list |
| **E. A `tests/conftest.py`** with an autouse store-reset fixture, a `databases` default, and (optionally) a store-parameterized fixture | Removes the per-module copy-paste (Gap 8); makes "run the whole suite against the durable store" a flag rather than a rewrite | Small; the store-parameterization is the non-trivial half, because `clear()` is memory-only |
| **F. `django_assert_num_queries` on the two documented query-count claims** | Pins `append_many`'s batching and `skip_unchanged`'s trade (Gap 9) | Low; query counts are backend-sensitive, so assert `<=` bounds rather than exact numbers |
| **G. Test the documented ASGI mount** — build the composed app from `docs/django-integration.md` in `tests/.../asgi.py` and drive it | Takes `integration.py` off 0%; proves the prefix-stripping instruction users are given | Low |
| **H. `serialized_rollback=True` case** for the `raw=True` guard | Tests the guard against the mechanism its docstring names | Low, but `serialized_rollback` is slow and needs `TransactionTestCase` semantics |
| **I. `fail_under` on coverage; add `typecheck` to `just check`** | Stops silent erosion; makes the local gate match CI | Trivial |
| **J. One async hermeticity test** — arm `deny_database_access`, drive a store call through `run_sync`, assert it still trips | Settles Gap 10, the one place where a *safety guard* may be silently inert | Ten lines. The result is the finding either way |
| **K. A Channels-layer test that is not in-memory** (`ChannelsLiveServerTestCase`, or a `channels-redis` service) | Gap 7 — cross-process delivery, the reason the durable store exists, is currently only tested in the layer Channels documents as unable to do it | Needs a file-backed test DB ("You can't use an in-memory database for your live tests") and a Redis service; the `prod` extra already declares `channels-redis`, and `justfile:121` already has `redis-up` |

### For the integration code

| Option | Problem it addresses | Trade-off |
|---|---|---|
| **1. Move broadcast to `transaction.on_commit`** (`channels_signals.py:95`, `django_store.py:849`) | Gap 4 — a rolled-back append currently notifies subscribers. Django: "If the transaction is instead rolled back … the callback will be discarded, and never called", and "If you call `on_commit()` while there isn't an open transaction, the callback will be executed immediately" (<https://docs.djangoproject.com/en/5.2/topics/db/transactions/#performing-actions-after-commit>) — so the no-transaction case keeps today's behaviour for free | Changes observable timing; under the default `django_db` test mode "no transaction is ever actually committed, thus your `on_commit()` callbacks will never be run", so ~every existing broadcast test would need `captureOnCommitCallbacks` / `django_capture_on_commit_callbacks` or `transaction=True`. Non-trivial, and arguably the highest-value change on this list |
| **2. Forward `using` through `@stream_model`** — take `kwargs["using"]` in `handle_post_save`/`handle_post_delete`, thread it into `create_stream_event`, `transaction.atomic(using=…)`, and `write_enveloped_event` | Gap 5 — a non-default-alias save currently splits its write across two databases | Widens `create_stream_event`'s signature (public API, `__init__.py`); needs a router-configured test to be meaningful |
| **3. `--database` on `manage.py replay`** | Gap 5 — the operator-facing entry point cannot reach the seam the library's ADR 0003 story is built on | Small and additive |
| **4. Explicit backend capability check** — a startup check (`rakaia.W002`?) when `RAKAIA_STORE = "durable"` and `connection.features.has_select_for_update` is `False` | Makes Gap 1 visible to *deployments*, not just to CI. A SQLite production deployment of the durable store has no offset serialization and nothing says so | Another check to silence in dev; needs care not to fire during tests |
| **5. Close/return the DB connection in the SSE generator** after catch-up, before the indefinite `receive()` loop | Gap 7 — held connection per open EventSource | Needs verification that this is actually the behaviour under ASGI before changing anything; see open questions |
| **6. Mark `ProvenanceMiddleware` explicitly sync/async-capable** | Gap 7 — removes the ambiguity about `ContextVar` propagation under ASGI | Only worth doing once the behaviour is measured |
| **7. Give `deny_database_access` an alias-scoped escape** for the log's own reads | `hermeticity.py:81-92` and ADR 0003 both note the guard cannot be armed where the log lives on the guarded alias — which is the common single-database case | Design work; the row-count guard exists precisely because this is hard |

### The cheapest coherent bundle

If only four things are done: **D** (migration check), **G** (test the
documented mount), **E** (a real `conftest.py`) and **J** (the async hermeticity
test) are all small and all remove a class of silent failure — and **J** is the
only one that might turn up a live defect rather than a missing assertion.

If only one *large* thing is done, it is **A + C together** — the Postgres leg
*and* moving the locking tests to `transaction=True`. Five separate comments in
the source already say the suite cannot see what it needs to see, options **4**
and the meaning of `tests/server_store_contract.py:205` both depend on it, and
Django's own documentation says each half alone is insufficient.

---

## Open questions / what would change the answer

- **Does the sync `ProvenanceMiddleware` `ContextVar` survive Django's ASGI
  middleware adaptation?** Everything in Gap 7's third bullet is inference from
  reading `middleware.py` and `rakaia/context.py`. One test — append inside a
  view, under `AsyncClient`, assert the envelope `metadata` — settles it, and
  it may well already work.
- **Is `deny_database_access` inert across the `sync_to_async` boundary?** Gap
  10. Django documents the wrapper as installed "on the thread-local connection
  object"; the store deliberately runs its ORM work on another thread. This is
  the single question on this list whose answer could be a *live defect in a
  safety guard* rather than a missing test, and it is also the cheapest to
  settle.
- **Would the existing suite pass on Postgres unmodified?** Unknown. The
  contract suites are backend-agnostic by construction, but
  `test_django_store.py` and the `overlay` alias tests are not obviously so, and
  `:memory:` aliases have no Postgres equivalent. Running it once is the
  experiment; the failures are the finding.
- **Is Django 4.2 actually supported, and should it be?** The floor in
  `pyproject.toml:36` has never been installed, and 4.2 is no longer on Django's
  supported list at all. Three coherent answers: test it (a `4.2` matrix leg),
  raise the floor to `>=5.2` (the current LTS), or state the floor is
  best-effort. Raising it is a breaking change for consumers; leaving it as-is
  is a claim the project cannot back and Django no longer stands behind.
- **Is pre-commit broadcast a bug or a decision?** `django_store.py:847` frames
  it as consistency ("matching the receiver's existing timing"), not as a
  choice about at-most-once vs at-least-once delivery. `subscription.py:53-55`
  shows the project has thought carefully about at-least-once elsewhere. If the
  SSE channel is explicitly best-effort — a live tail that a client resumes via
  `Last-Event-ID` against the durable log (`channels_views.py:27-31`) — then
  pre-commit broadcast is defensible and should be *written down*, not fixed.
  That is the question to answer before touching option 1.
- **Does a second adopter deploy on anything but Postgres?** If the answer is
  "durable store is Postgres-only, forever", option 4 becomes a hard error
  rather than a warning, and Gap 1 gets simpler.

---

## Docs build

This file is **not** added to `zensical.toml`'s `nav`, for the reason
[`handler-types.md`](handler-types.md) records: research notes are not part of
the published manual, and zensical uses an explicit `nav`, so an un-navigated
page under `docs/` is simply not published rather than being an error.
