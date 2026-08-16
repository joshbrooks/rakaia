# How can this project's value best be demonstrated to a potential user?

**Status:** research notes, 2026-08-15. Not a decision — no ADR is implied by this
file, and nothing here changes source.

> **Correction, 2026-08-16.** This note repeatedly claims PyPI serves only
> `rakaia-streams 0.1.0`. **That is wrong.** 0.2.0 was published on
> 2026-08-15T15:58:51Z (wheel and sdist), verified against
> `https://pypi.org/pypi/rakaia-streams/json`. The note was written against a
> stale view of the index. Everything below that reasons from "PyPI is a version
> behind" — the "Problem 1 — you get 0.1.0" walkthrough, the `ImportError`
> failure mode in the friction table, and the first half of Option 1 — is void.
> The rest of Option 1 (publish the docs site, fix the 404ing URLs) still
> stands, and so does the observation that `partisipa-import` is *itself* still
> pinned to 0.1.0, which is a different problem with a different fix.

**Where this lives and why.** Same reasoning as
[`handler-types.md`](handler-types.md): `docs/` holds prose listed in
`zensical.toml`'s explicit `nav`, `docs/adr/` holds decisions, `okf/` holds the
machine-readable bundle. This is none of those — it is not a decision, not a
user-facing manual page, and not part of the concept bundle. It sits in
`docs/research/` so it is searchable and versioned alongside the code it argues
about, and it is deliberately **not added to `zensical.toml`'s nav**.

A note on method: every claim about rakaia below is either a `path:line` citation
or something I ran on this checkout at this commit, and the transcript is quoted.
Every claim about another project is a link to that project's own documentation.
Where I am inferring rather than reporting, it says **inference**.

---

## The question, and why now

rakaia is 101 commits old (first commit 2026-03-13), has one production consumer,
**zero GitHub stars, no repository description, and no homepage URL** (`gh repo
view`, 2026-08-15). It has 1,372 passing tests in 17 seconds, twelve runnable
examples, twenty-five docs pages, four ADRs, and an independent protocol
conformance harness. The engineering is far ahead of the presentation.

So the question is not "is there value" — it is "which single artifact would make
a stranger believe it in under ten minutes", and the honest answer depends on
which stranger.

---

## The headline finding, up front

**Three things, in this order.**

1. **The strongest demonstration already exists in the repo and is mislabelled.**
   `just partisipa-history-demo` proves, with assertions, that a rakaia stream
   reproduces a `pgh_event` audit table byte-for-byte, recovers the same peak
   snapshot pghistory recovery does, and survives re-replay. It is the only
   artifact in the project that argues against a named incumbent. It appears in
   the nav as **"pghistory retirement (spike)"** (`zensical.toml:35`), is absent
   from the four-example table on the front page (`docs/index.md:147-152`), and is
   absent from the top table of the guided tour (`docs/whats-new.md:46-51`). A
   reader following the documented "start here" path never sees it.

2. **The first five minutes are broken in a way nobody has run.** The README
   quickstart (`README.md:30-42`) ends at `uvicorn rakaia:app`. The obvious next
   two commands — `PUT` a stream, `POST` an event — return **`409 Conflict:
   Content-type mismatch`** on a clean install, because the `PUT` must carry the
   content type the later `POST` will use. That is documented nowhere in the
   quickstart. Worse: **PyPI serves 0.1.0 while this repo is 0.2.0**
   (`pyproject.toml:6`), so `pip install rakaia-streams` gives a version that
   predates `django_rakaia.envelope`, the public-API tiers, and most of what the
   docs describe.

3. **The sharpest differentiator is not event sourcing — it is the rebuild
   gate.** Nothing else in the Python or Django field lets you replay a log into a
   throwaway database, *prove* the replay read nothing but the log and wrote
   nothing live, and diff the result against production before you trust it. Every
   competitor surveyed below has zero of the three. That is the argument that
   answers the actual objection to adopting event sourcing, which is not "is it
   elegant" but "how do I know the rebuild is right".

---

## Who the plausible user actually is

The docs are unusually forthcoming about this, because they were written
alongside a real adopter rather than for an imagined one.

### The evidenced user: a Django team with a load-bearing audit log

The consumer is `partisipa-import`, named directly in the docs
(`docs/versioned-handlers.md:13-18`, `docs/adr/0004-handler-types-and-fold-order.md:12`,
`docs/research/handler-types.md:28`). What we know about it from primary sources
in this repo:

| Evidence | Citation |
|---|---|
| It accumulated **178 imports and 84 direct database queries** against rakaia before the public API existed | `docs/public-api.md:11-13` |
| Its `Submission` table is tracked by `django-pghistory`, with three live consumers: a `/history` audit API, per-change actor context, and a `repair_blank_save_dataloss` recovery command | `docs/pghistory-retirement.md:14-21` |
| It carried a "re-save the row so newer signals fire" backfill pattern (`_task_sf12_backfill_project_ids`, `_task_ff4_backfill`) — the workaround versioned handlers replace | `docs/versioned-handlers.md:13-18` |
| It hit four distinct seam failures in one rebuild command: a dormant upcaster, a `merge_replay` with no `ts` to order on, a hand-rolled `pgh_id` watermark, and stale internal docs | `docs/adr/0002-framework-vs-protocol-server-boundary.md:39-52` |
| A shared projection helper silently read the **live** database from inside a rebuild that claimed to be log-only | `docs/adr/0003-handler-hermeticity.md:38-45` |
| It is still pinned to PyPI `rakaia-streams 0.1.0` and carries a duplicate of an upstreamed helper because of it | `docs/research/handler-types.md:280-283` |
| Adoption is still open as issue #11, "Partisipa still has to adopt the streams work we finished" | `gh issue list` |

That is a very specific person: **a Django team, several years in, with forms or
submissions, an audit trail somebody depends on, and a folder of backfill scripts
they are ashamed of.** They are not shopping for an event store. They have a
concrete pain — "our history table and our current table disagree", "we cannot
re-run last year's rules", "our backfill re-saved 40,000 rows and we do not know
what it wrote".

Every one of those failures has a rakaia feature named after it, which is the
strongest evidence that this is who the library is for.

### The user the front door addresses

Not that person. `README.md:3` opens with:

> **Rakaia** is a Python implementation of the Durable Streams protocol — an
> HTTP-based protocol for append-only, ordered, durable byte streams.

`docs/index.md:7` is the same sentence. The Django integration is introduced as
the *second* of two packages (`README.md:8-12`). The word "audit" does not appear
in the README; "pghistory" does not appear in the README; "replay" first appears
at `README.md:91`, past the fold on any screen.

**Inference:** the README describes what the code *is* (a protocol
implementation) rather than what it *fixes* (a Django team's audit and backfill
problem). Those are two different products with two different buyers, and the
repo currently leads with the one that has the smaller, more sophisticated
audience.

### A third, real but thin, user

There is genuinely no Durable Streams server implementation in Python. Upstream
lists a Node reference server, a Caddy plugin, a Cloudflare server, and two
community servers (Go, Java) — no Python
(https://github.com/durable-streams/durable-streams). rakaia is therefore the
only way to serve this protocol from a Python process, which is a real and
uncontested niche.

The niche is also nine months old. The spec is a self-published **DRAFT** from a
single vendor: `docs/protocol.md:1-8` reproduces its own header —

> `# DRAFT: The Durable Streams Protocol` … **Date:** 2025-01-XX … **Author:**
> ElectricSQL

— with the date still an unfilled placeholder. The conformance package is at
`0.3.6`. ElectricSQL announced it 2025-12-09
(https://electric-sql.com/blog/2025/12/09/announcing-durable-streams).

**Inference:** the protocol audience is real but small and speculative today; the
Django audience is small but *evidenced*. Weighting the demonstration toward the
evidenced one is the lower-variance bet.

---

## What is genuinely differentiated

All rows are from the project's own documentation. Nothing here is sourced from a
comparison post.

### The field

| | Append-only log | Rebuild by replay | Time-correct (versioned) handlers | Schema evolution | Live stream to a client | HTTP wire protocol | Dry-run rebuild |
|---|---|---|---|---|---|---|---|
| **rakaia** | yes (`StreamStore` / `DjangoStreamStore`) | yes (`replay`, `merge_replay`) | **yes** (`effective_from`/`effective_to`) | yes (upcasters) | yes (SSE via Channels) | **yes** (Durable Streams) | **yes** (`CollectingExecutor`) |
| **eventsourcing** (pyeventsourcing) | yes | yes | no — `upcast_vX_vY` migrates *data forward* to one current handler | yes | no (Python iterator; Postgres/POPO only) | no | no |
| **eventsourcing-django** | yes (storage adapter only) | inherits | no | inherits | no | no | no |
| **django-eventstream** | no — 24-hour buffer | no | no | no | yes (SSE) | yes (SSE) | no |
| **django-pghistory** | yes (row snapshots, trigger-enforced) | no — per-object `revert` only | no | no | no | no | no |
| **django-simple-history** | yes, but bypassable | no — `as_of` + revert-by-save | no | no | no | no | no |
| **KurrentDB / EventStoreDB** | yes | yes | no | no | yes (subscriptions) | yes (gRPC) | no |

Sources, in order: https://eventsourcing.readthedocs.io/en/stable/topics/domain.html#versioning ·
https://github.com/pyeventsourcing/eventsourcing-django ·
https://github.com/fanout/django-eventstream ·
https://django-pghistory.readthedocs.io/en/stable/basics/ and
https://django-pghistory.readthedocs.io/en/stable/reversion/ ·
https://django-simple-history.readthedocs.io/en/latest/common_issues.html ·
https://docs.kurrent.io/server/v25.0/features/projections/

### Four claims that survive contact with the primary sources

**1. Time-correct handlers are unique.** The nearest thing in the field is
`eventsourcing`'s upcasting: *"Static methods of the form `upcast_vX_vY()` will be
called to update the state of a stored aggregate event or snapshot from a lower
version X to the next higher version Y"*
(https://eventsourcing.readthedocs.io/en/stable/topics/domain.html#versioning).
That is the opposite move — migrate the old *data* into the shape today's single
handler expects. rakaia keeps every historical *rule* in source and dispatches by
seq range (`docs/whats-new.md:68-105`), so an event from before a tax change still
gets the pre-change answer. The distinction is easy to state and easy to
demonstrate, and no surveyed project offers it.

Rakaia also has upcasters *as well*, for the schema-shape case
(`docs/whats-new.md:113-130`) — so it is a superset, not a substitute.

**2. The rebuild gate is unique, and is the strongest argument.** Three primitives
compose into something no competitor has:

- `deny_database_access(*aliases)` — "Raise `AmbientDatabaseAccess` on any query
  to `aliases` in the block" (`src/django_rakaia/hermeticity.py:80`)
- `assert_no_live_writes(...)` — raises `LiveWriteLeaked` if a guarded model's row
  count moves (`src/django_rakaia/hermeticity.py:155,68-70`)
- `diff_effects_against_rows(...)` → a `DiffReport` with a `verdict` that
  distinguishes `GREEN` from `VACUOUS`, and a `VacuousVerification` error for a
  sweep that compared nothing (`src/django_rakaia/verification.py:110,210,408`;
  narrated at `docs/whats-new.md:348-381`)

ADR 0003 states the motivating incident plainly: a shared helper computed a
dangling-FK check with an un-aliased `.objects.filter(...).exists()` *from inside
a rebuild running under `using="rebuild"`* — "the rebuild silently consults
production, so its 'clean rebuild' verdict is not actually log-only"
(`docs/adr/0003-handler-hermeticity.md:38-45`).

Nothing in the Python/Django field addresses this. pghistory's revert
`RuntimeError`s rather than degrade
(https://django-pghistory.readthedocs.io/en/stable/reversion/); simple-history's
revert is a plain `.save()` that mutates production immediately
(https://django-simple-history.readthedocs.io/en/latest/quick_start.html). Neither
has a dry run.

**3. Against the two Django audit libraries, the differentiator is *derivation*,
not history.** Both incumbents store **row snapshots**, not domain events —
pghistory's own basics page defines an event as "a historical version of a model"
(https://django-pghistory.readthedocs.io/en/stable/basics/). They can tell you
what the row looked like; they cannot recompute what it *should* look like under a
corrected rule. rakaia inverts the arrow: the table is the derived thing
(`docs/glossary.md:67-71`).

The counter-argument is worth stating because a competent evaluator will make it:
**pghistory's trigger-based capture is strictly more reliable than anything
signal- or ORM-based**, by its own account — capture "includes bulk methods and
even changes that happen in raw SQL"
(https://django-pghistory.readthedocs.io/en/stable/). rakaia's `@stream_model`
sits in the ORM, so it inherits exactly the bypass hole django-simple-history
documents against itself: "for certain bulk operations, such as `bulk_create`,
`bulk_update`, and queryset updates, signals are not sent, and the history is not
saved automatically"
(https://django-simple-history.readthedocs.io/en/latest/common_issues.html). Any
honest before/after must concede this, and the honest rakaia answer is that the
log is *the write path*, not a mirror of one — which is a bigger architectural
commitment than dropping in a decorator, and should be sold as such.

**4. Log-plus-wire-protocol in one package is unoccupied.** `eventsourcing` has
the log and no protocol — its interface page still carries the banner "this page
is under development — please check back soon"
(https://eventsourcing.readthedocs.io/en/stable/topics/interface.html).
`django-eventstream` has the protocol (SSE) and a 24-hour buffer, not a log: "When
storage is enabled, events are written to the database before they are published,
and they persist for 24 hours"
(https://github.com/fanout/django-eventstream). KurrentDB has both but is a
separate server you deploy, under a source-available licence with a hosted-service
restriction (https://github.com/kurrent-io/KurrentDB/blob/master/LICENSE.md) and a
license key for some features
(https://docs.kurrent.io/server/v25.0/quick-start/installation.html). rakaia is
MIT, `pip install`, and runs inside the Django process you already have.

**Inference:** this is the most *interesting* differentiator and the least
*persuasive* one, because the buyer who wants it does not exist in quantity yet.

### One claim that needs softening

`README.md:186-188` says rakaia "passes the full protocol surface today except the
stream **forking** family". Measured on this checkout:

```
276 passed, 56 failed, 6 skipped (of 338)
0 NEW failure(s) · 0 newly passing · 56 expected (known gap)
```

The statement is true — all 56 failures are the baselined forking family
(`conformance/expected-failures.txt`, 56 non-comment lines) — but "except forking"
reads as a footnote when it is 17% of the suite. **A reader who runs
`just conformance` and sees `56 failed` before reading the regression summary will
draw the wrong conclusion.** Fifteen words in the README fixes it, and the honest
framing is stronger than the current one: *276 of 338 upstream conformance tests,
with the entire remaining gap tracked as one named family (#61).*

---

## What a newcomer hits in the first five minutes today

Everything in this section was run against this checkout on 2026-08-15.

### Path A — the README quickstart, on a clean machine

`README.md:30-42` and `docs/index.md:52-70` both offer:

```bash
pip install rakaia-streams uvicorn
uvicorn rakaia:app --port 4437
```

I ran exactly that in a fresh venv:

```
rakaia-streams 0.1.0
INFO:     Uvicorn running on http://127.0.0.1:4437
```

**Problem 1 — you get 0.1.0.** PyPI has one release; `pyproject.toml:6` says
`version = "0.2.0"`. So the installed package predates `django_rakaia.envelope`
(`append_event` / `fold_events`, `docs/whats-new.md:385-405`), the Tier-1/2/3
public API contract (`docs/public-api.md`), and the rename of `SCRATCH_PATH`. This
is not speculative: it is already the reason the production consumer carries a
duplicate helper (`docs/research/handler-types.md:280-283`). **Every code sample
in the docs is written against a version a stranger cannot install.**

**Problem 2 — the quickstart stops before anything happens.** It starts a server
and says nothing about how to use it. The obvious next moves fail:

```
$ curl -X PUT  http://127.0.0.1:4437/hello                                  → 201
$ curl -X POST http://127.0.0.1:4437/hello -H 'Content-Type: application/json' -d '{"a":1}'
409 Conflict
Content-type mismatch
```

The `PUT` with no body created the stream as `application/octet-stream`; the
`POST` declared JSON; the server correctly refused. The working sequence is:

```
$ curl -X PUT  http://127.0.0.1:4437/h2 -H 'Content-Type: application/json'   → 201
$ curl -X POST http://127.0.0.1:4437/h2 -H 'Content-Type: application/json' -d '{"a":1}'  → 204
$ curl -X POST http://127.0.0.1:4437/h2 -H 'Content-Type: application/json' -d '{"a":2}'  → 204
$ curl        http://127.0.0.1:4437/h2
[{"a":1},{"a":2}]
```

Four lines. They are not in the README, not in `docs/index.md`, and not in
`docs/protocol.md`'s quickstart (there isn't one). **The single highest
value-per-byte edit available to this project is pasting those four lines into
`README.md:36`.** The payoff line — `[{"a":1},{"a":2}]` — is a durable append-only
log answering a plain `curl`, which is the whole pitch in one screenful.

### Path B — the guided tour, in a git checkout

`docs/index.md:20-24` says "New here? Start with the guided tour of what's new",
and `docs/whats-new.md:37-40` says:

```bash
just install     # sync all dependency groups
just demo        # runs the scripted demos end-to-end, with narration
```

This works, and works well. `just orders-demo` completed in **1.9 seconds** with
assertion-backed output ending `Replayed again: 6 -> 6 rows — idempotent ✓`.
`just cookbook-demo` printed five numbered checks including
`[3] vacuous green: a sweep that compared 0 rows reports VACUOUS and refuses to
certify ✓`. The demos are genuinely excellent, and `docs/examples.md:196-202`
("Assertion-backed … there is no 'looks right' path") is an accurate description
of them.

Three frictions:

- **`just` is a hard prerequisite for every published command.** Every table in
  every doc gives the run instruction as `just <name>-demo`. `README.md:149`
  hedges ("If you have `just` and `podman`") but that hedge is under *Running it*,
  after the sample-application table that already used `just`. The direct form
  (`cd examples/orders && uv run python manage.py demo_orders`) is mentioned once,
  in passing, at `docs/examples.md:197-199`.
- **`just install` syncs four extras** (`justfile:54`: `dev`, `django`, `docs`,
  `prod`) — 58 packages including `daphne`, `channels-redis`, `hypercorn`,
  `whitenoise`, and `zensical` — to run a demo that needs none of them.
- **The demo you most want is not in the tour.** `just demo` runs `orders` then
  `formkit_submissions` (`justfile:62-80`). `partisipa_history` — the pghistory
  parity proof — is not in it.

### Path C — the docs site

There isn't one. `/site/` is gitignored, there is no Pages workflow (only
`ci.yml`, `conformance.yml`, `publish.yml`), and `zensical.toml:12` sets
`site_url = "https://github.com/joshbrooks/durable-streams"` — which **404s**, as
does the social link at `zensical.toml:88`. The repo is `joshbrooks/rakaia`.

So the twenty-five-page documentation set, which is the project's largest asset,
is readable only as GitHub Markdown by someone who has already found the repo.

### What the nav says about the project

`zensical.toml:19-53` lists 22 top-level entries. Five are labelled **"(spike)"**
— pghistory retirement, staged replay, close preconditions, multi-stream merge,
tree-reconcile — and one "(example)". Nearly a third of the published manual
announces itself as provisional, and one of the five is the best demonstration in
the repo.

Reading it against Diátaxis (https://diataxis.fr/), which splits documentation
into tutorials, how-to guides, reference, and explanation
(https://diataxis.fr/foundations/): rakaia has excellent **reference**
(`public-api.md`, `protocol.md`, `glossary.md`), excellent **explanation** (the
ADRs, `framework-vs-protocol-server.md`), decent **how-to** (`django-integration.md`,
`deployment.md`) — and **no tutorial at all**. Diátaxis names this exact pattern:

> Tutorials are rarely done well, partly because they are genuinely difficult to
> do well, and partly because they are not well understood. In software, many
> products lack good tutorials, or lack tutorials completely; tutorials are often
> conflated with how-to guides. (https://diataxis.fr/tutorials/)

`whats-new.md` is the closest candidate and is not one: it is a feature tour
organised by *what shipped recently*, whose reader is assumed to already want the
features. A tutorial in the Diátaxis sense is "a practical activity, in which the
student learns by doing something meaningful, towards some achievable goal… Its
purpose is not to help the user get something done, but to help them learn"
(https://diataxis.fr/tutorials/), and its craft rules — "deliver visible results
early and often", "ruthlessly minimise explanation" — are precisely what the
current front door does not do.

### Where a newcomer is lost, ranked

| # | Loss point | Evidence | Recoverable by |
|---|---|---|---|
| 1 | Installs 0.1.0, follows 0.2.0 docs, hits `ImportError` | PyPI vs `pyproject.toml:6` | a release |
| 2 | Two curls after the quickstart return `409 Content-type mismatch` | run above | four lines in `README.md` |
| 3 | Reads a protocol tagline, is a Django person with an audit problem | `README.md:3` | a second sentence |
| 4 | Cannot find the docs site; `site_url` 404s | `zensical.toml:12,88` | a Pages workflow |
| 5 | Must install `just` before any documented demo | every docs table | one direct-invocation line per table |
| 6 | Never sees the pghistory demo | `docs/index.md:147-152`, `zensical.toml:35` | promote it |
| 7 | Sees `56 failed` from `just conformance` | run above | reword `README.md:186-188` |

Items 1–4 are not documentation problems. They are **distribution** problems, and
no amount of new demo material routes around them.

---

## Demonstration options

Seven options. Each states what it proves, who it converts, what it costs, and
what it risks. They are not exclusive, and roughly half are prerequisites for the
other half mattering.

### Option 1 — Ship the thing (release 0.2.0; publish the docs; fix the URLs)

**Proves:** nothing. **Enables:** everything else.

- Tag and publish 0.2.0 (`publish.yml` already exists and checks the tag against
  the project version).
- Add a Pages job for `zensical build`; fix `site_url` and the social link
  (`zensical.toml:12,88`).
- Set the GitHub repo description and homepage — both are currently empty.

**Cost:** hours. **Risk:** near zero, except that publishing commits you to the
Tier-1 surface in `docs/public-api.md` — which is the point of having written it.

**Note:** `docs/public-api.md:104` already tells adopters to pin
`rakaia-streams>=0.2,<0.3`, a version that does not exist on PyPI. That is the
clearest evidence the release is overdue.

### Option 2 — A 60-second curl tutorial in the README

**Proves:** the protocol layer works, immediately, with no Python and no Django.
**Converts:** the protocol-curious, and every skimmer.

The four curl lines from Path B above, plus the `[{"a":1},{"a":2}]` payoff, placed
at `README.md:36`. Optionally a fifth line showing SSE tailing.

**Cost:** under an hour, including a test that runs it in CI so it cannot rot.
**Risk:** essentially none. It is also the only option that helps *all three*
candidate users.

This is the highest ratio of value to cost on the list, and it should probably
happen regardless of what else does.

### Option 3 — Promote and harden the pghistory before/after

**Proves:** rakaia replaces a named incumbent, with byte-for-byte parity, on the
incumbent's own ground. **Converts:** the evidenced user, directly.

Three moves of increasing cost:

1. **Relabel and promote** (hours). Drop "(spike)" from `zensical.toml:35`; add
   `partisipa_history` to `docs/index.md:147-152` and `docs/whats-new.md:46-51`;
   add it to `just demo` (`justfile:62`).
2. **Run against real pghistory** (a day or two). Today the parity target is
   `PghEventGolden`, a hand-written model — its own docstring says "Not a live
   pghistory instance; a golden reference the stream must reproduce"
   (`examples/partisipa_history/history/models.py:58-65`), and
   `docs/pghistory-retirement.md:115-117` repeats the caveat. A sceptical
   evaluator will find that in thirty seconds and discount the whole demo.
   Installing `django-pghistory` as an example-only dependency and diffing against
   its real `pgh_event` table turns "we modelled their output" into "we matched
   their output". Note pghistory requires Postgres
   (https://django-pghistory.readthedocs.io/en/stable/), so the example would need
   a Postgres service — a real cost, and the reason to keep it example-scoped.
3. **Write the migration guide** (a week). A how-to: what you keep, what you
   delete, what breaks. `docs/pghistory-retirement.md:107-113` already sketches
   the three-step path.

**Risk, and it is real.** A head-to-head invites the comparison rakaia loses:
pghistory captures at the database, "includes bulk methods and even changes that
happen in raw SQL" (https://django-pghistory.readthedocs.io/en/stable/), whereas
`@stream_model` is ORM-level and inherits the bulk-operation blind spot
django-simple-history documents against itself
(https://django-simple-history.readthedocs.io/en/latest/common_issues.html).
**Conceding this up front is stronger than being caught by it** — the honest frame
is that rakaia is not a better audit *mirror*, it is a different write path, and
you adopt it when you want the tables *derived*, not when you want them watched.

### Option 4 — One worked application instead of twelve feature demos

**Proves:** rakaia composes into something whole. **Converts:** the evaluator who
believes each feature and doubts the sum.

Today `examples/` has twelve projects, each proving one concept
(`docs/examples.md:71-101`), and `docs/examples.md:167-176` honestly lists four
public APIs with no example at all. What does not exist is one application a
reader recognises — with forms, an audit page, a rule that changed last quarter,
and a rebuild command — where the reader can break a rule, replay, and watch the
tables converge.

**Cost:** the largest on this list — one to three weeks, plus permanent
maintenance in `just demos` (`justfile:90`).
**Risk:** high. A thirteenth example dilutes rather than concentrates unless it
*replaces* several. And the twelve existing demos are a genuine asset —
assertion-backed, CI-gated, fast — so this is a restructuring, not an addition.

**Inference:** worth doing eventually; wrong to do before options 1–3.

### Option 5 — A benchmark

**Proves:** replay is fast enough. **Converts:** almost nobody.

**Cost:** days, plus permanent flakiness in CI.
**Risk:** high and asymmetric. The competitors are not competing on throughput —
pghistory is a Postgres trigger and will win any write-path microbenchmark by
construction; KurrentDB is a purpose-built database. rakaia's claim is
*correctness under change*, and a benchmark answers a question nobody asked while
inviting one that goes badly.

The one number that *would* help is not a benchmark but a scale statement: how
long a replay of N events takes on the production consumer's real log. That is one
sentence of evidence, not a harness. **Evidence for that number does not exist in
this repo** — flagged under open questions.

### Option 6 — A tutorial (Diátaxis sense), not another explanation

**Proves:** a stranger can succeed. **Converts:** the evidenced user, at the point
where they are deciding whether to spend an afternoon.

One page, one goal, guaranteed success: start from an ordinary Django model with
no streams; emit events; replay into a projection; change a rule; replay again;
watch the old events keep their old answer. Ninety per cent of it already exists
inside `examples/orders`; what is missing is the narrative frame and the promise
that following it works.

Diátaxis's craft rules apply directly: "deliver visible results early and often",
"maintain a narrative of the expected", "ruthlessly minimise explanation"
(https://diataxis.fr/tutorials/) — the last being the discipline this project's
docs would find hardest, since their instinct is to explain, and they explain very
well.

**Cost:** two to four days including a CI test that executes it.
**Risk:** low, but it is wasted effort while option 1 is outstanding — a tutorial
whose first line is `pip install rakaia-streams` currently installs 0.1.0 and
fails at step three.

### Option 7 — A full Diátaxis restructure of the nav

**Proves:** nothing on its own. **Converts:** by removing friction rather than
adding evidence.

Split `zensical.toml`'s 22 flat entries into Tutorial / How-to / Reference /
Explanation, move the five "(spike)" pages and the four ADRs under Explanation,
and stop leading the manual with provisional material.

**Cost:** a day of moving files, plus every existing inbound link and every
`docs/*.md` cross-reference.
**Risk:** moderate — churn with no user-visible new capability, and the current
nav, while flat, is at least honest. The mapping is also imperfect: `whats-new.md`
is a genuine fifth thing (a release-oriented tour) that Diátaxis has no slot for,
and forcing it into one would lose it.

**Inference:** do the cheap 80% — a tutorial section at the top (option 6),
"(spike)" pages demoted below the fold — and skip the taxonomy purity.

### Ranked, with the reasoning rather than the verdict

| | Option | Cost | Risk | Converts |
|---|---|---|---|---|
| 1 | Ship 0.2.0 + publish docs + fix URLs | hours | none | prerequisite for all |
| 2 | Four curl lines in the README | <1 hour | none | everyone |
| 3a | Relabel/promote the pghistory demo | hours | none | the evidenced user |
| 3b | Real pghistory in the example | 1–2 days | low | the sceptic |
| 6 | A tutorial | 2–4 days | low | the evaluator |
| 3c | Migration guide | ~1 week | low | the committed adopter |
| 7 | Nav restructure | ~1 day | moderate | nobody directly |
| 4 | One worked application | 1–3 weeks | high | the doubter of the sum |
| 5 | Benchmark | days | high | nobody |

The shape of that table is the real finding: **the four cheapest items are all
distribution and framing, and none of them is a demo.** The project's problem is
not that it lacks proof. It has more proof than most libraries ten times its size.
The proof is unshipped, unpublished, mislabelled, and behind a tagline aimed at a
different reader.

---

## What NOT to do

Each was considered and rejected; the reason matters more than the verdict.

1. **Do not write more explanation.** `docs/` is 25 pages, 5,600 lines, and the
   ADRs are excellent. Adding a page has never been this project's bottleneck.
   `docs/whats-new.md` is already 449 lines of "here is why this is good", which
   is roughly the length at which an evaluator stops reading.

2. **Do not lead with the protocol.** It is the most technically distinctive thing
   here and the least evidenced demand. The spec is a vendor DRAFT with a
   placeholder date (`docs/protocol.md:1-8`), the conformance tooling is `0.3.6`,
   and there are five server implementations in the world. Keep it as the second
   sentence and the reason the library is trustworthy; do not make it the reason
   to install.

3. **Do not benchmark against pghistory.** See option 5. It is a trigger; it wins;
   the comparison is not about speed.

4. **Do not build a thirteenth example.** `docs/examples.md:167-176` already lists
   four public APIs with no example (`register_reducer`, `reconcile_tree`,
   `replay_stream`, `DjangoExecutor(skip_unchanged=True)`). Filling those gaps is
   maintenance, not demonstration, and doing it will not move a single evaluator.

5. **Do not vendor a newer copy of the upstream spec into `docs/protocol.md`
   as a demonstration move.** The copy is stale — 1,043 lines and 12 sections
   here, against ~1,560 lines and 13 sections upstream (forking as §4.2, Reserved
   Subscription APIs, webhook signatures, IANA header registrations). That is
   worth *fixing*, and it explains why the forking family is the conformance gap
   (#61) — but it is a correctness chore, not a way to demonstrate value, and it
   should not compete for the same week.

6. **Do not rename or reposition the library.** "rakaia" is fine; the distribution
   rename to `rakaia-streams` is already documented (`README.md:22-28`,
   `UPGRADING.md`). A second identity change would cost the one thing the project
   has accumulated, which is internal consistency.

---

## Open questions / where the evidence is thin

- **We have one user and no user research.** Everything in "who the plausible user
  is" is inferred from one consumer's failure modes as recorded in this repo's own
  ADRs. That consumer is the author's other project. It may be representative; we
  have no evidence either way, and `partisipa-import` is not in this checkout, so
  even its side of the story is second-hand here.

- **No scale evidence exists.** Nothing in this repo states how many events a real
  replay covers, how long it takes, or how large the durable store gets. The demos
  seed six events. Issue #138 ("Our stream positions have a size limit we chose by
  hand") suggests the limits are chosen rather than measured. A single honest
  sentence — "the production log is N events; a full rebuild takes T" — would be
  worth more than option 5's entire harness, and **cannot currently be written.**

- **Is the pghistory story the *general* pitch, or one adopter's pitch?** It is
  overwhelmingly the sharpest artifact in the repo, and it is entirely derived from
  `partisipa-import`'s specific situation (`docs/pghistory-retirement.md:14-21`).
  Whether "retire your audit library" generalises to other Django teams, or whether
  most of them are happy with pghistory and would read the pitch as a solution to a
  problem they do not have, is unknown. A second adopter would settle it — the same
  trigger `handler-types.md` names for the edge-primitive question.

- **Does the ORM-level capture hole matter in practice?** Section "one claim that
  needs softening" argues it must be conceded. Whether it is *disqualifying* for
  the target user — a team that does bulk imports, which by the sound of
  `partisipa-import` is exactly what that team does — is not answered anywhere in
  this repo, and it is the single most likely reason a well-informed evaluator
  bounces. Worth a documented answer before option 3b invites the question.

- **Zensical is alpha.** Its own about page says "While currently in alpha, it's
  already compatible with Material for MkDocs" (https://zensical.org/about/), and
  `ci.yml` gates on `zensical build`. Committing to a published docs site (option
  1) inherits that. Probably fine; worth knowing it is a choice.

- **Would a released 0.2.0 actually be adopted?** Issue #11 has been open since the
  early days and `docs/research/handler-types.md:280-283` records the consumer
  still pinned to 0.1.0. If the one user who wants this most has not upgraded, the
  release may not be the unlock this note assumes it is — or the release may be
  precisely what is blocking them. Nothing in this repo distinguishes those.

---

## Docs build

`uv run zensical build` is part of the CI gate (`.github/workflows/ci.yml:51`) and
uses an explicit `nav`, so a file absent from `zensical.toml`'s `nav` is simply not
published — it neither breaks nor warns. This file is **not** added to the nav, on
the same reasoning as `handler-types.md`: research notes are not part of the
published manual, and a `docs/research/` directory of ten of them would not survive
being in it.
