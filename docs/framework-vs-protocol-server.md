# Framework vs. protocol server — the package boundary

rakaia is **two products in one repository**, and knowing which one you are
using tells you what you have to install, what you can extend, and what will
break if a dependency is missing. This page draws that boundary and gives the
**"what needs Django / what is pure"** matrix. For the *why* — the seam analysis
and the decision to formalize it rather than split the package — see
[ADR&nbsp;0002](adr/0002-framework-vs-protocol-server-boundary.md).

## The two tiers

- **Tier 1 — the event-sourcing framework.** `replay` / `merge_replay`, the
  handler·upcaster·reducer registries, `Effect` / `Executor`, projections, and
  the `ReadableStore` / `WritableStore` / `CursorStore` / `ProjectionReader`
  protocols. Pure, deterministic, dependency-inverted. This is the layer you
  build on.
- **Tier 2 — the Durable Streams protocol server.** The raw ASGI
  `handler` (PUT/POST/GET/HEAD), producer epoch/seq fencing, CDN cursors, SSE,
  TTL, and the in-memory `StreamStore` lifecycle. A wire-protocol implementation.

Both tiers live in the **`rakaia`** package and share `StreamMessage`,
`AppendOptions`, and the store object — so the boundary is a **convention
enforced by layering discipline and this doc, not by packaging** (ADR&nbsp;0002,
*Consequences*). The **`django_rakaia`** package adds a Django-backed durable
store, projection execution, a DB-backed protocol server, and SSE.

## What needs Django / what is pure

| Capability | Where it lives | Requires |
|---|---|---|
| Event-sourcing framework — `replay`, `merge_replay`, registries, upcasters, effects, projections, dry-run executors | `rakaia` (Tier 1) | **Python stdlib only** |
| Standalone protocol server — ASGI PUT/POST/GET/HEAD, SSE, producer fencing, TTL, in-memory `StreamStore` | `rakaia` (Tier 2: `rakaia.handler`) | **Python stdlib only** (plus any ASGI server to run it) |
| Subscriber cursors — incremental per-consumer reads with rewind detection | `rakaia.subscription` (Tier 1) | **Python stdlib only** |
| Durable event store — survives restarts; `replay()` across processes | `django_rakaia.django_store` (`DjangoStreamStore`) | **Django** (ORM) |
| Emit events from your models — `@stream_model` | `django_rakaia.decorators` | **Django** (ORM) |
| Replay into DB tables — `effect_executor`, `projection_reader` | `django_rakaia` | **Django** (ORM) |
| Provenance / actor capture on append | `django_rakaia.middleware` | **Django** |
| Durable protocol HTTP server (DB-backed) | `django_rakaia.protocol_views` | **Django** (ORM) |
| Durable subscriber cursors — persisted watermarks | `django_rakaia.subscription` (`ConsumerCursor`) | **Django** (ORM) |
| Admin browsing of streams/events | `django_rakaia.admin` | **Django** |
| Real-time SSE broadcast | `django_rakaia.channels_signals`, `channels_views` | **Django + `channels`/`daphne`** — *optional*, see below |

The load-bearing line: **everything in `rakaia` is stdlib-only** (the package
declares `dependencies = []`), and within `django_rakaia` **only SSE needs
`channels`**. A projections-only consumer never touches `channels`.

## Install profiles

```bash
# 1. Framework + standalone protocol server, zero dependencies
pip install rakaia-streams

# 2. Django durable store + projections + DB-backed protocol server (no SSE)
pip install rakaia-streams django            # NB: not the [django] extra — see below

# 3. Everything, including real-time SSE
pip install "rakaia-streams[django]"         # pulls django + channels + daphne
```

!!! note "The `[django]` extra currently bundles `channels`+`daphne`"
    `rakaia-streams[django]` installs `django`, `channels`, **and** `daphne`
    together, so profile 2 (framework-tier Django use with no SSE) is expressed
    by installing `rakaia-streams` + `django` directly rather than via the extra. The **runtime** import
    of `channels` is already optional (below); splitting the extra into
    `[django]` (ORM only) and `[sse]` (`channels`+`daphne`) so the *install* is
    also minimal is a natural follow-up (tracked under #41).

## Keeping `channels` optional at runtime

`DjangoRakaiaConfig.ready()` separates its two tiers so a framework-only
consumer can boot without `channels` installed:

- **Framework tier** — handler/upcaster autodiscovery (`autodiscover()`) always
  runs and has no `channels` dependency.
- **Protocol tier** — SSE signal wiring is gated by an optional
  `RAKAIA_ENABLE_SSE` setting:

| `RAKAIA_ENABLE_SSE` | `channels` installed | Behaviour |
|---|---|---|
| *unset* (default) | yes | SSE signals wired (today's behaviour, unchanged) |
| *unset* (default) | no | skipped silently — framework-only consumer boots clean |
| `True` | no | `ImportError` raised — you asked for SSE but didn't install the extra |
| `False` | either | never wired |

So framework-tier consumers get no `ModuleNotFoundError: channels` at app load,
existing SSE consumers are unaffected, and a misconfiguration (opted into SSE
without the dependency) still fails loudly.

## The store seams

The framework reads and writes through **exported, conformance-tested
protocols** (`rakaia.__all__`): `ReadableStore`, `WritableStore`, `CursorStore`,
and `ProjectionReader`. "Test on the in-memory store, ship on the durable store"
is safe because both `StreamStore` and `DjangoStreamStore` pass one shared
[conformance suite](../tests/store_contract.py) (#36).

Two divergences between the stores are **intentional and pinned**, not gaps:

- **Offset *format* is backend-specific.** The in-memory store emits
  `{seq}_{byte}`; the durable store emits a zero-padded integer. The protocol
  mandates opacity, not one format (§6). What *is* contract — and tested on both
  — is offset *behaviour*: opaque, lexicographically sortable, strictly
  increasing (#49, #55).
- **`DjangoStreamStore` implements the framework store surface, not the
  protocol-server one.** It satisfies `WritableStore` + `CursorStore`
  (`create` / `append` / `read` / `has` / `get_current_offset`, plus the
  `get` / `delete` / `list_paths` conveniences), but **not** the Tier-2
  concerns — producer epoch/seq dedup, stream close, TTL, and long-poll are
  deliberately absent, so a framework consumer must not depend on them from the
  durable store.

## See also

- [ADR&nbsp;0002](adr/0002-framework-vs-protocol-server-boundary.md) — the seam
  analysis and the decision behind this boundary.
- [Django integration](django-integration.md) — setup, `@stream_model`, the
  durable store, SSE, admin.
- [Protocol specification](protocol.md) — the Tier-2 wire format.
