---
icon: lucide/file-signature
---

# The public API

What rakaia promises not to break, what it reserves the right to change, and how
to depend on it.

This exists because the first production consumer accumulated 178 imports and 84
direct database queries against rakaia, and neither side could say which of those
were supported. Some were. Some worked only by accident. The library had never
said which was which — `django_rakaia` exported *nothing*, so every consumer
import was forced to name an internal module.

## Three tiers

### Tier 1 — Stable

**`rakaia.__all__` and `django_rakaia.__all__`.** Import these from the package
root:

```python
from rakaia import replay, Effect, HandlerRegistry, AppendOptions
from django_rakaia import DjangoExecutor, DjangoProjectionReader, get_store
```

These names will not be removed or change meaning without a **major** version
bump and an entry in [`UPGRADING.md`](https://github.com/joshbrooks/rakaia/blob/main/UPGRADING.md).

`django_rakaia`'s names resolve lazily, so importing the package does not pull in
the ORM — that would raise `AppRegistryNotReady` during Django's own startup, and
is why the package exported nothing for so long.

!!! warning "Import from the package, not the module it lives in"

    `from rakaia.replay import replay` works and will keep working, but it pins
    the module **layout** rather than the surface. Splitting a file internally
    would break you even though `__all__` never changed. Prefer
    `from rakaia import replay`.

    Older docs and examples used the submodule form for names that are exported;
    that was our inconsistency, not a second sanctioned spelling.

### Tier 2 — Provisional: the ORM models and the database schema

`django_rakaia.models` — `Stream`, `StreamEvent`, `StreamEntry`,
`StreamProducer`, `StreamOffsetWatermark`, `ConsumerCursor`, `ConsumerOutcome`
— plus the table shape behind them.

**These are usable, and deliberately not in `__all__`.** Importing them from
`django_rakaia.models` keeps the weaker guarantee visible at the import site
instead of hiding it among the stable names.

What "provisional" means here:

- The models may gain fields in a minor release (they have, repeatedly).
- The *relationships* between them — that a `StreamEvent` is joined to a `Stream`
  through a `StreamEntry`, that `StreamEntry.offset` is an orderable integer
  column, that `StreamEvent.data` is a queryable `JSONField` — are not promised.
  A future storage change could denormalise them.
- Every change will appear in `UPGRADING.md` with the migration.

**Why they are available at all.** Because the alternative today is worse. The
store protocol offers `read(path, offset)` and nothing else: no filter, no limit,
no "the latest event matching this predicate". A consumer that needs to ask the
log a question — the canonical case being *"has this already been recorded?"*,
which a producer must ask of the log rather than the projection — has to read the
whole stream and filter in Python, which is O(stream) on a write path. Querying
`StreamEntry` with a JSON predicate is O(index). We are not going to pretend
that choice is unreasonable while offering no alternative.

If you are on Tier 2, say so in your own code, and expect to read `UPGRADING.md`
on every bump.

### Tier 3 — Internal

Everything else:

- any name beginning with `_`, in either package;
- any submodule of `django_rakaia` not listed in `_EXPORTS`;
- `rakaia`'s submodules as *import paths* — the names they hold may be public,
  the layout is not;
- the templates, URLs, views and admin registrations, unless you have registered
  them yourself.

These change without notice, in any release.

## Versioning

rakaia is **pre-1.0**. Semantic versioning gives a `0.x` release no compatibility
guarantee at all, so the tiers above are the promise, and the version number is
how we signal it:

| Bump | Means |
|---|---|
| `0.x.Y` → `0.x.Y+1` | Tier 1 unchanged. Bug fixes, new names. |
| `0.X` → `0.X+1` | Tier 1 may change. Tier 2 may change. Read `UPGRADING.md`. |

### Depend on us with an upper bound

```toml
# pyproject.toml
dependencies = ["rakaia-streams>=0.2,<0.3"]
```

**Not `>=0.2`.** On a pre-1.0 library an unbounded lower bound admits every
future breaking change, and takes it silently on the next lockfile refresh. This
is not hypothetical — it is the exact exposure the first consumer is carrying as
this is written.

Note the distribution is `rakaia-streams`; the import names are `rakaia` and
`django_rakaia` (plain `rakaia` was already taken on PyPI).

#### If you depend on Tier 2, pin harder

`>=0.2,<0.3` is the right bound for code living inside Tier 1. It is *not* enough
if you also query the ORM models or import a submodule path directly, because the
table shape and the module layout are both allowed to move within a minor:

```toml
dependencies = ["rakaia-streams==0.2.*"]
```

Assume this applies to you rather than assuming it does not. The first production
consumer reached 127 import statements and roughly twenty direct ORM queries
against `StreamEntry`/`Stream` before anyone asked which tier it was on — and the
answer was Tier 2, plus one private symbol. Two questions settle it:

- Does anything you import begin with `_`, or come from a `django_rakaia`
  submodule that is not listed in `_EXPORTS`?
- Do you query `django_rakaia.models` directly, or rely on `StreamEntry.offset`
  being an orderable integer column?

A "yes" to either puts you outside the Tier 1 promise, and `==0.2.*` is the
honest bound until what you need is promoted. Tell us what you are reaching for
when that happens: a Tier 2 dependency is a gap in Tier 1, and the store protocol
offering only `read(path, offset)` is the usual reason.

**Whichever bound you pick, do not verify an upgrade by reading
`rakaia.__version__` alone.** It reports the installed distribution version,
which is correct — but a consumer pinned to a git revision gets whatever version
the *source* declares, and two revisions a whole release apart can report the
same number. Check the artifact instead:

```python
importlib.metadata.distribution("rakaia-streams").read_text("direct_url.json")
```

which names the revision you actually installed.

## Contracts inside Tier 1

Being in `__all__` is not the whole promise. Four behavioural contracts are
documented where they are declared, and are worth naming here because breaking
them is easy and the failure is quiet:

- **Offsets are opaque.** Pass one back to `read()`; compare offsets *from the
  same store* lexicographically. Never parse one — no `int(offset)`. The format
  is store-specific by design ([`protocols.py`](https://github.com/joshbrooks/rakaia/blob/main/src/rakaia/protocols.py)).
- **Handlers are hermetic.** A handler reads only through the injected reader
  ([ADR 0003](adr/0003-handler-hermeticity.md)). Bind dependencies with
  `functools.partial`, not a closure — see the *Handler dependencies* entry in
  [the glossary](glossary.md).
- **`store.get()` returns metadata**, not your backend's row. It is a
  `rakaia.types.Stream` from every store.
- **Every executor and reader answers alike.** `InMemoryProjections` and
  `DjangoExecutor` converge to the same rows from the same batch, and
  `DjangoProjectionReader`, `PreloadedProjectionReader` and `InMemoryProjections`
  answer the same lookup the same way — including `model_label` being
  positional-only. Enforced by `tests/executor_contract.py` and
  `tests/projection_reader_contract.py`, the executor and reader twins of the
  store conformance suite.

## What is *not* a promise

- The wire format of the channel-layer SSE frame.
- The meta-stream payload shape (`__rakaia__:handlers` and friends). It is
  versioned by tolerance — old payloads keep loading — but not by contract.
- The in-memory `StreamStore`'s offset format, or the durable store's. Both are
  opaque tokens (see above).
- Anything in `examples/`. Those are demonstrations; copy from them freely, but
  they are not an interface.
