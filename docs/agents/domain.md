# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

This repo is **single-context**: one domain, one decision log, shared across both
packages (`src/rakaia`, the zero-dependency core, and `src/django_rakaia`, the
Django integration). There is no `CONTEXT-MAP.md` and none is needed.

## Before exploring, read these

- **`docs/glossary.md`** — this repo's domain-language document. It plays the role
  that `CONTEXT.md` plays in the skill templates: it defines the event-sourcing
  vocabulary (stream, event, projection, handler, cursor, fencing) in the precise
  sense Rakaia uses. Read it before naming any domain concept.
  There is no `CONTEXT.md` at the root and none is needed — don't create one.
- **`docs/adr/`** — read ADRs that touch the area you're about to work in.
  Currently:
  - ADR-0001 — ordering child collections in projections
  - ADR-0002 — framework vs protocol server boundary
  - ADR-0003 — handler hermeticity
- The wider `docs/` tree is reference material, not domain definition. Reach for a
  specific page (`protocol.md`, `subscriber-cursors.md`, `versioned-handlers.md`, …)
  when the glossary entry points at it.

If a file named above has since been removed, **proceed silently**. Don't flag its
absence and don't suggest recreating it.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a
hypothesis, a test name), use the term as defined in `docs/glossary.md`. Don't drift
to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal — either you're
inventing language the project doesn't use (reconsider) or there's a real gap (note
it, and add the entry to `docs/glossary.md` rather than starting a `CONTEXT.md`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0003 (handler hermeticity) — but worth reopening because…_

New ADRs go in `docs/adr/` following the existing `NNNN-kebab-title.md` numbering.
