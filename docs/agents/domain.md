# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

This repo is **single-context**: one `CONTEXT.md` + `docs/adr/` at the root.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root
- **`docs/adr/`** — read ADRs that touch the area you're about to work in

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

> **Note for this repo:** `CONTEXT.md` and `docs/adr/` both exist as of 2026-07-15, created during a `/grill-with-docs` session on the situation-analysis work. `CONTEXT.md` defines the population-figure vocabulary (Affected / Casualties / PIN / Displaced) and the Report / Incident / Event Type distinction — read it before naming anything in those areas, as several of those terms were actively conflated in code before it existed. `/improve-codebase-architecture` is not installed, so `/domain-modeling` is reachable via `/grill-with-docs` or directly.
>
> Note ADRs are split across two repos: extraction and modelling decisions live here; aggregation decisions live in `clear-api/docs/adr/`. ADR-0002 here and ADR-0001 there are two halves of one story.

## File structure

Single-context repo (this repo):

```
/
├── CONTEXT.md
├── docs/adr/
│   ├── 0001-....md
│   └── 0002-....md
└── src/
```

If this repo ever splits into genuinely separate domains, switch to multi-context by adding a `CONTEXT-MAP.md` at the root pointing at one `CONTEXT.md` per context, with context-scoped ADRs under `src/<context>/docs/adr/`.

## Use the glossary's vocabulary

When your output names a domain concept (in a ticket title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 (event-sourced orders) — but worth reopening because…_
