---
status: accepted
---

# Structured per-node analysis output schemas — taxonomy-guided, a true superset of v5

## Context

The agentic analysis DAG (`defs/analysis`) generates per **taxonomy node**; the
unified frame generator (`generate_and_upsert_for_frame`, historically in
`defs/situation`) is the live frame-level analysis producer behind both drains and
publishes the **v5** `analyses.data` payload (`ai_summary`, `sectors`,
`context_risks`, `displacement`, `humanitarian_access`,
`hazards_and_vulnerabilities`, `scenarios`, `datapoints`, …). ADR-0007 reserved
retiring it once the descriptive DAG is built.

The descriptive DAG currently emits a **generic** `findings: list[str]` per node
and a lossy v6→v5 projection (`project_v5`): the 9 `context_risks` domains collapse
to empty, and structured sub-fields (`sectors.*.severity`/`top_needs`,
`displacement.push_factors`/`return_intention`) flatten to findings. **clear-mvp**
(`mappers/situation-analysis.ts`) is a live consumer of those top-level structured
fields, so cutting the drains over to the current v6 would **regress the frontend**.

The key insight (why the projection is lossy at all): the taxonomy should be
**guidance for coverage, not output structure**. A node's analytical question says
*what to answer*; the answer is a **typed object**, not prose. The Displacement
question is answered by the `displacement` object, not a paragraph. Done this way,
v6 is a *true superset* of v5 — the same structured objects, with the taxonomy
expanding which objects exist. `SectionProfile.output_schema` was reserved for
exactly this (left `None` in PR-1; the generic `findings` is its placeholder).

## Decision

**Every taxonomy node emits a structured object; the published payload is a true
superset of v5 (no projection); the overlapping L1 nodes reuse v5's published
shapes at their exact paths.** Concretely:

### 1. Seven archetypes (the only schema models in code)

Each node carries a `NodeEnvelope` (`summary`, `completeness`,
`questions_not_answered`, `source_report_ids`) + a typed `data` of one archetype:

| Archetype | `data` shape | Fits |
|---|---|---|
| **Figures** | `figures: [{metric, value: RangeFigure, group?, location?, source_ids}]` | Casualties, displacement stock, PiN counts |
| **Drivers** | `factors: [{factor, description, locations, scale_or_severity?, source_ids}]` | push/pull/aggravating factors, Shock-Event |
| **SeverityNeeds** (= v5 sector) | `severity, top_needs, affected?: RangeFigure, findings: [Described]` | 10 Sectors, Humanitarian Conditions, Priority Needs |
| **Distribution** | `options: [{option, share?: RangeFigure, by_group?, description, source_ids}]` | movement intentions, priority convergence |
| **StatusGaps** | `status?, gaps: [Described], enablers: [Described], comparison?` | access barriers, parity, system functionality, capacity |
| **Described** (fallback) | `findings: [Described]` | Context domains, qualitative nodes — the universal floor |
| **Section** (parents) | `overview, subsections: {child_key → NodeEnvelope}` + archetype roll-ups | every parent, composing children |

`Described = {description, locations[], source_ids[]}`;
`RangeFigure = {low, high, point, unit, as_of}`.

### 2. L1 reconciliation (the non-regression contract)

The 22 L1 objects assemble into `analyses.data`. The **v5-overlapping** ones
publish to their current paths unchanged; **new-only** ones go under
`data.taxonomy.<task>.<node_key>` (additive):

- `Displacement`→`data.displacement`; `Context`→`data.context_risks.<domain>`
  (9 L2 = the 9 domains, 1:1, name-mapped); `Shock-Event`→`data.hazards_and_vulnerabilities`;
  `Humanitarian Access`→`data.humanitarian_access`; the 10 **Sectors** (their own
  *task*)→`data.sectors.<key>`.
- **New-only**: `Casualties`, `Information & Communication`, `Operational Capacity`,
  and all five **Pillars-2D** L1s (incl. Humanitarian Conditions — kept off
  `data.sectors` since Sectors is a distinct task).
- Carried over untouched: `ai_summary` (← root overview), `datapoints`, `scenarios`,
  `changes`, `sources`, `evidence_index`.

**The v5 contract is defined by what clear-mvp reads**, pinned by a
`test_v5_contract`. `Figures` nodes **cite** the aggregated `datapoints` bucket
when one covers the figure (one numeric source of truth), extracting only as
fallback.

### 3. Schema registry — one regenerable JSON file

Per-node schemas live in `data/node_output_schemas.json` (sibling of
`taxonomy.json`), keyed by `node_key` for every node, **regenerated on every
taxonomy change** (`scripts/build_node_schemas.py`). Each entry is
`{archetype, v5_path?, pinned}` (+ optional `fields` override). The v5-contract
nodes are **`pinned`** — the regenerator never touches them; the rest are
(re)classified from the analytical question (wording → archetype), LLM-assisted and
human-reviewed on the diff. The agent loads the registry (`lru_cache`) and resolves
`archetype` → Pydantic model. Code = 7 archetypes; data = taxonomy + registry.

### 4. Composition / the join

Every node — leaf or parent — is the same `NodeEnvelope`, so joining is a uniform
merge: an L2 composes its L3 leaves; an L1 composes its L2; the root places each L1
object at its v5 path (+ taxonomy section). The "big analysis object" is the root
`Section`. A deep-dive over selected leaves composes a *partial* parent — exactly
what `generate_nodes_for_frame` + `resolve_selection` (PR #213) already produce.

## Consequences

- **No frontend regression**: clear-mvp reads `data.displacement.push_factors`,
  `data.sectors.<k>.severity`, `data.context_risks.<domain>` unchanged; new taxonomy
  fields are additive.
- **situation retires cleanly**: its domain generators become the nodes'
  `output_schema` (subsumed, not deleted ad hoc), run by the agentic engine.
- **More up-front work** than generic findings: 7 models + a generated registry +
  the assembler. Mitigated by archetype reuse and the regenerable registry (not 695
  bespoke models).
- **One numeric source of truth**: Figures cite `datapoints`; no divergent numbers.

## Alternatives considered

- **Keep the lossy projection** — rejected: regresses clear-mvp's structured fields.
- **Additive hybrid (keep full v5 + append v6 nodes), situation stays** — viable and
  non-regressive, but never retires situation and duplicates the numeric/narrative
  work. The structured-node approach reaches the same superset while *replacing* the
  v5 producer.
- **Migrate the frontend to v6 taxonomy nodes** — a forward UX change, not "no
  regression"; deferred to after the structured superset ships.
- **A bespoke schema per leaf in code** — 695 models; rejected for the 7-archetype +
  regenerable-registry split.

## Related

- `docs/analysis-output-schemas.md` — the full design spec this ADR records
  (archetype fields, the per-node registry format, the composition rule).
- ADR-0007 (unified frame analysis), ADR-0003 (LLM role selection — why narrative
  was pinned to Claude; structured nodes run on DeepSeek).
- PR #213 — `generate_nodes_for_frame` / `resolve_selection` / frame-addressing, the
  building blocks the composition reuses.
- `defs/analysis/profiles.py` (`SectionProfile.output_schema`), `payload.py`
  (`_V5_SECTOR_KEY`, the projection being replaced).
