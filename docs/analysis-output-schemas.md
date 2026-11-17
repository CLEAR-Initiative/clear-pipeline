# Analysis output schemas — structured per-node objects (taxonomy-guided, v5-reconciled)

Status: design / proposal. Audience: engineers on the `analysis-retire-situation`
migration. Companion to `docs/adr/0007-*` (unified frame analysis) and the
`SectionProfile.output_schema` hook in `defs/analysis/profiles.py`.

## The principle

The taxonomy is **guidance for coverage, not output structure.** A node's
analytical question says *what to answer*; the answer is a **typed object**, never
a prose paragraph. Example — the Displacement L1 question ("Who is displaced, in
what numbers… what push and pull factors… intentions… local integration") is
answered by the v5 `displacement` object (`push_factors`, `return_intention`,
`estimated_current_displacement`, `returnees`), not a narrative blob.

Three consequences:

1. **Every node emits a structured object** (its `output_schema`). The current
   generic `DescriptiveLLM.findings: list[str]` is the PR-1 placeholder the hook
   was reserved for — it gets replaced per node family.
2. **Composition, not flattening.** L3 leaves are atomic structured answers; an L2
   composes its L3 children (+ a short synthesis); an L1 composes its L2; the root
   assembles the L1 objects into the published analysis object.
3. **v6 is a true superset of v5.** The L1 nodes that overlap v5 reuse v5's
   *published* shapes (so clear-mvp reads them byte-unchanged); new nodes add new
   fields. No lossy `project_v5` — the structured objects ARE the v5 fields.

## Archetype catalog

Most of the 695 questions fit **seven** output shapes. Each node is assigned one
archetype as its `output_schema`; the v5 models already instantiate several.

Shared envelope every node object carries (provenance + self-rating, already in
`NodeDescriptive`):

```
NodeEnvelope:
  summary: str                       # 1–2 sentence structured synthesis, NOT prose
  completeness: float                # self-rating (existing)
  questions_not_answered: list[str]  # existing
  source_report_ids: list[str]       # existing
  data: <archetype>                  # the typed answer below
```

`Described = { description: str, locations: list[str] = [], source_ids: list[str] = [] }`
`RangeFigure = { low: float|null, high: float|null, point: float|null, unit: str, as_of: str|null }`

| # | Archetype | Shape (`data`) | Fits (examples) |
|---|---|---|---|
| 1 | **Figures** | `figures: list[{metric, value: RangeFigure, group?, location?, source_ids}]` | Casualties (killed/injured/missing × location), displacement stock, PiN counts, malnutrition scale |
| 2 | **Drivers** | `factors: list[{factor, description, locations, scale_or_severity?, source_ids}]` | push/pull factors, aggravating/mitigating drivers, Shock-Event hazards |
| 3 | **SeverityNeeds** ← v5 `RawSector` | `severity: none\|low\|medium\|high\|critical\|null, top_needs: list[str], affected: RangeFigure?, findings: list[Described]` | the 10 Sectors, Humanitarian Conditions, Priority Needs |
| 4 | **Distribution** | `options: list[{option, share?: RangeFigure, by_group?, description, source_ids}]` | movement intentions (return/stay/relocate), priority convergence |
| 5 | **StatusGaps** | `status: enum\|null, gaps: list[Described], enablers: list[Described], comparison?: str` | humanitarian access barriers, local-integration parity, system functionality (Logistics/Education), Operational Capacity |
| 6 | **Described** (fallback) | `findings: list[Described]` | Context domains, qualitative nodes — the universal floor; every node can at least be this |
| 7 | **Section** (parents) | `overview: str, subsections: dict[child_key → NodeEnvelope]` + any rolled-up archetype fields | every L1 and L2 (composition of children) |

Archetype 6 is the safety net: a node with no better fit degrades to richer
structured `Described` findings (locations + sources per finding) — still typed,
never a bare string.

## L1 reconciliation map (22 nodes)

`v5 path` = where the assembled L1 object publishes into `analyses.data` so
clear-mvp reads it unchanged. `NEW` = no v5 home; publishes under the taxonomy
section (`data.taxonomy.<task>.<node_key>`), additive, zero regression.

| Task | L1 node | Archetype | v5 path (reuse) / NEW |
|---|---|---|---|
| Pillars 1D | **Displacement** | Section → maps L2 to v5 fields | `data.displacement` (`push_factors`, `return_intention`, `estimated_current_displacement`, `returnees`) |
| Pillars 1D | **Context** | Section (9 L2 = domains) | `data.context_risks.<domain>` (9 domains, each Described) |
| Pillars 1D | **Shock-Event** | Drivers | `data.hazards_and_vulnerabilities` |
| Pillars 1D | **Humanitarian Access** | StatusGaps | `data.humanitarian_access` |
| Pillars 1D | **Casualties** | Figures | **NEW** |
| Pillars 1D | **Information & Communication** | StatusGaps | **NEW** |
| Pillars 1D | **Operational Capacity** | StatusGaps | **NEW** |
| Sectors | **Agriculture … WASH** (×10) | SeverityNeeds | `data.sectors.<key>` (`_V5_SECTOR_KEY` already maps the 10) |
| Pillars 2D | **Humanitarian Conditions** | SeverityNeeds | **NEW** (overlaps sectors conceptually; keep separate) |
| Pillars 2D | **Impact** | Described / Figures | **NEW** |
| Pillars 2D | **Capacities & Response** | StatusGaps | **NEW** |
| Pillars 2D | **Priority Needs** | Distribution | **NEW** |
| Pillars 2D | **Priority Interventions** | Distribution | **NEW** |

Plus the existing non-node payload fields carry over untouched: `data.ai_summary`
(← root overview synthesis), `data.datapoints` (← numbers), `data.scenarios`,
`data.changes`, `data.sources`, `data.evidence_index`.

**The v5 contract is defined by what clear-mvp reads** (`mappers/situation-analysis.ts`:
`severity`, `top_needs`, `push_factors`, `return_intention`,
`estimated_current_displacement`, `returnees`, `context_risks.<domain>`), not by
an internal schema — the overlapping L1 assemblers must emit exactly those field
names/shapes. Pin a `test_v5_contract` that asserts the assembled payload still
satisfies the clear-mvp mapper's expectations.

## Schema registry — one JSON file, agent-consumed, regenerated on taxonomy update

The per-node output schemas live in a single committed file,
`data/node_output_schemas.json` (sibling of `taxonomy.json`), keyed by `node_key`
for **every** node (any node can be a generated frontier in a deep-dive). The
agents read a node's schema from it to structure their output — nothing is
hard-coded per node — and the file is **regenerated on every taxonomy change**, so
the schema set never drifts from the nodes.

Each entry resolves a node to a structured schema:

```json
"pillars_1d/displacement/push_factors": { "archetype": "Drivers", "pinned": false },
"sectors/health":                        { "archetype": "SeverityNeeds", "v5_path": "sectors.health", "pinned": true },
"pillars_1d/context/security":           { "archetype": "Described", "v5_path": "context_risks.security", "pinned": true }
```

- **`archetype`** → one of the 7 base shapes (the only Pydantic models in code).
  Most entries are just a reference; a node needing extra fields carries a
  `fields` override that extends the archetype.
- **`v5_path` + `pinned: true`** mark the v5-contract nodes (the reconciliation
  table): their schema and publish path are FIXED to the v5 shape, and the
  regenerator never touches them — they are the frontend contract.
- **Un-pinned entries** are (re)assigned by the regenerator from the node's
  analytical question. The wording signals the archetype: "how many / scale" →
  Figures; "which drivers / factors" → Drivers; "how severe / what needs" →
  SeverityNeeds; "what share / intentions" → Distribution; "how functional /
  barriers / parity / capacity" → StatusGaps; else → Described. LLM-assisted,
  human-reviewed on the diff.

Regeneration (`scripts/build_node_schemas.py`, run on taxonomy update): load
`taxonomy.json`; keep every `pinned` entry verbatim; (re)assign the rest;
validate the node set matches the taxonomy counts (like `_load`) so a regen can't
silently drop or reshape nodes. The agent loads this file once (`lru_cache`) and
resolves `archetype` → Pydantic model.

Net: **seven archetype models** in code, **one regenerable JSON registry** in
data, and the v5 contract **pinned** against drift.

## The join — leaves → the big analysis object

Composition is the same at every level:

```
node.object = NodeEnvelope{ summary, completeness, …, data: <archetype> }
parent.object (Section) = {
    overview: <short synthesis over children>,
    subsections: { child_key: child.object, … },
    <+ archetype roll-up fields the parent publishes, e.g. Displacement →
       push_factors from its "Push factors" L2, return_intention from "Intentions">
}
```

- **Deep-dive (selected leaves only):** generate exactly the selected leaves; the
  enclosing parent object is the *partial* composition of those leaves (no sibling
  invented). This is what `generate_nodes_for_frame` + `resolve_selection`
  (PR #213) already produce — the per-node objects are frame-addressed in S3 and
  joined at assembly time.
- **Default (country / L1):** generate the 22 L1 objects; each L1 Section
  composes its L2 (generated when depth ≥ L2, else summarised from the L1 pass);
  the root assembler places each L1 object at its v5 path + taxonomy section.
- **Publish:** `analyses.data` = { …v5 fields from the overlapping L1 assemblers,
  `taxonomy`: { per-task node tree of NodeEnvelopes }, `ai_summary`, `datapoints`,
  `scenarios`, … }. A superset of today's v5 payload.

Because every node — leaf or parent — is the same `NodeEnvelope` shape, joining
leaves into a parent, and parents into the root, is a uniform merge. That
uniformity is the point of structured-everywhere: the "big analysis object" is
just the root `Section`.

## Interaction with depth & the drains

- The frame-driven engine (the `generate_frame_analysis` slice) runs the L1 set by
  default and composes into the v5-superset payload, replacing
  `generate_and_upsert_for_frame`. Both drains cut over together (one shared
  `_run_frame_generation`).
- `SectionProfile.output_schema` + `depth` already carry the per-node schema and
  the fan-out level — this design fills `output_schema` with the archetype models
  and uses `depth` to decide how deep a given run composes.

## Open questions

1. **Context 9 domains** — RESOLVED: the taxonomy's 9 Context L2 children are 1:1
   with v5's `context_risks` domains; only the labels differ, so each L2 needs a
   `v5_key` tag:
   `Demographic→demographics, Economics→economy, Environment→environment,
   Humanitarian coordination→humanitarian_coordination,
   Infrastructure & technology→infrastructure, Legal & policy→legal_policy,
   Politics→political, Security→security, Socio-cultural→socio_culture`.
   (The 10 Sectors already match `_SECTOR_NAME_TO_KEY` / `_V5_SECTOR_KEY` exactly.)
2. **Humanitarian Conditions vs Sectors** — RESOLVED: separate paths. `Sectors`
   is its own taxonomy TASK (the 10 sector L1s publish to `data.sectors.<key>`);
   Humanitarian Conditions is a Pillars-2D L1 and publishes under the taxonomy
   section — no collision.
3. **Figures vs `datapoints`** — RESOLVED: Figures nodes **cite** the aggregated
   `datapoints` when a bucket covers the figure (one numeric source of truth), and
   fall back to extraction only when none does.
4. **Schema registry ownership** — RESOLVED: `scripts/build_node_schemas.py`
   produces `data/node_output_schemas.json`, committed and re-run on every
   taxonomy regen (see the Schema registry section).

## Next steps (slices)

1. **Archetype models** — define the 7 Pydantic schemas (reusing the v5 sector /
   displacement / Described models as archetypes 3/1/6).
2. **L1 reconciliation** — wire the 22 L1 `output_schema`s + the overlapping
   assemblers to the v5 paths; land `test_v5_contract`.
3. **Schema registry** — `scripts/build_node_schemas.py` → `data/node_output_schemas.json`
   (pinned v5 nodes verbatim + question-classified rest); the agent loads it to
   resolve each node's `output_schema`.
4. **Structured `analyse_node`** — emit the node's `output_schema` (not generic
   findings); assembly joins per the composition rule.
5. **Frame-driven engine + drains cutover** — produce the v5-superset payload;
   retire `defs/situation`.
