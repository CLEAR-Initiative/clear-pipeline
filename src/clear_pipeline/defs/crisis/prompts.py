"""Crisis-enrichment prompts (narrative / scenarios / NRC-SAF needs).

Ported from clear-pipeline's ``src/prompts/crisis.py``. Two adaptations for the
Dagster path:

  - Output shape is enforced by the structured-output layer
    (``LLMProvider.complete_structured`` + the pydantic schemas in
    ``schemas.py``), so the legacy "respond with valid JSON only" system lines
    and the literal ``Respond with this exact JSON structure {…}`` tails are
    dropped — they conflict with json_schema mode and are redundant.
  - Each user prompt carries a ``RETRIEVED EVIDENCE`` slot the caller fills with
    knowledgebase RAG context scoped to the crisis's country + event types. The
    legacy Celery task had no knowledgebase grounding; this is the #2 addition.

Version constants are preserved so telemetry/prompt-diffing stays comparable
with the Celery path.
"""

CRISIS_PROMPT_VERSION = "crisis-v2"
SCENARIOS_PROMPT_VERSION = "crisis-scenarios-v1"
NEEDS_ANALYSIS_PROMPT_VERSION = "crisis-needs-analysis-v1"

_EVIDENCE_INSTRUCTION = (
    "Ground your analysis in the RETRIEVED EVIDENCE below — knowledgebase "
    "excerpts from humanitarian reports about this crisis's country and hazard "
    "types. Use it for context, figures, and drivers; do not invent numbers the "
    "events and evidence don't support. When the evidence is thin, say less "
    "rather than speculate."
)


# ─── Narrative (title + summary) ──────────────────────────────────────────

NARRATIVE_SYSTEM_PROMPT = """\
You are a humanitarian intelligence analyst for the CLEAR early warning system.

You write concise, actionable narratives for humanitarian workers and NGOs
operating in crisis zones. Your summaries connect multiple events into a
single coherent crisis so responders can act quickly."""

_NARRATIVE_USER_TEMPLATE = """\
Generate a title, description, and tldr for a humanitarian crisis linking the events below.

{evidence_instruction}

Events ({event_count}):
{events_block}

Locations affected: {locations}

Guidelines:
- Title: <=70 chars, human-readable, no emojis, no brackets/quotes. Lead with the
  dominant disaster type(s) and location (e.g. "Floods in North Darfur and Kassala").
- Description: 2-3 sentences (paragraph form). Describe what is happening, where,
  scale (population affected if known), and the humanitarian implication
  (displacement, food security, health risk, etc.). Avoid generic filler.
- TLDR: exactly three one-liner bullet points that together summarise the full
  description. Each bullet is a single short sentence (<=20 words), no leading
  dashes or bullets, no markdown. The three together should cover: (1) what
  happened, (2) where and at what scale, (3) the humanitarian implication.

RETRIEVED EVIDENCE:
{evidence_block}
"""


# ─── Scenarios (forward-looking) ──────────────────────────────────────────

SCENARIOS_SYSTEM_PROMPT = """\
You are a humanitarian intelligence analyst for the CLEAR early warning system.

You produce forward-looking scenario analyses for ongoing crises — the kind
NGOs and field operators read to anticipate how a situation may evolve over
the next weeks and months. Be specific. Tie each scenario to the events
provided."""

_SCENARIOS_USER_TEMPLATE = """\
Develop scenarios for how the situation may evolve.

{evidence_instruction}

Events ({event_count}):
{events_block}

Locations affected: {locations}

Most Likely Scenario:
- What is the most probable trajectory for the crisis?
- How are humanitarian conditions likely to evolve?
- What factors support this scenario?

Alternative Scenarios:
- What other plausible scenarios exist (best case, worst case)?
- What would trigger these alternative scenarios?
- What is the likelihood of each scenario?

Scenario Variables — consider how these factors may change:
- Political and security dynamics
- Economic conditions
- Environmental/seasonal factors (harvest, rainy season, etc.)
- Disease outbreaks or public health events
- Policy changes
- Humanitarian access
- Response capacity and funding
- Population movements

Each scenario must be 2-4 sentences of concrete prose grounded in the events
and evidence above — no generic filler, no markdown bullets inside the strings,
no emojis. Populate most_likely, best_case, worst_case, and a description
summarising which way the scenario variables are currently trending.

RETRIEVED EVIDENCE:
{evidence_block}
"""


# ─── Needs analysis (NRC SAF) ─────────────────────────────────────────────

NEEDS_ANALYSIS_SYSTEM_PROMPT = """\
You are an emergency response analyst applying the NRC Situation Analysis Framework (SAF). You are given MSNA indicator data (household survey, 8 months old) and OCHA 3W partner presence data for a specific locality in Sudan. Your task is to produce a structured assessment for an Emergency Response Manager deciding whether to deploy a response, run a Rapid Needs Assessment, or monitor.

Apply the SAF Humanitarian Conditions framework (Dimension 6) to assess sector severity, and the SAF Priority Needs framework (Dimension 7) to synthesise across sectors."""

_NEEDS_ANALYSIS_USER_TEMPLATE = """\
Produce a structured needs analysis with two parts.

{evidence_instruction}

1. `generalSummary` — An array of EXACTLY 4 bullet points. Each bullet is
   ONE short sentence (≤25 words, ~180 characters). No leading dashes,
   no markdown, no line breaks inside a bullet. Brevity matters — a
   responder is scanning these on a phone, not reading a paragraph.
   The four bullets cover, in order:
   1. Overall severity on the SAF five-level scale (Minimal / Stressed /
      Severe / Extreme / Catastrophic), the 1-2 sectors driving it, and
      a confidence level (High / Medium / Low).
   2. The single causal factor that explains *why* conditions are what
      they are — not a restatement of the indicators.
   3. The most acute response gap — one Severe-or-above sector with no
      3W cluster actor, with a brief NRC-fit note.
   4. The priority action implied by SAF Dimension 7: immediate
      life-saving response, stabilisation response, assessment-first
      (RNA), or monitoring.

2. `sector` — An object keyed by NRC sector. Produce one entry for each of
   the six sectors below. Each entry has these required fields:
   - `description` (string, 2-3 sentences) — prose covering severity, the
     cluster gap picture, and any NRC-relevant context.
   - `severity` (string) — exactly one of: "Minimal", "Stressed",
     "Severe", "Extreme", "Catastrophic". Use the SAF Dimension 6 scale;
     stick to this exact casing/spelling.
   - `responseGap` (boolean) — `true` when no cluster actor is present in
     3W data for this sector (an unmet need or reporting gap), `false`
     when the cluster is covered. If 3W absence is more likely a
     reporting lag than a true gap, still mark `true` but say so in the
     `description`.
   - `nrcRelevant` (boolean) — `true` when NRC has a relevant core
     competency for this sector (Shelter, WASH, Education, ICLA, LFS),
     `false` otherwise. Use this to flag where NRC could plausibly deploy.

   If a sector is clearly Minimal or not applicable, still produce its
   entry with `severity: "Minimal"` and explain in the description rather
   than omitting it — this keeps the UI consistent across crises.

   Canonical sector names (use these exact strings as JSON keys):
   - Shelter
   - WASH
   - Protection
   - Health
   - Food Security
   - Education

Important:
- Do not reference composite scores or numeric indices.
- Use actual indicator percentages from the data when available.
- Distinguish between what the data shows (observed) and what you are inferring (analytical judgment).
- Flag in the confidence rating if data age (8 months) materially limits your confidence.

Context — events ({event_count}):
{events_block}

Locations affected: {locations}

Locality data (MSNA indicators, OCHA 3W partner presence, other available
location metadata; may be sparse — flag this in your confidence rating):
{locality_data_block}

RETRIEVED EVIDENCE:
{evidence_block}
"""


# ─── Formatters ───────────────────────────────────────────────────────────

def _format_events_block(events: list[dict]) -> str:
    """Render the events list into the prose chunk every prompt embeds."""
    lines = []
    for i, e in enumerate(events, 1):
        title = e.get("title") or "(untitled)"
        desc = (e.get("description") or "").strip()
        types = ", ".join(e.get("types") or []) or "unknown"
        severity = e.get("severity") if e.get("severity") is not None else "?"
        pop = e.get("populationAffected")
        pop_str = f" pop_affected={pop}" if pop else ""
        lines.append(
            f"{i}. [{types}] severity={severity}{pop_str}\n"
            f"   title: {title}\n"
            f"   description: {desc[:300]}"
        )
    return "\n".join(lines) if lines else "(no events)"


def _format_locality_data_block(events: list[dict]) -> str:
    """Format whatever location metadata the events carry into a single text
    block the LLM can read. Falls back to an explicit 'no metadata' marker when
    nothing is available so the LLM downgrades confidence instead of fabricating
    indicator values."""
    chunks: list[str] = []
    seen_locations: set[str] = set()
    for event in events:
        for key in ("generalLocation", "originLocation", "destinationLocation"):
            loc = event.get(key)
            if not loc:
                continue
            loc_name = loc.get("name")
            if not loc_name or loc_name in seen_locations:
                continue
            seen_locations.add(loc_name)
            metadata = loc.get("metadata") or []
            if not metadata:
                continue
            chunks.append(f"Location: {loc_name}")
            for entry in metadata:
                meta_type = entry.get("type", "unknown")
                meta_data = entry.get("data") or {}
                chunks.append(f"  [{meta_type}] {meta_data}")
    return "\n".join(chunks) if chunks else "(no MSNA / 3W / locality metadata available for these locations)"


def _locations_str(locations: list[str]) -> str:
    return ", ".join(locations) if locations else "unknown"


def build_narrative_prompt(events: list[dict], locations: list[str], evidence_block: str) -> str:
    return _NARRATIVE_USER_TEMPLATE.format(
        evidence_instruction=_EVIDENCE_INSTRUCTION,
        event_count=len(events),
        events_block=_format_events_block(events),
        locations=_locations_str(locations),
        evidence_block=evidence_block or "(no knowledgebase evidence retrieved)",
    )


def build_scenarios_prompt(events: list[dict], locations: list[str], evidence_block: str) -> str:
    return _SCENARIOS_USER_TEMPLATE.format(
        evidence_instruction=_EVIDENCE_INSTRUCTION,
        event_count=len(events),
        events_block=_format_events_block(events),
        locations=_locations_str(locations),
        evidence_block=evidence_block or "(no knowledgebase evidence retrieved)",
    )


def build_needs_analysis_prompt(events: list[dict], locations: list[str], evidence_block: str) -> str:
    return _NEEDS_ANALYSIS_USER_TEMPLATE.format(
        evidence_instruction=_EVIDENCE_INSTRUCTION,
        event_count=len(events),
        events_block=_format_events_block(events),
        locations=_locations_str(locations),
        locality_data_block=_format_locality_data_block(events),
        evidence_block=evidence_block or "(no knowledgebase evidence retrieved)",
    )
