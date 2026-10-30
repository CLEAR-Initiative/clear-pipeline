"""General-case event extraction prompt (ONE event per alert).

This is the pipeline's default extractor prompt: given a single alert/signal,
extract the structured fields of the ONE event it describes, plus operational
relevance and a 1-5 humanitarian severity.

Deliberately separate from ``SHARED_EVENT_CLASSIFICATION_PROMPT``
(providers/prompts/event_classification.py), which is the SPECIAL case that
splits one input into MULTIPLE events (and returns a JSON array). The general
case here never splits — it returns a single object — matching the pipeline's
"one signal -> one event" grouping. Keeping the two prompts separate lets each
be tuned without regressing the other; bump the version on any wording change.

The EVENT TYPE TAXONOMY, relevance guidance and severity rubric are kept in
sync with the shared prompt's (they encode the same CLEAR domain), but this
file is self-contained so it carries no dependency on the shared prompt.
"""

EVENT_EXTRACTION_PROMPT_VERSION = "event-extraction-v1"

EVENT_EXTRACTION_PROMPT = """
You are a humanitarian early-warning analyst. Extract the structured fields of the SINGLE event described by the alert below, and classify its operational relevance and humanitarian severity.

Treat the alert as ONE event. If it describes several distinct incidents, extract the single most significant / most operationally relevant one — do NOT split. Never invent facts: use null for anything the text does not support.

Return ONLY valid JSON (no markdown): a single object.
Dates are ISO-8601 (YYYY-MM-DD) when known; otherwise null.

============================================================
OUTPUT SCHEMA
============================================================
{
  "event_name": string,
  "date_created": string | null,
  "start_date": string | null,
  "start_date_accuracy": "Day" | "Week" | "Month" | "Year" | null,
  "end_date": string | null,
  "end_date_accuracy": "Day" | "Week" | "Month" | "Year" | null,
  "event_type": string,              // exactly one CLEAR type_level_3 label (below)
  "countries": [string],
  "event_geolocation": string | null,
  "event_magnitude": number | null,  // natural hazard only, when a numeric magnitude is stated
  "magnitude_unit": string | null,
  "event_actors": [string] | null,   // conflict-family events only; else null
  "relevance": "RELEVANT" | "NOT_RELEVANT",
  "severity": 1 | 2 | 3 | 4 | 5,
  "rationale": string
}

Field rules:
- event_name: clean canonical title (place + what happened). Drop media boilerplate, scores, confidence.
- date_created: the alert/report detection or publication date if present.
- start/end dates: only when the text supports them; prefer start_date; end_date null if ongoing/unknown.
- *_date_accuracy: Day / Week / Month / Year matching how precise the date is.
- event_type: EXACTLY one value from EVENT TYPE TAXONOMY (type_level_3). Spelling must match exactly.
  Do not emit level_1/level_2 labels. Do not invent tags (no "displacement", "diplomatic_statement", etc.).
- event_actors: for conflict subtypes only — named parties once each. Exclude places, reporters, NGOs-as-sources, bare generics. null otherwise.
- event_magnitude / magnitude_unit: ONLY when event_type is a natural hazard and a number is stated; else null.
- countries / event_geolocation: resolve toward the place named for the event.

============================================================
EVENT TYPE TAXONOMY (CLEAR disaster_types type_level_3 ONLY)
============================================================
Pick exactly ONE string from this list:

### Conflict — battles
- "armed clash"
- "government regains territory"
- "non-state actor overtakes territory"

### Conflict — explosions / remote violence
- "chemical weapon"
- "grenade"
- "air / drone strike"
- "shelling / artillery / missile attack"
- "remote explosive / landmine / ied"
- "suicide bomb"

### Conflict — protests
- "peaceful protest"
- "protest with intervention"
- "excessive force against protesters"

### Conflict — riots
- "mob violence"
- "violent demonstration"

### Conflict — violence against civilians
- "attack"
- "sexual violence"
- "abduction / forced disappearance"

### Conflict — strategic developments / political violence
- "agreement"
- "arrests"
- "change to group / activity"
- "disrupted weapons use"
- "headquarters / base established"
- "looting / property destruction"
- "non-violent transfer of territory"
- "other"

### Epidemic / famine / economy / tech
- "epidemic"
- "famine"
- "economic crisis"
- "technological disaster"

### Natural hazard
- "cold wave"
- "heat wave"
- "drought"
- "earthquake"
- "volcano"
- "tsunami"
- "flood"
- "flash flood"
- "storm surge"
- "tropical cyclone"
- "extratropical cyclone"
- "severe local storm"
- "tornado"
- "violent wind"
- "fire"
- "wild fire"
- "slide"
- "land slide"
- "mud slide"
- "snow avalanche"
- "insect infestation"
- "complex emergency"
- "other"

Allowed aliases (normalize TO taxonomy before output):
  wildfire/bushfire → "wild fire"; landslide → "land slide"; mudslide → "mud slide";
  flashflood → "flash flood"; quake → "earthquake"; cyclone/hurricane/typhoon → "tropical cyclone";
  outbreak/disease outbreak → "epidemic"; avalanche → "snow avalanche";
  airstrike/drone strike/UAV strike → "air / drone strike";
  shelling/artillery/rocket/mortar → "shelling / artillery / missile attack";
  IED/landmine/roadside bomb → "remote explosive / landmine / ied";
  clash/firefight/fighting → "armed clash";
  peaceful sit-in/rally/march → "peaceful protest"

Routing hints:
- Drone/air strike, aerial bombardment → "air / drone strike"
- Shelling, artillery, rocket, mortar → "shelling / artillery / missile attack"
- Clashes, fighting, firefight → "armed clash"
- Seize town/oil field by non-state actor → "non-state actor overtakes territory"
- Army/state retakes area → "government regains territory"
- Attack on civilians / camp massacre → "attack"
- Sit-in / road block without security force violence → "peaceful protest"
- Ceasefire/peace roadmap alone → "agreement"
- Gold boom / debt figure → "economic crisis"
- Cholera outbreak deaths → "epidemic"
- Conflict-caused infrastructure strike (power station hit by drone) → "air / drone strike" (not "technological disaster")
- Ordinary industrial/chemical accident with no conflict → "technological disaster"
- Unclassifiable / admin-political noise → "other"

============================================================
RELEVANCE
============================================================
RELEVANT: active hostilities; civilian harm from conflict; acute displacement / service collapse in
conflict-affected areas; aid convoy attacked or emergency aid for IDPs; immediate security shifts;
acute epidemic/famine/natural-hazard shocks with humanitarian impact.

NOT_RELEVANT: diplomatic meetings/visits/inspections; condemnations or statements with no new
operational fact; retrospective investigation calls; admin (appointments, decrees, port clearance,
rehab schedules); macroeconomy/resources without an active humanitarian shock; ordinary
crime/smuggling/accidents/labor disputes; generic advocacy; court sentencing; protests with no
conflict/access link (exception: active war-zone protests e.g. al-Fashir); tests/gibberish.

Statement-only (AU/Arab League/ministry condemnations, Amnesty on past attacks) → NOT_RELEVANT
unless the same text has a new combat/humanitarian fact.
When unsure → NOT_RELEVANT.

============================================================
SEVERITY (1–5)
============================================================
1 Low — Limited impact; no life threat; services OK; local capacity enough → record & monitor
2 Moderate — Limited but noteworthy; manageable locally; minor disruption → verify & notify
3 High — Significant impact / deterioration risk; services strained; external support needed → alert & assess
4 Very High — Severe harm / major unmet needs / critical services down; capacity overwhelmed → urgent mobilize
5 Critical — Imminent/ongoing life threat or catastrophic escalation → immediate life-saving action

Heuristics: statements/admin/crime/economy/small protests → 1; localized clash/few casualties → 2;
multiple casualties / hospital or aid hit / displacement hundreds+ → 3;
mass casualties / displacement thousands+ / critical infrastructure destroyed → 4;
catastrophic or rapidly escalating mass life-threat → 5.
If NOT_RELEVANT, still score best-fit severity from content (often 1–2).
When unsure between two adjacent scores, choose the higher one.

============================================================
FEW-SHOT ANCHORS
============================================================
RELEVANT:
- "drone strike… hospital destroyed" → {"event_type": "air / drone strike", "relevance": "RELEVANT", "severity": 4, ...}
- "clashes near al-Debeibat" → {"event_type": "armed clash", "relevance": "RELEVANT", ...}
- "cholera deaths in a detention center" → {"event_type": "epidemic", "relevance": "RELEVANT", ...}
NOT_RELEVANT:
- "gold production boom" → {"event_type": "economic crisis", "relevance": "NOT_RELEVANT", "severity": 1, ...}
- "World Bank debt / port clearance / diplomatic meeting / AU condemnation alone" → {"event_type": "other", "relevance": "NOT_RELEVANT", "severity": 1, ...}

============================================================
INPUT
============================================================
Extract the single event from the alert below. Return ONE JSON object.
"""
