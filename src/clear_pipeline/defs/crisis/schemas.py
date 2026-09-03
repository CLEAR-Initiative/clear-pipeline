"""Pydantic schemas for the crisis-enrichment LLM outputs.

Ported verbatim (shapes + validators) from clear-pipeline's
``src/models/clear.py`` so the DB-written shapes stay byte-compatible with the
existing Celery path and every UI consumer. Three outputs:

  - ``CrisisNarrative``     → title + summary (``{description, tldr}`` stringified
                              into ``crises.summary``).
  - ``CrisisScenarios``     → forward-looking ``crises.scenarios`` JSONB.
  - ``CrisisNeedsAnalysis`` → NRC-SAF ``crises.needs.{generalSummary, sector}``.
"""

from typing import Literal

from pydantic import BaseModel, ConfigDict, field_validator

# Canonical sector list for `crises.needs.sector`. Source of truth — the needs
# prompt references this same tuple so the LLM produces matching keys, and the
# validator below rejects anything else (no hallucinated sectors).
NEEDS_SECTORS: tuple[str, ...] = (
    "Shelter",
    "WASH",
    "Protection",
    "Health",
    "Food Security",
    "Education",
)

# SAF (NRC Situation Analysis Framework) Dimension 6 — Humanitarian Conditions
# severity scale. Ordered low → high. Used as the `severity` field on each
# SectorAnalysis entry.
SAF_SEVERITY_LEVELS = Literal[
    "Minimal",
    "Stressed",
    "Severe",
    "Extreme",
    "Catastrophic",
]


class CrisisNarrative(BaseModel):
    """Coherent title + structured summary linking a crisis's events. The
    pipeline stringifies ``{description, tldr}`` into the ``crises.summary``
    column (see ``CRISIS_PROMPT_VERSION`` = crisis-v2)."""

    title: str
    description: str
    tldr: list[str]


class CrisisScenarios(BaseModel):
    """Forward-looking scenario analysis, stored verbatim on ``crises.scenarios``
    (JSONB). Four prose fields — most_likely / best_case / worst_case plus a
    scenario-variables summary."""

    most_likely: str
    best_case: str
    worst_case: str
    description: str


class SectorAnalysis(BaseModel):
    """One per-sector entry inside ``crises.needs.sector``.

    Required fields are the ones every UI consumer expects to render uniformly
    across sectors. Anything beyond them (indicator percentages, recommended
    response type, cluster actors, etc.) is allowed via ``extra='allow'`` so the
    schema can grow without a Pydantic change.
    """

    model_config = ConfigDict(extra="allow")

    description: str
    severity: SAF_SEVERITY_LEVELS
    responseGap: bool
    nrcRelevant: bool


class CrisisNeedsAnalysis(BaseModel):
    """Output from the needs-analysis generation (NRC SAF framework).

    Top-level ``generalSummary`` (4 bullet points) + ``sector`` (per-sector
    breakdown keyed by canonical NRC sector names — see ``NEEDS_SECTORS``).
    Stored under ``crises.needs.{generalSummary, sector}`` via a JSONB merge so
    other keys the user supplied at creation time stay intact.
    """

    generalSummary: list[str]
    sector: dict[str, SectorAnalysis]

    @field_validator("generalSummary")
    @classmethod
    def _validate_general_summary(cls, v: list[str]) -> list[str]:
        # Non-empty list of non-empty strings. We don't enforce exactly 4 at
        # validation time — the prompt drives the count and a 3-or-5 response is
        # still useful enough to keep.
        if not v:
            raise ValueError("generalSummary must contain at least one bullet")
        if any(not isinstance(item, str) or not item.strip() for item in v):
            raise ValueError("generalSummary entries must be non-empty strings")
        return v

    @field_validator("sector")
    @classmethod
    def _validate_sector_keys(
        cls, v: dict[str, SectorAnalysis],
    ) -> dict[str, SectorAnalysis]:
        unknown = set(v.keys()) - set(NEEDS_SECTORS)
        if unknown:
            raise ValueError(
                f"Unknown sector keys: {sorted(unknown)}; expected subset of {NEEDS_SECTORS}"
            )
        return v
