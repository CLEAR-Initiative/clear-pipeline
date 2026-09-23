"""Pydantic schema for the hotline-enrichment LLM output.

Shaped after `providers.classify.SignalClassification` (severity 1-5 +
glide-code disaster type) per the ticket's "reuse SignalClassification"
instruction — but produced by an LLM call over raw chat text instead of
`classify_locally`'s local sentence-transformer classifier, which needs an
already-extracted title/description that hotline messages don't have.
"""

import json
from typing import Literal

from pydantic import BaseModel, field_validator

from clear_pipeline.providers.classify import DEFAULT_TAXONOMY_PATH

# clear-api's GROUND_CLASSIFICATIONS set (src/resolvers/ground.resolver.ts) —
# must match exactly, the mutation rejects anything else.
HotlineMessageClassification = Literal[
    "field_report", "news_digest", "operational", "chatter"
]

# Loaded once from the same taxonomy file `classify_locally` uses
# (id_type == "glide_number" for every entry today — see
# disasterTypes.idType on clear-api). A lightweight JSON read; does NOT
# import the classifier class, so no sentence-transformer model load.
# Exposed (not `_`-prefixed) so prompts.py can build the prompt's code list
# from the same source instead of a second, driftable copy.
DISASTER_TYPE_TAXONOMY: list[dict] = json.loads(
    DEFAULT_TAXONOMY_PATH.read_text(encoding="utf-8")
)
VALID_DISASTER_TYPE_CODES: frozenset[str] = frozenset(
    str(r["id"]) for r in DISASTER_TYPE_TAXONOMY
)


class HotlineEnrichment(BaseModel):
    """One hotline message's enrichment: classification + headline + a
    severity/disaster-type suggestion. Location is resolved separately by
    the geoparser (not an LLM output — see `stages.py`)."""

    classification: HotlineMessageClassification
    """Short LLM-generated headline, <=70 chars."""
    title: str
    """1-5 scale, same range as signals.severity."""
    severity: Literal[1, 2, 3, 4, 5]
    """Glide code (e.g. "fl", "cf") from the same taxonomy
    `classify_locally` matches against, or null when no disaster type
    applies (e.g. pure chatter)."""
    disaster_type: str | None
    """Contributor's own uncertainty tag ("unconfirmed", "rumour"), if the
    message text carries one. Null when absent."""
    uncertainty_marker: str | None

    @field_validator("disaster_type")
    @classmethod
    def _validate_disaster_type(cls, v: str | None) -> str | None:
        if v is not None and v not in VALID_DISASTER_TYPE_CODES:
            raise ValueError(f"disaster_type {v!r} is not a known glide code")
        return v
