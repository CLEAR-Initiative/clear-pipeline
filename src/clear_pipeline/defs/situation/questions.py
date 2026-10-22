"""Load first-level analysis questions from CSV.

Questions are the source of truth for both RAG retrieval queries and
generation prompts. Generators must not invent parallel topic prompts —
look up the CSV question by schema key instead.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from functools import lru_cache
from pathlib import Path

_DEFAULT_CSV = (
    Path(__file__).resolve().parent / "data" / "analysis_questions_first_level.csv"
)

# Map CSV ``Name`` → pipeline schema keys. Every first-level CSV row must
# appear here so generators can look up analysis questions by schema key.
_CONTEXT_NAME_TO_KEY: dict[str, str] = {
    "Demographics": "demographics",
    "Political": "political",
    "Economy": "economy",
    "Socio culture": "socio_culture",
    "Security": "security",
    "Legal and policy": "legal_policy",
    "Infrastructure & technology": "infrastructure",
    "Environment": "environment",
    "Humanitarian Coordination": "humanitarian_coordination",
}

_SITUATION_NAME_TO_COMPONENT: dict[str, str] = {
    "Displacement": "displacement",
    "Shock-Event": "hazards_and_vulnerabilities",
    "Humanitarian Access": "humanitarian_access",
}

_SECTOR_NAME_TO_KEY: dict[str, str] = {
    "Agriculture": "agriculture",
    "Education": "education",
    "Food Security": "food_security",
    "Health": "health",
    "Livelihoods": "livelihoods",
    "Logistics": "logistics",
    "Nutrition": "nutrition",
    "Protection": "protection",
    "Shelter": "shelter",
    "WASH": "wash",
}

# Single taxonomy source for generators / changes digests. Order matches
# the CSV maps above (insertion order) and the dashboard tab layout.
CONTEXT_DOMAIN_KEYS: tuple[str, ...] = tuple(_CONTEXT_NAME_TO_KEY.values())
SECTOR_KEYS: tuple[tuple[str, str], ...] = tuple(
    (key, name) for name, key in _SECTOR_NAME_TO_KEY.items()
)
SECTOR_SCHEMA_KEYS: tuple[str, ...] = tuple(key for key, _ in SECTOR_KEYS)


@dataclass(frozen=True)
class AnalysisQuestion:
    """One first-level analysis question from the framework CSV."""

    pillar_type: str
    name: str
    question: str
    # Pipeline schema / component key when this row maps to an existing field.
    schema_key: str | None = None


def _schema_key_for(pillar_type: str, name: str) -> str | None:
    pt = pillar_type.strip().lower()
    if pt == "context pillar":
        return _CONTEXT_NAME_TO_KEY.get(name)
    if pt == "situation analysis pillar":
        return _SITUATION_NAME_TO_COMPONENT.get(name)
    if pt == "sector":
        return _SECTOR_NAME_TO_KEY.get(name)
    return None


def load_analysis_questions(csv_path: Path | str | None = None) -> list[AnalysisQuestion]:
    """Read the first-level questions CSV (no Answer column)."""
    path = Path(csv_path) if csv_path else _DEFAULT_CSV
    rows: list[AnalysisQuestion] = []
    with path.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {"Pillar Type", "Name", "Analysis Questions"}
        if not reader.fieldnames or not required.issubset(set(reader.fieldnames)):
            raise ValueError(
                f"analysis questions CSV missing required columns {sorted(required)}; "
                f"got {reader.fieldnames!r} in {path}"
            )
        for raw in reader:
            pillar_type = (raw.get("Pillar Type") or "").strip()
            name = (raw.get("Name") or "").strip()
            question = (raw.get("Analysis Questions") or "").strip()
            if not (pillar_type and name and question):
                continue
            rows.append(
                AnalysisQuestion(
                    pillar_type=pillar_type,
                    name=name,
                    question=question,
                    schema_key=_schema_key_for(pillar_type, name),
                )
            )
    return rows


@lru_cache(maxsize=1)
def _cached_questions() -> tuple[AnalysisQuestion, ...]:
    return tuple(load_analysis_questions())


def get_analysis_questions() -> list[AnalysisQuestion]:
    """Cached default-CSV load used by generators at runtime."""
    return list(_cached_questions())


def question_for_schema_key(schema_key: str) -> AnalysisQuestion | None:
    """Return the analysis question mapped to a pipeline schema key."""
    for q in get_analysis_questions():
        if q.schema_key == schema_key:
            return q
    return None


def context_questions_by_key() -> dict[str, AnalysisQuestion]:
    """``demographics`` → question, for every mapped context pillar."""
    return {
        q.schema_key: q
        for q in get_analysis_questions()
        if q.schema_key and q.pillar_type.lower() == "context pillar"
    }


def situation_questions() -> list[AnalysisQuestion]:
    """Situation-analysis pillar questions (displacement, shock, access)."""
    return [
        q
        for q in get_analysis_questions()
        if q.pillar_type.lower() == "situation analysis pillar"
    ]


def sector_question_for(schema_key: str) -> AnalysisQuestion | None:
    """Analysis question for one SAF sector key (e.g. ``food_security``)."""
    q = question_for_schema_key(schema_key)
    if q is not None and q.pillar_type.lower() == "sector":
        return q
    return None


def rag_query_for(
    country_name: str,
    question: AnalysisQuestion | None,
    *,
    fallback: str = "",
) -> str:
    """Build a knowledgebase search query from an analysis question.

    The CSV question text is the retrieval input; the country name scopes
    the semantic match geographically. Falls back to ``fallback`` only when
    no question is mapped.
    """
    if question is not None and question.question:
        return f"{country_name} {question.question}".strip()
    return f"{country_name} {fallback}".strip()


def rag_query_for_schema_key(
    country_name: str,
    schema_key: str,
    *,
    fallback: str = "",
) -> str:
    """Convenience: look up the CSV question for ``schema_key``, then
    ``rag_query_for``."""
    return rag_query_for(
        country_name,
        question_for_schema_key(schema_key),
        fallback=fallback or schema_key.replace("_", " "),
    )


def format_question_block(question: AnalysisQuestion | None, *, fallback_topic: str) -> str:
    """Minimal prompt fragment: the analysis question, nothing else."""
    if question is None:
        return f"QUESTION: {fallback_topic}"
    return f"QUESTION ({question.name}):\n{question.question}"
