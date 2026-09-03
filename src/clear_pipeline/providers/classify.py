"""Signal classification provider — local event classifier + singleton + classify_locally.

Consolidated from clear-pipeline's event_classifier / classifier_singleton /
local_classify. The v2 grouping algorithm's local (sentence-transformer)
classifier, its lazily-loaded singleton, and the SignalClassification result
model, all in one file.
"""

from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from pydantic import BaseModel

logger = logging.getLogger(__name__)


class SignalClassification(BaseModel):
    """Output from signal classification (disaster types, relevance, severity)."""

    disaster_types: list[str]  # glide numbers e.g. ["fl", "ff"]
    relevance: float  # 0.0-1.0 (== the classifier's confidence)
    severity: int  # 1-5
    summary: str
    # Full taxonomy prediction from the local classifier, carried so the grouping
    # stage can reuse it instead of running a second inference on the same text.
    # ``disaster_types[0]`` is the glide code; ``relevance`` is the confidence.
    type_level_1: str | None = None
    type_level_2: str | None = None
    type_level_3: str | None = None


# ── Event classifier (sentence-transformer taxonomy matcher) ─────────────────
DEFAULT_TAXONOMY_PATH = Path(__file__).resolve().parent / 'event_categories.json'
DEFAULT_MODEL_NAME = 'sentence-transformers/all-MiniLM-L6-v2'


@dataclass(frozen=True)
class EventCategory:
    type_level_1: str
    type_level_2: str
    type_level_3: str
    id: str
    id_type: str
    key_words: list[str]
    key_phrases: list[str]
    prototype: str


class EventClassifier:
    def __init__(
        self,
        taxonomy_path: str | Path = DEFAULT_TAXONOMY_PATH,
        model_name: str = DEFAULT_MODEL_NAME,
        lexical_weight: float = 0.65,
        semantic_weight: float = 0.35,
        confidence_threshold: float = 0.42,
    ) -> None:
        self.taxonomy_path = Path(taxonomy_path)
        self.lexical_weight = lexical_weight
        self.semantic_weight = semantic_weight
        self.confidence_threshold = confidence_threshold
        self._validate_weights()

        self._np = self._load_numpy()
        self._fuzz = self._load_rapidfuzz()
        self._embedding_model = self._load_embedding_model(model_name)

        self.categories = self._load_taxonomy(self.taxonomy_path)
        self._category_embeddings = self._encode_category_prototypes(
            self.categories
        )

    def _validate_weights(self) -> None:
        if self.lexical_weight < 0 or self.semantic_weight < 0:
            msg = 'lexical_weight and semantic_weight must be non-negative.'
            raise ValueError(msg)
        if self.lexical_weight == 0 and self.semantic_weight == 0:
            msg = 'At least one of lexical_weight or semantic_weight must be > 0.'
            raise ValueError(msg)

    @staticmethod
    def _load_numpy() -> Any:
        try:
            import numpy as np
        except ImportError as exc:
            msg = (
                'Missing dependency `numpy`. '
                'Install with `uv add numpy` or `pip install numpy`.'
            )
            raise ImportError(msg) from exc
        return np

    @staticmethod
    def _load_rapidfuzz() -> Any:
        try:
            from rapidfuzz import fuzz
        except ImportError as exc:
            msg = (
                'Missing dependency `rapidfuzz`. '
                'Install with `uv add rapidfuzz` or `pip install rapidfuzz`.'
            )
            raise ImportError(msg) from exc
        return fuzz

    @staticmethod
    def _load_embedding_model(model_name: str) -> Any:
        try:
            from sentence_transformers import SentenceTransformer
        except ImportError as exc:
            msg = (
                'Missing dependency `sentence-transformers`. '
                'Install with `uv add sentence-transformers` '
                'or `pip install sentence-transformers`.'
            )
            raise ImportError(msg) from exc
        return SentenceTransformer(model_name)

    @staticmethod
    def normalize_text(text: str) -> str:
        clean = text.lower()
        clean = re.sub(r'http\S+|www\.\S+', ' ', clean)
        clean = re.sub(r'[@#]\w+', ' ', clean)
        clean = re.sub(r"[^a-z0-9\s/\-']", ' ', clean)
        clean = re.sub(r'\s+', ' ', clean).strip()
        return clean

    @staticmethod
    def _build_prototype(record: dict[str, Any]) -> str:
        type_level_1 = str(record.get('type_level_1', ''))
        type_level_2 = str(record.get('type_level_2', ''))
        type_level_3 = str(record.get('type_level_3', ''))
        key_phrases = [str(x) for x in record.get('key_phrases', [])]
        key_words = [str(x) for x in record.get('key_words', [])]
        prototype_parts = [
            type_level_1,
            type_level_2,
            type_level_3,
            *key_phrases[:8],
            *key_words[:12],
        ]
        return ' | '.join(part for part in prototype_parts if part)

    def _load_taxonomy(self, taxonomy_path: Path) -> list[EventCategory]:
        data = json.loads(taxonomy_path.read_text(encoding='utf-8'))
        categories: list[EventCategory] = []
        for record in data:
            categories.append(
                EventCategory(
                    type_level_1=str(record['type_level_1']),
                    type_level_2=str(record['type_level_2']),
                    type_level_3=str(record['type_level_3']),
                    id=str(record['id']),
                    id_type=str(record['id_type']),
                    key_words=[
                        str(word).lower()
                        for word in record.get('key_words', [])
                    ],
                    key_phrases=[
                        str(phrase).lower()
                        for phrase in record.get('key_phrases', [])
                    ],
                    prototype=self._build_prototype(record),
                )
            )
        return categories

    def _encode_category_prototypes(
        self, categories: list[EventCategory]
    ) -> Any:
        prototypes = [category.prototype for category in categories]
        return self._embedding_model.encode(
            prototypes,
            normalize_embeddings=True,
        )

    def _lexical_score(self, text: str, category: EventCategory) -> float:
        phrase_hits = sum(
            1 for phrase in category.key_phrases if phrase in text
        )

        keyword_hits = 0
        for keyword in category.key_words:
            if re.search(rf'\b{re.escape(keyword)}\b', text):
                keyword_hits += 1

        base_hint = f'{category.type_level_3} {" ".join(category.key_phrases[:2])}'.strip()
        fuzzy = self._fuzz.token_set_ratio(text, base_hint) / 100.0

        raw = (1.5 * phrase_hits) + keyword_hits
        bounded = 1.0 - self._np.exp(-(raw / 4.0))
        return float((0.7 * bounded) + (0.3 * fuzzy))

    def _semantic_scores(self, text: str) -> Any:
        text_embedding = self._embedding_model.encode(
            [text],
            normalize_embeddings=True,
        )[0]
        return self._category_embeddings @ text_embedding

    def _blend_scores(self, lexical_scores: Any, semantic_scores: Any) -> Any:
        total_weight = self.lexical_weight + self.semantic_weight
        lexical_part = self.lexical_weight / total_weight
        semantic_part = self.semantic_weight / total_weight
        return (lexical_part * lexical_scores) + (
            semantic_part * semantic_scores
        )

    def predict(self, text: str, top_k: int = 1) -> dict[str, Any]:
        normalized_text = self.normalize_text(text)
        lexical_scores = self._np.array(
            [
                self._lexical_score(normalized_text, category)
                for category in self.categories
            ]
        )
        semantic_scores = self._semantic_scores(normalized_text)
        final_scores = self._blend_scores(lexical_scores, semantic_scores)

        top_indexes = self._np.argsort(-final_scores)[: max(top_k, 1)]
        predictions = []
        for index in top_indexes:
            category = self.categories[int(index)]
            predictions.append(
                {
                    'type_level_1': category.type_level_1,
                    'type_level_2': category.type_level_2,
                    'type_level_3': category.type_level_3,
                    'id': category.id,
                    'score': round(float(final_scores[index]), 4),
                    'lexical': round(float(lexical_scores[index]), 4),
                    'semantic': round(float(semantic_scores[index]), 4),
                }
            )

        best = predictions[0]
        label = (
            best['type_level_3']
            if best['score'] >= self.confidence_threshold
            else 'other'
        )
        return {
            'input': text,
            'normalized': normalized_text,
            'label': label,
            'confidence': best['score'],
            'top_k': predictions,
        }

    def predict_batch(
        self, texts: list[str], top_k: int = 3
    ) -> list[dict[str, Any]]:
        return [self.predict(text=text, top_k=top_k) for text in texts]


def _demo() -> None:
    classifier = EventClassifier()
    examples = [
        '7.1 magnitude quake hits coastal Peru, tsunami warning issued',
        'Police used tear gas as protests turned violent in the capital',
        'Prices surge as currency collapses, deepening cost of living crisis',
        'Unknown incident reported by local media',
    ]
    for sample in examples:
        result = classifier.predict(sample, top_k=3)
        print('=' * 80)
        print(f'Text: {result["input"]}')
        print(f'Label: {result["label"]} (confidence={result["confidence"]})')
        for candidate in result['top_k']:
            print(
                f'  - {candidate["type_level_3"]}: '
                f'score={candidate["score"]} '
                f'(lex={candidate["lexical"]}, sem={candidate["semantic"]})'
            )


if __name__ == '__main__':
    _demo()

# ── Classifier singleton + taxonomy maps ─────────────────────────────────────

_classifier: Any | None = None
_taxonomy: list[dict] | None = None

_TAXONOMY_PATH = Path(__file__).resolve().parent / "event_categories.json"


def _load_taxonomy() -> list[dict]:
    global _taxonomy
    if _taxonomy is None:
        _taxonomy = json.loads(_TAXONOMY_PATH.read_text(encoding="utf-8"))
    return _taxonomy


def get_classifier():
    """Return the singleton EventClassifier instance (lazy init on first call)."""
    global _classifier
    if _classifier is None:
        logger.info("[CLASSIFIER] Loading event classifier (first call)…")
        # Imported lazily so workers that never call Claude-less classification
        # don't pay the torch / sentence-transformers import cost.

        _classifier = EventClassifier()
        logger.info(
            "[CLASSIFIER] Loaded with %d categories",
            len(_classifier.categories),
        )
    return _classifier


def code_to_level2_map() -> dict[str, str]:
    """Glide code → level_2 group name (e.g. 'fl' → 'flood', 'pp' → 'protests')."""
    taxonomy = _load_taxonomy()
    out: dict[str, str] = {}
    for row in taxonomy:
        code = row.get("id")
        l2 = row.get("type_level_2")
        if code and l2:
            out[code] = l2
    return out


def level2_to_codes_map() -> dict[str, list[str]]:
    """Level_2 group name → list of glide codes (multiple when level_3 codes
    are distinct, e.g. 'protests' → ['pp','pi','pf'])."""
    taxonomy = _load_taxonomy()
    out: dict[str, list[str]] = {}
    for row in taxonomy:
        code = row.get("id")
        l2 = row.get("type_level_2")
        if not (code and l2):
            continue
        out.setdefault(l2, [])
        if code not in out[l2]:
            out[l2].append(code)
    return out


def code_to_level1_map() -> dict[str, str]:
    """Glide code → level_1 category name (e.g. 'ba' → 'conflict')."""
    taxonomy = _load_taxonomy()
    out: dict[str, str] = {}
    for row in taxonomy:
        code = row.get("id")
        l1 = row.get("type_level_1")
        if code and l1:
            out[code] = l1
    return out


def code_to_level3_map() -> dict[str, str]:
    """Glide code → level_3 sub-type name (e.g. 'ba' → 'armed clash',
    'pp' → 'peaceful protest'). Used for the per-event-type stats lookup
    (acled_event_type_stats.json), whose keys are the level_3 strings."""
    taxonomy = _load_taxonomy()
    out: dict[str, str] = {}
    for row in taxonomy:
        code = row.get("id")
        l3 = row.get("type_level_3")
        if code and l3:
            out[code] = l3
    return out


# ── Level-2 event-type vocabulary (shared with KB enrichment + datapoints) ────
#
# The `disaster_types` table's level_2 column is the ONE authoritative event-type
# vocabulary. `event_categories.json` is the pipeline's local mirror of it; the
# signal classifier already picks a `type_level_2` from it. These helpers expose
# the same level_2 set (+ a coercion) so the LLM extraction paths (enrich.py,
# datapoints) stop emitting free-text tags like 'displacement' / 'search-and-rescue'
# that aren't event types.

# Safe synonym map for LLM tags that unambiguously mean a level_2 label but aren't
# spelled exactly. Anything neither a level_2 value nor here is DROPPED.
_EVENT_TYPE_ALIASES: dict[str, str] = {
    "wildfire": "wild fire",
    "wildfires": "wild fire",
    "wild-fire": "wild fire",
    "bushfire": "wild fire",
    "landslide": "land slide",
    "mudslide": "mud slide",
    "flashflood": "flash flood",
    "flash-flood": "flash flood",
    "quake": "earthquake",
    "cyclone": "tropical cyclone",
    "hurricane": "tropical cyclone",
    "typhoon": "tropical cyclone",
    "outbreak": "epidemic",
    "disease outbreak": "epidemic",
    "disease-outbreak": "epidemic",
    "disease": "epidemic",
    "protest": "protests",
    "riot": "riots",
    "avalanche": "snow avalanche",
    "eruption": "volcano",
    "volcanic eruption": "volcano",
}


def level2_values() -> list[str]:
    """Sorted distinct disaster_types level_2 labels — the authoritative event-type
    vocabulary, mirrored from `event_categories.json` (== clear-api disaster_types
    level_2). Shared by signal classification, KB enrichment, and datapoint extraction."""
    return sorted({
        str(row["type_level_2"]).strip().lower()
        for row in _load_taxonomy()
        if row.get("type_level_2")
    })


def coerce_event_types(values: object) -> object:
    """Map LLM-emitted event-type tags onto the level_2 taxonomy, DROPPING anything
    off-taxonomy. Lowercased, aliased, deduped, order-preserving — so a stray tag
    ('displacement', 'search-and-rescue', …) never becomes a bogus event type.
    A non-list is returned unchanged so pydantic reports it as it would today."""
    if not isinstance(values, list):
        return values
    valid = set(level2_values())
    out: list[str] = []
    for item in values:
        key = str(item).strip().lower()
        canon = key if key in valid else _EVENT_TYPE_ALIASES.get(key)
        if canon and canon in valid and canon not in out:
            out.append(canon)
    return out


# ── classify_locally (v2 entry point) ────────────────────────────────────────

DEFAULT_FALLBACK_SEVERITY = 3  # used when source didn't supply one



def classify_locally(
    title: str | None,
    description: str | None,
    source_severity: int | None = None,
    default_severity: int = DEFAULT_FALLBACK_SEVERITY,
) -> SignalClassification:
    """Build a `SignalClassification` from the local EventClassifier. No
    network calls.

    `source_severity` — the 1-5 severity already attached to the signal by
    its source (Dataminr alertType mapping, GDACS alert level, ACLED
    fatalities). Pass through if present; otherwise fall back to
    `default_severity` so downstream gates (e.g. severity >= 4 alert check)
    still have something to look at.
    """
    classifier = get_classifier()
    text = " ".join(filter(None, [title, description])) or "unknown event"
    pred = classifier.predict(text, top_k=1)
    top = pred["top_k"][0] if pred.get("top_k") else {}

    glide_code: str | None = top.get("id")
    level_1: str | None = top.get("type_level_1")
    level_2: str | None = top.get("type_level_2")
    level_3: str | None = top.get("type_level_3")
    confidence: float = float(pred.get("confidence") or 0.0)

    # Summary is a cheap extraction from the title/description — no LLM
    summary_src = (title or description or "").strip()
    summary = summary_src[:200] if summary_src else (level_3 or "unknown event")

    classification = SignalClassification(
        disaster_types=[glide_code] if glide_code else ["ot"],
        relevance=confidence,
        severity=source_severity if source_severity is not None else default_severity,
        summary=summary,
        type_level_1=level_1,
        type_level_2=level_2,
        type_level_3=level_3,
    )
    logger.info(
        "[LOCAL CLASSIFY] l1=%s l2=%s l3=%s code=%s confidence=%.3f severity=%d (source=%s)",
        level_1, level_2, level_3, glide_code, confidence,
        classification.severity, source_severity,
    )
    return classification
