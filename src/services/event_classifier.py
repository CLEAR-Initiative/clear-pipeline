from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

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
