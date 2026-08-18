"""Per-field SHA-256 hashes of canonical English text. Mirrors the TS helper
at clear-api/src/utils/translation-hash.ts exactly so a hash computed here and a
hash computed in clear-api over the same canonical field produce the same digest.

Concretely:
  - String fields hash as raw UTF-8 bytes.
  - JSON / dict / list fields hash as a stable-stringified JSON with sorted keys
    (so {"a":1,"b":2} and {"b":2,"a":1} produce equal hashes).
  - None hashes as a sentinel so an empty field still produces a deterministic
    value.

This file is the source of truth for which fields per entity_type are
translatable. Adding a new translatable field requires updating HASH_FIELDS here
AND the matching dict in the TS helper AND adding a resolver overlay on clear-api.

Ported verbatim from clear-pipeline services/translation_hash.py.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any

# Keep in sync with clear-api/src/utils/translation-hash.ts → HASH_FIELDS.
HASH_FIELDS: dict[str, tuple[str, ...]] = {
    "event":    ("title", "description"),
    "crisis":   ("title", "summary", "scenarios", "needs"),
    "location": ("name",),
    # Situation-analysis prose components. The canonical passed for this type is
    # the PROSE PROJECTION (providers/situation_prose.py → extract_situation_prose),
    # not the raw payload, so these keys hash the prose-only sub-structures. Keep
    # aligned with PROSE_COMPONENTS there.
    "situationAnalysis": (
        "ai_summary",
        "context_risks",
        "hazards_and_vulnerabilities",
        "displacement",
        "sectors",
        "changes",
    ),
}


def _stable_stringify(value: Any) -> str:
    """JSON.stringify with deterministically sorted object keys.

    Mirrors the TS implementation. `json.dumps(..., sort_keys=True,
    separators=(",", ":"))` produces an identical byte sequence to the TS helper
    for any structurally-equal JSON value, so hashes agree.
    """
    if value is None:
        return "null"
    if isinstance(value, str):
        return json.dumps(value, ensure_ascii=False)
    if isinstance(value, (bool, int, float)):
        return json.dumps(value, ensure_ascii=False)
    # dict / list: sort_keys recurses for nested dicts; lists preserve order.
    return json.dumps(
        value,
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )


def _hash_value(value: Any) -> str:
    """SHA-256 of a single field. Matches TS hashValue()."""
    h = hashlib.sha256()
    if value is None:
        h.update(b"\x00null")
    elif isinstance(value, str):
        h.update(value.encode("utf-8"))
    else:
        h.update(_stable_stringify(value).encode("utf-8"))
    return f"sha256:{h.hexdigest()}"


def compute_source_hashes(entity_type: str, canonical: dict[str, Any]) -> dict[str, str]:
    """Build {field: 'sha256:...'} for every translatable field of the given
    entity. Fields not present on `canonical` are still hashed (as null) so the
    result has a stable shape per entity_type — the staleness check relies on that.
    """
    if entity_type not in HASH_FIELDS:
        raise ValueError(
            f"Unknown entity_type {entity_type!r}; expected one of "
            f"{tuple(HASH_FIELDS.keys())}"
        )
    return {
        field: _hash_value(canonical.get(field))
        for field in HASH_FIELDS[entity_type]
    }


def stale_fields(
    current: dict[str, str],
    stored: dict[str, str] | None,
) -> list[str]:
    """Return the field names whose hashes differ between `current`
    (just-computed) and `stored` (from a translation row's source_hashes). When
    `stored` is None, every field is considered stale — the cold-start path."""
    if not stored:
        return list(current.keys())
    return [field for field, hash_ in current.items() if stored.get(field) != hash_]
