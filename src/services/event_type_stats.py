"""Per-event-type statistics lookup (fatalities, population_affected).

Source: clear-research analysis of historical conflict events. Bundled at
`src/data/acled_event_type_stats.json`. Keys are level_3 sub-type names
(matching the taxonomy's `type_level_3` field, e.g. "armed clash",
"peaceful protest") which in turn match ACLED's `sub_event_type` values
verbatim.

Used by the event grouping layer to derive event-level `casualties` (q75 of
fatalities) and `populationAffected` (median of population_1km) from the
incoming signal's classification. Works for any source — ACLED via
`sub_event_type`, Dataminr / GDACS / manual via the classifier's level_3
mapping — as long as the signal classifies into a conflict-related type
that has stats in the file.
"""

import json
import logging
from pathlib import Path
from typing import TypedDict

logger = logging.getLogger(__name__)

_STATS_PATH = Path(__file__).resolve().parent.parent / "data" / "acled_event_type_stats.json"


class _PercentileStats(TypedDict):
    max: float
    median: float
    min: float
    q25: float
    q75: float


class _EventTypeStats(TypedDict, total=False):
    event_count: int
    fatalities: _PercentileStats
    population_1km: _PercentileStats
    population_2km: _PercentileStats
    population_5km: _PercentileStats


def _normalize_key(s: str) -> str:
    """Normalise a level_3 / sub_event_type string for lookup.

    JSON keys are lowercase with " / " around slashes (e.g.
    "abduction / forced disappearance"); ACLED's API returns
    "Abduction/forced disappearance". Normalise both to the JSON form.
    """
    if not s:
        return ""
    lowered = s.strip().lower().replace("/", " / ")
    return " ".join(lowered.split())


def _load_stats() -> dict[str, _EventTypeStats]:
    try:
        with _STATS_PATH.open("r", encoding="utf-8") as f:
            raw: dict[str, _EventTypeStats] = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.error("[EVENT-STATS] Failed to load %s: %s", _STATS_PATH, e)
        return {}

    return {_normalize_key(k): v for k, v in raw.items()}


# Loaded once at module import — small file (~24 entries).
_STATS: dict[str, _EventTypeStats] = _load_stats()


class EventTypeStats(TypedDict):
    casualties: int | None
    population_affected: int | None


def get_stats_for_event_type(event_type: str) -> EventTypeStats:
    """Return (q75 fatalities, median population_1km) for a level_3 / sub-type.

    Both values may be None when the event_type is unknown to the stats file
    (e.g. natural-hazard types — flood, earthquake — currently have no
    entries) or when the stat itself is 0/None. Callers should treat None
    as "no fallback available" and skip the field rather than write 0.
    """
    if not event_type:
        return {"casualties": None, "population_affected": None}

    key = _normalize_key(event_type)
    stats = _STATS.get(key)
    if not stats:
        logger.debug(
            "[EVENT-STATS] No stats for event_type=%r (key=%r)",
            event_type, key,
        )
        return {"casualties": None, "population_affected": None}

    fatalities = stats.get("fatalities") or {}
    pop_1km = stats.get("population_1km") or {}

    q75_fatalities = fatalities.get("q75")
    median_pop = pop_1km.get("median")

    return {
        "casualties": int(q75_fatalities) if q75_fatalities and q75_fatalities > 0 else None,
        "population_affected": int(median_pop) if median_pop and median_pop > 0 else None,
    }
