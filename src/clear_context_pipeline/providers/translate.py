"""Single-call translator. Asks the model for every target locale at once so an
entity with N translatable fields × M target locales costs one call, not N×M.
The model returns JSON keyed by locale, mirroring the canonical shape per locale.

Ported from clear-pipeline services/translate.py; the Claude client is swapped
for ``make_llm_provider("translate")`` and GraphQL goes through
``clear_context_pipeline.providers.clear_api``. Draining the translation queue
(``pending_translations``) and calling ``translate_and_upsert`` per entity is the
translate stage's job (see defs/signals/stages.py).
"""

from __future__ import annotations

import json
import logging
from typing import Any, Iterable

from clear_context_pipeline.providers import clear_api
from clear_context_pipeline.providers.llm import make_llm_provider
from clear_context_pipeline.providers.redis_lock import redis_lock
from clear_context_pipeline.providers.translation_hash import (
    compute_source_hashes,
    stale_fields,
)
from clear_context_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

# Per-entity dedup lock — prevents the same (entity_type, entity_id) from being
# translated by two concurrent drains. TTL covers the worst-case call duration.
_TRANSLATE_LOCK_TTL_SECONDS = 360  # 6 min

# locale code → human-readable name used in the prompt so the model translates
# into the right variety (e.g. "Arabic — Modern Standard" vs dialect drift).
LOCALE_LABELS: dict[str, str] = {
    "ar": "Arabic (Modern Standard, MSA)",
    "fr": "French",
}

TRANSLATION_PROMPT_VERSION = "v1"

# Nested crisis translations (needs, scenarios) blow past small caps; non-Latin
# scripts inflate output tokens ~1.5–2x. 16384 covers 2 locales × the heaviest
# crisis we emit.
_TRANSLATE_MAX_TOKENS = 16384


def _system_prompt() -> str:
    return (
        "You are a professional translator for humanitarian crisis content "
        "produced by the Norwegian Refugee Council (NRC). You will be given "
        "a JSON object describing one entity (an event, a crisis, or an "
        "admin location) and asked to translate selected fields into one or "
        "more target languages.\n"
        "\n"
        "Rules:\n"
        "- Preserve every JSON key exactly. Only translate string values.\n"
        "- When a value is itself a JSON object or array, recurse: keep its "
        "  shape exactly and translate the string leaves.\n"
        "- Preserve technical terminology, NRC SAF sector names "
        "  (Shelter, WASH, Protection, Health, Food Security, Education), "
        "  glide codes, proper nouns, place names, dates, numbers, and "
        "  acronyms unchanged unless the locale has an established "
        "  convention (e.g. WHO → منظمة الصحة العالمية is acceptable).\n"
        "- Output VALID JSON only. No commentary, no markdown fences.\n"
        "- For each target locale, return an object whose keys are the "
        "  same field names you were asked to translate, with the "
        "  translated values (matching the canonical shape per field).\n"
        "- Top-level shape: {\"<locale>\": {<field>: <translated_value>, ...}, ...}"
    )


def _build_user_prompt(
    entity_type: str,
    canonical: dict[str, Any],
    target_locales: list[str],
    fields_to_translate: list[str],
) -> str:
    fields_payload = {f: canonical.get(f) for f in fields_to_translate}
    locale_descriptions = "\n".join(
        f"  - {code}: {LOCALE_LABELS.get(code, code)}" for code in target_locales
    )
    return (
        f"Entity type: {entity_type}\n"
        f"Target locales:\n{locale_descriptions}\n"
        f"Fields to translate (canonical English, JSON):\n"
        f"{json.dumps(fields_payload, ensure_ascii=False, indent=2)}\n"
        "\n"
        "Return JSON with one key per target locale code. Each value is an "
        "object containing the translated fields, with the SAME keys and "
        "the SAME nested shape as the canonical input above."
    )


def _parse_json(text: str) -> dict[str, Any] | None:
    """Parse the model's JSON body, tolerating a ```json code fence."""
    s = text.strip()
    if s.startswith("```"):
        s = s[3:]
        if s[:4].lower() == "json":
            s = s[4:]
        if s.endswith("```"):
            s = s[:-3]
        s = s.strip()
    try:
        parsed = json.loads(s)
    except (json.JSONDecodeError, ValueError):
        return None
    return parsed if isinstance(parsed, dict) else None


def translate_entity(
    entity_type: str,
    canonical: dict[str, Any],
    target_locales: Iterable[str],
    fields_to_translate: Iterable[str],
    *,
    entity_id: str | None = None,
) -> dict[str, dict[str, Any]] | None:
    """Translate the requested fields into every target locale in one call.

    Returns ``{locale: {field: translated_value}}`` (exactly the shape each
    upsert row's ``data`` expects), or None when there's nothing to translate or
    the model returns unparseable output (logged, non-fatal). Provider transport
    errors propagate so the translate stage can isolate/retry per entity.
    """
    target_locales = [loc for loc in target_locales if loc and loc != "en"]
    fields_to_translate = list(fields_to_translate)
    if not target_locales or not fields_to_translate:
        return None

    text = make_llm_provider("translate").complete_text(
        system=_system_prompt(),
        user=_build_user_prompt(entity_type, canonical, target_locales, fields_to_translate),
        max_tokens=_TRANSLATE_MAX_TOKENS,
    )
    result = _parse_json(text)
    if result is None:
        logger.error(
            "[TRANSLATE] %s %s: could not parse model JSON (%d locales × %d fields)",
            entity_type, entity_id, len(target_locales), len(fields_to_translate),
        )
        return None

    # A missing locale or non-dict entry is a quality issue — log and keep the
    # rest so one bad locale doesn't drop the others.
    out: dict[str, dict[str, Any]] = {}
    for locale in target_locales:
        locale_data = result.get(locale)
        if not isinstance(locale_data, dict):
            logger.warning(
                "[TRANSLATE] %s %s: locale %s missing/non-object — skipping",
                entity_type, entity_id, locale,
            )
            continue
        out[locale] = locale_data
    return out or None


def configured_target_locales() -> list[str]:
    """`target_locales` from settings, 'en' stripped (canonical is never a
    target), lowercased."""
    raw = settings.target_locales or ""
    parsed = [code.strip().lower() for code in raw.split(",") if code.strip()]
    return [code for code in parsed if code != "en"]


# translate_and_upsert outcomes. The translate stage relies on these to know
# whether the entity's queue rows were CLEARED (so the row can't re-drain and
# re-invoke the paid LLM every run). Only LOCKED leaves rows queued — and a peer
# holds the lock, so it will clear them.
TRANSLATED = "translated"    # LLM ran + upsert → rows cleared
NOOP = "noop"                # nothing stale (or disabled) → rows cleared
UNPARSEABLE = "unparseable"  # model output unusable → rows cleared (dropped) so it can't re-loop
LOCKED = "locked"            # a peer holds the lock → rows left queued (transient)


def translate_and_upsert(
    entity_type: str,
    entity_id: str,
    canonical: dict[str, Any],
) -> str:
    """Translate ``canonical`` into every configured target locale and upsert via
    clear-api ``upsertTranslations`` (which clears the queue rows). Skips the LLM
    when translation is disabled or every locale is already current.

    Returns one of ``TRANSLATED`` / ``NOOP`` / ``UNPARSEABLE`` / ``LOCKED`` — the
    stage treats everything except ``LOCKED`` as "rows cleared". A short-TTL Redis
    dedup lock keeps two concurrent drains off the same entity.
    """
    target_locales = configured_target_locales()
    if not target_locales:
        return NOOP

    lock_key = f"translate:{entity_type}:{entity_id}"
    with redis_lock(lock_key, ttl_seconds=_TRANSLATE_LOCK_TTL_SECONDS, wait_seconds=0) as acquired:
        if not acquired:
            logger.info(
                "[TRANSLATE] %s %s: another worker holds the lock — leaving queued",
                entity_type, entity_id,
            )
            return LOCKED
        return _translate_and_upsert_locked(entity_type, entity_id, canonical)


def _translate_and_upsert_locked(
    entity_type: str,
    entity_id: str,
    canonical: dict[str, Any],
) -> str:
    target_locales = configured_target_locales()
    if not target_locales:
        return NOOP

    fresh_hashes = compute_source_hashes(entity_type, canonical)
    stored = {
        row["locale"]: row
        for row in clear_api.get_translations(entity_type, entity_id)
    }

    # Per-locale stale set. A locale with no stored row is fully stale.
    per_locale_stale: dict[str, list[str]] = {}
    for locale in target_locales:
        stored_hashes = (stored.get(locale) or {}).get("sourceHashes")
        fields = stale_fields(fresh_hashes, stored_hashes)
        if fields:
            per_locale_stale[locale] = fields

    if not per_locale_stale:
        logger.info(
            "[TRANSLATE] %s %s: all %d locale(s) current — skipping model",
            entity_type, entity_id, len(target_locales),
        )
        _clear_queue(entity_type, entity_id, target_locales)
        return NOOP

    union_fields = sorted({f for fields in per_locale_stale.values() for f in fields})
    translated = translate_entity(
        entity_type,
        canonical,
        target_locales=list(per_locale_stale.keys()),
        fields_to_translate=union_fields,
        entity_id=entity_id,
    )
    if not translated:
        # Model output was unusable. Clear the queue rows so this entity can't
        # re-drain and re-invoke the (paid) model on every run — for events that's
        # harmless (re-enqueued on the next group); the alternative is a poisoned
        # queue head that stalls all translation. Logged as an error above.
        _clear_queue(entity_type, entity_id, target_locales)
        return UNPARSEABLE

    # Merge fresh translations over stored data so fields this pass didn't
    # refresh keep their previous translations. source_hashes always overwrite.
    upsert_rows: list[dict] = []
    for locale, new_fields in translated.items():
        previous_data = (stored.get(locale) or {}).get("data") or {}
        merged_data = {**previous_data, **new_fields}
        upsert_rows.append({
            "locale": locale,
            "data": merged_data,
            "sourceHashes": fresh_hashes,
        })

    if not upsert_rows:
        _clear_queue(entity_type, entity_id, target_locales)
        return NOOP

    clear_api.upsert_translations(entity_type, entity_id, upsert_rows)
    logger.info(
        "[TRANSLATE] %s %s: wrote %d locale(s), %d field(s) max",
        entity_type, entity_id, len(upsert_rows), len(union_fields),
    )
    return TRANSLATED


def _clear_queue(entity_type: str, entity_id: str, locales: list[str]) -> None:
    """Remove an entity's rows from the translation queue (best-effort)."""
    for locale in locales:
        try:
            clear_api.mark_translated(entity_type, entity_id, locale)
        except Exception:  # noqa: BLE001 — queue cleanup must not fail the drain
            pass
