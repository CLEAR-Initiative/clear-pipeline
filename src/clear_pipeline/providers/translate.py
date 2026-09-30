"""Single-call translator. Asks the model for every target locale at once so an
entity with N translatable fields × M target locales costs one call, not N×M.
The model returns JSON keyed by locale, mirroring the canonical shape per locale.

Ported from clear-pipeline services/translate.py; the Claude client is swapped
for ``make_llm_provider("translate")`` and GraphQL goes through
``clear_pipeline.providers.clear_api``. Draining the translation queue
(``pending_translations``) and calling ``translate_and_upsert`` per entity is the
translate stage's job (see defs/signals/stages.py).

Hotline messages (``groundMessage``) are the exception to "English in, every
configured locale out": they are translated ON DEMAND, only into the locales a
reviewer asked for, from whatever language the reporter wrote in (``en``
included as a target). See ``translate_ground_message``.
"""

from __future__ import annotations

import json
import logging
import re
from typing import Any, Iterable

from clear_pipeline.providers import clear_api
from clear_pipeline.providers.llm import make_llm_provider
from clear_pipeline.providers.redis_lock import redis_lock
from clear_pipeline.providers.translation_hash import (
    compute_source_hashes,
    stale_fields,
)
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

# Per-entity dedup lock — prevents the same (entity_type, entity_id) from being
# translated by two concurrent drains. TTL covers the worst-case call duration.
_TRANSLATE_LOCK_TTL_SECONDS = 360  # 6 min

# locale code → human-readable name used in the prompt so the model translates
# into the right variety (e.g. "Arabic — Modern Standard" vs dialect drift).
LOCALE_LABELS: dict[str, str] = {
    "ar": "Arabic (Modern Standard, MSA)",
    "fr": "French",
    "es": "Spanish (Latin American)",
    # Only ever a target for on-demand (source-language) entity types — the
    # bulk entity types are authored in English.
    "en": "English",
}

# Entity types translated only when a reviewer asks (clear-api
# requestGroundMessageTranslation), into exactly the requested locales, from
# the author's own language. Everything else is English-canonical and goes to
# every configured target locale.
ON_DEMAND_ENTITY_TYPES: frozenset[str] = frozenset({"groundMessage"})

# clear-api intake detection codes (utils/language-detect.ts) → prompt names.
_SOURCE_LANGUAGE_NAMES: dict[str, str] = {
    "ar": "Arabic",
    "en": "English",
    "fr": "French",
    "es": "Spanish",
}

# Must survive a hotline translation untouched. The phone placeholder is
# clear-api's PHONE_REDACTION_PLACEHOLDER (services/whatsapp-export.ts); the
# uncertainty tags mirror its UNCERTAINTY_MARKERS. A number was already
# stripped at ingest, so a dropped marker isn't a leak — but it silently
# changes what the reporter said, as does a dropped "unconfirmed".
PHONE_REDACTION_MARKER = "[phone redacted]"
UNCERTAINTY_TAGS: tuple[str, ...] = ("unconfirmed", "rumour", "rumor", "unverified", "not verified")

# A hotline message is a few sentences; Arabic → English output is well under
# this. Truncation fails the marker/JSON checks and drops the request, never
# writes a partial translation.
_GROUND_MAX_TOKENS = 4096

TRANSLATION_PROMPT_VERSION = "v1"

# Nested crisis translations (needs, scenarios) blow past small caps; non-Latin
# scripts inflate output tokens ~1.5–2x. Situation-analysis prose (summary +
# 8 risk domains + hazards + displacement + 6 sectors + change notes) is far
# larger than any crisis, so this cap covers a few locales × the heaviest
# situation payload. If a payload ever truncates, `_parse_json` fails and the
# drain re-queues it — it never corrupts a partial write.
_TRANSLATE_MAX_TOKENS = 32768


def _system_prompt() -> str:
    return (
        "You are a professional translator for humanitarian crisis content "
        "produced by the Norwegian Refugee Council (NRC). You will be given "
        "a JSON object describing one entity (an event, a crisis, an admin "
        "location, or a country situation analysis) and asked to translate "
        "selected fields into one or more target languages.\n"
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


def _ground_system_prompt() -> str:
    tags = ", ".join(f'"{t}"' for t in UNCERTAINTY_TAGS)
    return (
        "You translate short messages that members of the public send to a "
        "humanitarian hotline run by the Norwegian Refugee Council (NRC) in "
        "crisis zones (Sudan and neighbours). Messages are informal and often "
        "dialect (e.g. Sudanese Arabic), misspelt, or mix languages. A "
        "reviewer reads your translation beside the original to decide "
        "whether the report is real.\n"
        "\n"
        "Rules:\n"
        "- Translate from whatever language the message is in. You are told "
        "the detected source language when known; it is a hint from a "
        "simple detector, so trust the text if they disagree.\n"
        "- Translate faithfully and completely. Do not summarise, correct, "
        "soften, sharpen, or add anything; keep the reporter's hedging and "
        "certainty exactly as strong as it is.\n"
        f"- Copy these tokens exactly as written, untranslated, every time "
        f"they occur: \"{PHONE_REDACTION_MARKER}\" and the uncertainty "
        f"tags {tags}.\n"
        "- Keep place names, names, numbers, dates and acronyms as they are "
        "(transliterate names into the target script only when needed).\n"
        "- If part of the message is already in the target language, keep "
        "that part as is.\n"
        "- Output VALID JSON only: an object mapping each requested locale "
        "code to its translated text, e.g. {\"en\": \"...\"}. No "
        "commentary, no markdown fences."
    )


def _ground_user_prompt(text: str, source_language: str | None, target_locales: list[str]) -> str:
    source = (
        _SOURCE_LANGUAGE_NAMES.get(source_language, source_language)
        if source_language
        else "unknown — identify it from the text"
    )
    targets = "\n".join(f"  - {code}: {LOCALE_LABELS.get(code, code)}" for code in target_locales)
    return (
        f"Detected source language: {source}\n"
        f"Target locales:\n{targets}\n"
        f"Message:\n{json.dumps(text, ensure_ascii=False)}"
    )


def _marker_counts(text: str) -> dict[str, int]:
    lower = text.lower()
    counts = {PHONE_REDACTION_MARKER: lower.count(PHONE_REDACTION_MARKER)}
    for tag in UNCERTAINTY_TAGS:
        # Left word boundary only, so "rumours" still counts as "rumour".
        counts[tag] = len(re.findall(rf"(?<![a-z]){re.escape(tag)}", lower))
    return counts


def preserves_markers(source: str, translated: str) -> bool:
    """True when every phone-redaction marker and uncertainty tag in
    ``source`` survives in ``translated`` (at least as many occurrences)."""
    have = _marker_counts(translated)
    return all(have[k] >= n for k, n in _marker_counts(source).items() if n)


def translate_ground_message(
    canonical: dict[str, Any],
    target_locales: Iterable[str],
    *,
    entity_id: str | None = None,
) -> dict[str, dict[str, Any]] | None:
    """Translate one hotline message's ``text`` into each target locale.

    ``canonical`` is ``{text, language}`` from clear-api's
    ``groundMessageForTranslation`` — the reporter's original words and the
    intake-detected language (None when unknown). Any supported locale is a
    valid target, ``en`` included. Runs on the cheap ``signal`` role: one
    short message per call.

    Returns ``{locale: {"text": translated}}`` (the upsert rows' ``data``), or
    None when there is no text, the output is unparseable, or no locale kept
    its redaction markers and uncertainty tags (each failure logged). A locale
    that loses a marker is dropped rather than written.
    """
    text = (canonical.get("text") or "").strip()
    target_locales = [loc for loc in target_locales if loc]
    if not text or not target_locales:
        return None

    raw = make_llm_provider("signal").complete_text(
        system=_ground_system_prompt(),
        user=_ground_user_prompt(text, canonical.get("language"), target_locales),
        max_tokens=_GROUND_MAX_TOKENS,
    )
    result = _parse_json(raw)
    if result is None:
        logger.error("[TRANSLATE] groundMessage %s: could not parse model JSON", entity_id)
        return None

    out: dict[str, dict[str, Any]] = {}
    for locale in target_locales:
        translated = result.get(locale)
        if not isinstance(translated, str) or not translated.strip():
            logger.warning(
                "[TRANSLATE] groundMessage %s: locale %s missing/empty — skipping", entity_id, locale,
            )
            continue
        if not preserves_markers(text, translated):
            logger.error(
                "[TRANSLATE] groundMessage %s: locale %s dropped a redaction marker or "
                "uncertainty tag — not writing it", entity_id, locale,
            )
            continue
        out[locale] = {"text": translated.strip()}
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


def _target_locales(entity_type: str, requested_locales: Iterable[str] | None) -> list[str]:
    """Locales to translate this entity into. On-demand types get exactly the
    queued (requested) locales — ``en`` included, since their source isn't
    English; everything else gets every configured target locale."""
    if entity_type in ON_DEMAND_ENTITY_TYPES:
        return sorted({loc.strip().lower() for loc in requested_locales or () if loc and loc.strip()})
    return configured_target_locales()


def translate_and_upsert(
    entity_type: str,
    entity_id: str,
    canonical: dict[str, Any],
    requested_locales: Iterable[str] | None = None,
) -> str:
    """Translate ``canonical`` and upsert via clear-api ``upsertTranslations``
    (which clears the queue rows). Skips the LLM when translation is disabled or
    every target locale is already current.

    Targets are every configured locale, except for on-demand types
    (``groundMessage``), which get only ``requested_locales`` — the locales
    their queue rows asked for — and are never gated on ``target_locales``.

    Returns one of ``TRANSLATED`` / ``NOOP`` / ``UNPARSEABLE`` / ``LOCKED`` — the
    stage treats everything except ``LOCKED`` as "rows cleared". A short-TTL Redis
    dedup lock keeps two concurrent drains off the same entity.
    """
    target_locales = _target_locales(entity_type, requested_locales)
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
        return _translate_and_upsert_locked(entity_type, entity_id, canonical, target_locales)


def _translate_and_upsert_locked(
    entity_type: str,
    entity_id: str,
    canonical: dict[str, Any],
    target_locales: list[str],
) -> str:
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
    if entity_type in ON_DEMAND_ENTITY_TYPES:
        translated = translate_ground_message(
            canonical, list(per_locale_stale.keys()), entity_id=entity_id,
        )
    else:
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
    if entity_type in ON_DEMAND_ENTITY_TYPES:
        # A requested locale the model failed (dropped marker, missing key) is
        # dropped, not left queued: re-draining it would re-bill the model every
        # run. It reads as "unavailable" and the reviewer can ask again.
        _clear_queue(entity_type, entity_id, [loc for loc in per_locale_stale if loc not in translated])
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
