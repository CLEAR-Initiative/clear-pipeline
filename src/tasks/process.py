"""Celery task: process a single signal through classification → grouping → escalation."""

import json
import logging

import redis

from src.celery_app import app
from src.clients.claude import ClaudeRateLimited, call_claude
from src.clients.graphql import get_dataminr_source_id, get_disaster_types, update_signal_severity, escalate_event
from src.config import settings
from src.models.clear import SignalClassification
from src.models.dataminr import DataminrSignal
from src.prompts.classify import (
    CLASSIFY_PROMPT_VERSION,
    SYSTEM_PROMPT as CLASSIFY_SYSTEM,
    build_classify_prompt,
)
from src.services.alert import assess_and_escalate
from src.services.event import dispatch_group_signal
from src.services.signal import ingest_signal

logger = logging.getLogger(__name__)

_redis = redis.from_url(settings.redis_url, decode_responses=True)

# Cache data source ID and disaster types to avoid repeated lookups
_source_id_cache: str | None = None
_disaster_types_cache: list[dict] | None = None


def _get_source_id() -> str:
    global _source_id_cache
    if _source_id_cache is None:
        _source_id_cache = get_dataminr_source_id()
    return _source_id_cache


def _get_disaster_types() -> list[dict]:
    global _disaster_types_cache
    if _disaster_types_cache is None:
        _disaster_types_cache = get_disaster_types()
    return _disaster_types_cache


@app.task(
    name="src.tasks.process.process_signal",
    bind=True,
    max_retries=2,
    acks_late=True,
)
def process_signal(self, signal_data: dict):
    """
    Process a single Dataminr signal through the full pipeline:
    1. Parse & ingest as CLEAR signal
    2. Classify via Claude (disaster type, relevance, severity)
    3. If relevant: group into event (new or existing)
    4. If high severity: assess for alert escalation
    """
    try:
        signal = DataminrSignal.model_validate(signal_data)
        source_id = _get_source_id()

        # ─── Stage 1: Ingest signal ──────────────────────────────────────────
        created = ingest_signal(signal, source_id)
        signal_id = created["id"]
        logger.info("Signal ingested: %s", signal_id)

        # ─── Stage 2: Classify via Claude ────────────────────────────────────
        # Check cache first
        cache_key = f"classification:{signal_id}"
        cached = _redis.get(cache_key)

        if cached:
            classification = SignalClassification.model_validate_json(cached)
        else:
            # Build context from raw data
            raw_context_parts = []
            if signal.publicPost and signal.publicPost.text:
                raw_context_parts.append(f"Post text: {signal.publicPost.text}")
            if signal.publicPost and signal.publicPost.translatedText:
                raw_context_parts.append(f"Translated: {signal.publicPost.translatedText}")
            if signal.intelAgents:
                for agent in signal.intelAgents:
                    if agent.summary:
                        for s in agent.summary:
                            if s.content:
                                raw_context_parts.append(f"Intel: {s.content[0]}")
            if signal.eventCorroboration and signal.eventCorroboration.summary:
                for s in signal.eventCorroboration.summary:
                    if s.content:
                        raw_context_parts.append(f"Corroboration: {s.content}")
            if signal.liveBrief:
                for lb in signal.liveBrief:
                    if lb.summary:
                        raw_context_parts.append(f"Brief: {lb.summary}")

            raw_context = "\n".join(raw_context_parts) if raw_context_parts else "(no additional context)"

            location_name = None
            if signal.estimatedEventLocation and signal.estimatedEventLocation.name:
                location_name = signal.estimatedEventLocation.name

            disaster_types = _get_disaster_types()

            prompt = build_classify_prompt(
                title=signal.headline,
                description=created.get("title"),
                location_name=location_name,
                url=signal.publicPost.href if signal.publicPost else None,
                timestamp=signal.alertTimestamp,
                raw_context=raw_context,
                disaster_types=disaster_types,
            )

            result_data = call_claude(
                CLASSIFY_SYSTEM,
                prompt,
                stage="classify",
                prompt_version=CLASSIFY_PROMPT_VERSION,
                signal_id=signal_id,
            )
            classification = SignalClassification.model_validate(result_data)

            # Cache classification
            _redis.setex(cache_key, 24 * 3600, classification.model_dump_json())

        logger.info(
            "Signal %s classified: types=%s relevance=%.2f severity=%d",
            signal_id,
            classification.disaster_types,
            classification.relevance,
            classification.severity,
        )

        # Signal severity policy differs between grouping algorithms:
        #   v1 — Claude's classifier score wins (overrides the Dataminr estimate).
        #   v2 — source-provided severity is authoritative; Claude's value is
        #        only used as an event-level fallback and must NOT overwrite
        #        the signal row. A signal with no source severity stays null.
        existing_severity = created.get("severity")
        if settings.grouping_algo == "v1" and existing_severity != classification.severity:
            update_signal_severity(signal_id, classification.severity)
            logger.info(
                "Signal %s severity updated (v1): %s → %d",
                signal_id, existing_severity, classification.severity,
            )

        # ─── Stage 3: Event grouping (if relevant) ──────────────────────────
        if classification.relevance < settings.relevance_threshold:
            logger.info(
                "Signal %s below relevance threshold (%.2f < %.2f), skipping event grouping",
                signal_id,
                classification.relevance,
                settings.relevance_threshold,
            )
            return {
                "signal_id": signal_id,
                "classification": classification.model_dump(),
                "event": None,
                "alert": None,
            }

        location_name = None
        signal_lat = None
        signal_lng = None
        probability_radius_km = None
        if signal.estimatedEventLocation:
            location_name = signal.estimatedEventLocation.name
            if signal.estimatedEventLocation.coordinates and len(signal.estimatedEventLocation.coordinates) >= 2:
                signal_lat = signal.estimatedEventLocation.coordinates[0]
                signal_lng = signal.estimatedEventLocation.coordinates[1]
            if signal.estimatedEventLocation.probabilityRadius is not None:
                probability_radius_km = signal.estimatedEventLocation.probabilityRadius

        # Use location resolved by Claude/API during signal creation
        origin_loc = created.get("originLocation")
        general_loc = created.get("generalLocation")
        dest_loc = created.get("destinationLocation")
        # For event grouping, prefer the most specific resolved location name
        resolved_loc = origin_loc or general_loc or dest_loc
        if resolved_loc:
            location_name = resolved_loc.get("name") or location_name
        origin_id = origin_loc["id"] if origin_loc else (general_loc["id"] if general_loc else None)

        event = dispatch_group_signal(
            signal_id=signal_id,
            signal_title=signal.headline,
            signal_description=created.get("title"),
            signal_location_name=location_name,
            signal_origin_id=origin_id,
            signal_timestamp=signal.alertTimestamp,
            classification=classification,
            signal_lat=signal_lat,
            signal_lng=signal_lng,
            probability_radius_km=probability_radius_km,
            created_signal=created,
        )

        # ─── Stage 4: Alert escalation (if high severity) ───────────────────
        alert = None
        if event and classification.severity >= 4:
            alert = assess_and_escalate(
                event=event,
                signal_summaries=[classification.summary],
                max_severity=classification.severity,
            )

        return {
            "signal_id": signal_id,
            "classification": classification.model_dump(),
            "event_id": event["id"] if event else None,
            "alert_id": alert["id"] if alert else None,
        }

    except ClaudeRateLimited as exc:
        logger.warning(
            "[CLAUDE RATE-LIMIT] process_signal backing off %.0fs",
            exc.retry_after,
        )
        raise self.retry(exc=exc, countdown=int(exc.retry_after))
    except Exception as exc:
        logger.error("process_signal failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=10)


# Trusted source names that get auto-escalated to alerts
TRUSTED_SOURCE_NAMES = {"field_officer", "partner", "government"}


@app.task(
    name="src.tasks.process.process_manual_signal",
    bind=True,
    max_retries=2,
    acks_late=True,
)
def process_manual_signal(
    self,
    signal_id: str,
    source_type: str,
    title: str,
    description: str,
    severity: int | None = None,
    user_id: str = "",
):
    """
    Process a manually created signal from a trusted source:
    1. Classify via Claude (disaster type, severity)
    2. Group into event (new or existing)
    3. If source is trusted (field_officer/partner/government): auto-escalate the event to alert
       and record the user escalation in eventEscaladedByUsers
    """
    try:
        # ─── Stage 1: Classify via Claude ─────────────────────────────────────
        disaster_types = _get_disaster_types()

        prompt = build_classify_prompt(
            title=title,
            description=description,
            location_name=None,
            url=None,
            timestamp=None,
            raw_context=f"Manual signal from {source_type} source. Description: {description}",
            disaster_types=disaster_types,
        )

        result_data = call_claude(
            CLASSIFY_SYSTEM,
            prompt,
            stage="classify",
            prompt_version=CLASSIFY_PROMPT_VERSION,
            signal_id=signal_id,
        )
        classification = SignalClassification.model_validate(result_data)

        logger.info(
            "Manual signal %s classified: types=%s severity=%d",
            signal_id,
            classification.disaster_types,
            classification.severity,
        )

        # Update severity. v1: Claude's classifier value is a valid fallback.
        # v2: only write if the caller provided a source severity — otherwise
        # the signal stays null and the event-level calculator handles it.
        if severity is not None:
            update_signal_severity(signal_id, severity)
        elif settings.grouping_algo == "v1":
            update_signal_severity(signal_id, classification.severity)

        # ─── Stage 2: Event grouping ──────────────────────────────────────────
        # Manual signals don't carry a created_signal record with resolved
        # locations yet (the manual-signal mutation handles creation API-side).
        # v2 grouping will still work — admin-2 resolution returns None and
        # the signal becomes its own event, which matches the pre-existing
        # behaviour for manual entries.
        event = dispatch_group_signal(
            signal_id=signal_id,
            signal_title=title,
            signal_description=description,
            signal_location_name=None,
            signal_origin_id=None,
            signal_timestamp=None,
            classification=classification,
            created_signal={},
        )

        if not event:
            logger.warning("Manual signal %s: event grouping failed", signal_id)
            return {
                "signal_id": signal_id,
                "classification": classification.model_dump(),
                "event_id": None,
                "alert_id": None,
                "escalated": False,
            }

        # ─── Stage 3: Auto-escalate for trusted sources ──────────────────────
        escalated = False
        if source_type in TRUSTED_SOURCE_NAMES:
            logger.info(
                "Trusted source (%s) — auto-escalating event %s to alert",
                source_type,
                event["id"],
            )
            try:
                escalation = escalate_event(event["id"], user_id)
                escalated = True
                logger.info(
                    "Event %s escalated: escalation_id=%s",
                    event["id"],
                    escalation["id"],
                )
            except Exception as e:
                logger.error("Failed to escalate event %s: %s", event["id"], e)

        return {
            "signal_id": signal_id,
            "classification": classification.model_dump(),
            "event_id": event["id"],
            "escalated": escalated,
        }

    except ClaudeRateLimited as exc:
        logger.warning(
            "[CLAUDE RATE-LIMIT] process_manual_signal backing off %.0fs",
            exc.retry_after,
        )
        raise self.retry(exc=exc, countdown=int(exc.retry_after))
    except Exception as exc:
        logger.error("process_manual_signal failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=10)


@app.task(
    name="src.tasks.process.process_gdacs_signal",
    bind=True,
    max_retries=2,
    acks_late=True,
)
def process_gdacs_signal(
    self,
    signal_id: str,
    gdacs_event: dict,
    created_signal: dict | None = None,
):
    """
    Process a GDACS-sourced signal.

    GDACS events are already structured disaster data, so we:
    1. Build classification directly from GDACS metadata (skip Claude classification)
    2. Group into event (new or existing) via Claude
    3. Assess for alert escalation if high severity
    """
    try:
        glide_type = gdacs_event.get("glide_type", "ot")
        severity = gdacs_event.get("severity", 3)
        alert_level = gdacs_event.get("alert_level", "Green")
        title = gdacs_event.get("title", "GDACS event")
        description = gdacs_event.get("description") or ""
        location_name = gdacs_event.get("country")
        population_affected = gdacs_event.get("population_affected")

        # Enrich description with population data if available
        if population_affected:
            description = f"{description} Approximately {population_affected:,} people affected."

        # Build classification from GDACS metadata (no Claude needed)
        classification = SignalClassification(
            disaster_types=[glide_type],
            relevance=1.0 if alert_level in ("Red", "Orange") else 0.7,
            severity=severity,
            summary=f"GDACS {alert_level} alert: {title}" + (f" ({population_affected:,} affected)" if population_affected else ""),
        )

        logger.info(
            "GDACS signal %s: type=%s severity=%d alert=%s",
            signal_id, glide_type, severity, alert_level,
        )

        # Update signal severity
        update_signal_severity(signal_id, severity)

        # Skip low-relevance events
        if classification.relevance < settings.relevance_threshold:
            logger.info("GDACS signal %s below relevance threshold, skipping", signal_id)
            return {"signal_id": signal_id, "event_id": None, "alert_id": None}

        # Group into event (no probabilityRadius for GDACS — uses default 1km)
        event = dispatch_group_signal(
            signal_id=signal_id,
            signal_title=title,
            signal_description=description,
            signal_location_name=location_name,
            signal_origin_id=None,
            signal_timestamp=gdacs_event.get("from_date"),
            classification=classification,
            signal_lat=gdacs_event.get("lat"),
            signal_lng=gdacs_event.get("lng"),
            created_signal=created_signal or {},
        )

        # Assess for alert if high severity (Red/Orange)
        alert = None
        if event and severity >= 4:
            alert = assess_and_escalate(
                event=event,
                signal_summaries=[classification.summary],
                max_severity=severity,
            )

        return {
            "signal_id": signal_id,
            "event_id": event["id"] if event else None,
            "alert_id": alert["id"] if alert else None,
        }

    except ClaudeRateLimited as exc:
        logger.warning(
            "[CLAUDE RATE-LIMIT] process_gdacs_signal backing off %.0fs",
            exc.retry_after,
        )
        raise self.retry(exc=exc, countdown=int(exc.retry_after))
    except Exception as exc:
        logger.error("process_gdacs_signal failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=10)


@app.task(
    name="src.tasks.process.process_acled_signal",
    bind=True,
    max_retries=2,
    acks_late=True,
)
def process_acled_signal(
    self,
    signal_id: str,
    acled_event: dict,
    created_signal: dict | None = None,
):
    """
    Process an ACLED-sourced signal.

    ACLED events are structured conflict data, so we:
    1. Build classification directly from ACLED metadata (skip Claude classification)
    2. Group into event (new or existing) via Claude
    3. Assess for alert escalation if high severity (fatalities / event type)
    """
    try:
        glide_type = acled_event.get("glide_type", "ot")
        severity = acled_event.get("severity", 2)
        fatalities = acled_event.get("fatalities", 0)
        title = acled_event.get("title", "ACLED event")
        description = acled_event.get("description") or ""
        location_name = (
            acled_event.get("location")
            or acled_event.get("admin2")
            or acled_event.get("admin1")
            or acled_event.get("country")
        )

        # Build classification from ACLED metadata
        summary = f"ACLED {acled_event.get('event_type', 'conflict')} event"
        if fatalities:
            summary += f" ({fatalities} fatalities)"

        classification = SignalClassification(
            disaster_types=[glide_type],
            relevance=1.0 if fatalities > 0 or severity >= 4 else 0.8,
            severity=severity,
            summary=summary,
        )

        logger.info(
            "ACLED signal %s: type=%s severity=%d fatalities=%d",
            signal_id, glide_type, severity, fatalities,
        )

        # Update signal severity
        update_signal_severity(signal_id, severity)

        # Skip low-relevance events
        if classification.relevance < settings.relevance_threshold:
            logger.info("ACLED signal %s below relevance threshold, skipping", signal_id)
            return {"signal_id": signal_id, "event_id": None, "alert_id": None}

        # Group into event
        event = dispatch_group_signal(
            signal_id=signal_id,
            signal_title=title,
            signal_description=description,
            signal_location_name=location_name,
            signal_origin_id=None,
            signal_timestamp=acled_event.get("event_date"),
            classification=classification,
            signal_lat=acled_event.get("lat"),
            signal_lng=acled_event.get("lng"),
            created_signal=created_signal or {},
        )

        # Assess for alert if high severity
        alert = None
        if event and severity >= 4:
            alert = assess_and_escalate(
                event=event,
                signal_summaries=[classification.summary],
                max_severity=severity,
            )

        return {
            "signal_id": signal_id,
            "event_id": event["id"] if event else None,
            "alert_id": alert["id"] if alert else None,
        }

    except ClaudeRateLimited as exc:
        logger.warning(
            "[CLAUDE RATE-LIMIT] process_acled_signal backing off %.0fs",
            exc.retry_after,
        )
        raise self.retry(exc=exc, countdown=int(exc.retry_after))
    except Exception as exc:
        logger.error("process_acled_signal failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=10)
