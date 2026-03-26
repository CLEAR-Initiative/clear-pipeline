"""Celery task: process a single signal through classification → grouping → escalation."""

import json
import logging

import redis

from src.celery_app import app
from src.clients.claude import call_claude
from src.clients.graphql import get_dataminr_source_id, get_disaster_types
from src.config import settings
from src.models.clear import SignalClassification
from src.models.dataminr import DataminrSignal
from src.prompts.classify import SYSTEM_PROMPT as CLASSIFY_SYSTEM, build_classify_prompt
from src.services.alert import assess_and_escalate
from src.services.event import group_signal
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

            result_data = call_claude(CLASSIFY_SYSTEM, prompt)
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
        if signal.estimatedEventLocation:
            location_name = signal.estimatedEventLocation.name
            if signal.estimatedEventLocation.coordinates and len(signal.estimatedEventLocation.coordinates) >= 2:
                signal_lat = signal.estimatedEventLocation.coordinates[0]
                signal_lng = signal.estimatedEventLocation.coordinates[1]

        # Use location resolved by Claude/API during signal creation
        origin_loc = created.get("originLocation")
        general_loc = created.get("generalLocation")
        dest_loc = created.get("destinationLocation")
        # For event grouping, prefer the most specific resolved location name
        resolved_loc = origin_loc or general_loc or dest_loc
        if resolved_loc:
            location_name = resolved_loc.get("name") or location_name
        origin_id = origin_loc["id"] if origin_loc else (general_loc["id"] if general_loc else None)

        event = group_signal(
            signal_id=signal_id,
            signal_title=signal.headline,
            signal_description=created.get("title"),
            signal_location_name=location_name,
            signal_origin_id=origin_id,
            signal_timestamp=signal.alertTimestamp,
            classification=classification,
            signal_lat=signal_lat,
            signal_lng=signal_lng,
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

    except Exception as exc:
        logger.error("process_signal failed: %s", exc, exc_info=True)
        raise self.retry(exc=exc, countdown=10)
