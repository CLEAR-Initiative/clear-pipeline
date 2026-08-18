from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Dataminr — optional so the Dagster code location loads in any environment
    # (e.g. CI, or before cutover). Presence is enforced at Dataminr poll time,
    # not at import, so a missing cred never breaks loading the whole instance.
    dataminr_client_id: str = ""
    dataminr_client_secret: str = ""
    dataminr_auth_url: str = "https://api.dataminr.com/auth/v1/token"
    dataminr_alerts_url: str = "https://api.dataminr.com/firstalert/v1/alerts"
    dataminr_token_ttl: int = 12600  # 3.5 hours in seconds
    # Default Dagster poll cadence in minutes (prod = 60). DATAMINR_POLL_INTERVAL_MINUTES
    # sets the default; the live value is editable from the Dagster UI (poll sensor →
    # Edit cursor → {"interval_minutes": N}) without a redeploy. NOTE: the legacy
    # POLL_INTERVAL_SECONDS / *_POLL_INTERVAL_MINUTES fields are inert Celery-beat relics.
    dataminr_poll_interval_minutes: int = 60

    # Legacy API fallback (firstalert-api.dataminr.com)
    dataminr_use_legacy: bool = False  # Set to true to force legacy API
    dataminr_legacy_base_url: str = "https://firstalert-api.dataminr.com"
    dataminr_legacy_user_id: str = ""
    dataminr_legacy_password: str = ""
    dataminr_alert_version: int = 19  # Legacy API alert version param

    # GDACS (public API — no auth required)
    gdacs_base_url: str = "https://www.gdacs.org/gdacsapi"
    gdacs_countries: str = "Sudan,Afghanistan,Venezuela"  # Comma-separated; GDACS spelling
    gdacs_poll_interval_minutes: int = 30
    gdacs_source_name: str = "gdacs"

    # ACLED (Armed Conflict Location & Event Data Project)
    acled_base_url: str = "https://acleddata.com"
    acled_username: str = ""
    acled_password: str = ""  # API key
    acled_countries: str = "Sudan,Afghanistan,Venezuela"  # Comma-separated; ACLED spelling
    acled_poll_interval_minutes: int = 60
    # ACLED publishes in weekly batches where event_date lags publication. The poll
    # overscans its (event_date-filtered) query window by this many days so
    # late-published-but-earlier-dated events aren't filtered out forever; the seen
    # set + clear-api idempotency dedup the overlap.
    acled_publication_lag_days: int = 14
    acled_source_name: str = "acled"
    acled_token_ttl: int = 23 * 3600  # 23 hours (valid 24h)

    # darfur24.com (Sudanese news outlet — public WordPress RSS, no auth).
    # Comma-separated feed URLs. English edition by default; the Arabic
    # primary edition lives at https://darfur24.com/feed/ — see
    # src/clients/darfur24.py for the duplicate-story caveat before adding it.
    darfur24_feed_urls: str = "https://darfur24.com/en/feed/"
    darfur24_poll_interval_minutes: int = 30
    darfur24_source_name: str = "darfur24"
    # Country whose L0 location every darfur24 signal is attached to.
    # News articles carry no structured coordinates, but a signal without a
    # location is invisible in every country-scoped UI view (signalsPage
    # filters by location descendants) — so we pin signals to the outlet's
    # deployment country and leave finer-grained resolution to the
    # classification follow-up. Must match a level-0 location `name` in the
    # CLEAR API (expo-385).
    darfur24_default_country: str = "Sudan"

    # Manual — analyst-created signals (no poll, no lake blob). The drain reads
    # NEW signals with this source name straight from clear-api. No ingest asset;
    # a drain sensor checks for pending manual signals every N minutes (low so
    # an analyst's signal is classified/grouped promptly). MANUAL_SOURCE_NAME must
    # match a data_sources row and the `source` clear-api tags on those signals.
    manual_source_name: str = "manual"
    manual_poll_interval_minutes: int = 1

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # CLEAR API
    clear_api_url: str = "http://localhost:4000/graphql"
    clear_api_key: str = ""

    # LogIE roads & bridges (ArcGIS FeatureServers → locationMetadata).
    # Monthly sync; iso3 list is comma-separated. The roads view is multi-
    # country (covers SDN/AFG/VEN/SSD); bridges is world-wide LogIE.
    logie_roads_url: str = "https://services3.arcgis.com/t6lYS2Pmd8iVx1fy/arcgis/rest/services/Situational_Roads_view/FeatureServer/0"
    logie_bridges_url: str = "https://gis.logcluster.org/server/rest/services/LogIE/wld_trs_bridges_b_w_viewer/FeatureServer/0"
    logistics_iso3: str = "SDN,AFG,VEN"

    # Anthropic
    anthropic_api_key: str = ""
    # Default model — used unless a per-stage override below is set.
    claude_model: str = "claude-sonnet-4-6"

    # Per-stage model overrides. Lighter stages (boolean / NER / pattern
    # matching) default to Haiku; user-facing narrative stages stay on the
    # default model. Each can be flipped via env without code changes.
    #
    #   classify   — v1 signal classification (taxonomy lookup + severity)
    #   group      — v1 add-vs-create event clustering decision
    #   assess     — v1 alert-worthiness boolean
    #   rewrite    — v2 event title/description (USER-FACING)
    #   crisis     — crisis narrative (USER-FACING, less frequent)
    #   location   — text → location-name extraction (NER)
    claude_model_classify: str = "claude-haiku-4-5-20251001"
    claude_model_group: str = ""  # "" → falls back to claude_model
    claude_model_assess: str = "claude-haiku-4-5-20251001"
    claude_model_rewrite: str = ""  # falls back to claude_model
    claude_model_crisis: str = ""  # falls back to claude_model
    claude_model_location: str = "claude-haiku-4-5-20251001"
    # Translation is mostly mechanical (string-to-string with structure
    # preservation) — Haiku handles it well at ~10x the price advantage.
    claude_model_translate: str = "claude-haiku-4-5-20251001"
    # Ground-intel (WhatsApp signal pipeline). Message triage is a batched
    # 4-way label task — Haiku territory, like classify/assess. Threading is
    # a cross-message clustering judgement — stays on the default model.
    claude_model_ground_classify: str = "claude-haiku-4-5-20251001"
    claude_model_ground_thread: str = ""  # "" → falls back to claude_model

    # Translation — comma-separated BCP-47 codes. 'en' is the canonical
    # source and is never a target. Empty string disables translation
    # entirely, which is the safe default until management says go.
    target_locales: str = "ar,fr"

    # Celery
    celery_broker_url: str = "redis://localhost:6379/0"

    # Pipeline
    poll_interval_seconds: int = 15
    initial_lookback_days: int = 7
    relevance_threshold: float = 0.5
    dedup_ttl_hours: int = 48
    dataminr_source_name: str = "dataminr"
    max_pages_per_poll: int = 50  # Safety cap on pagination

    # S3 storage (for population GeoTIFF and other assets)
    s3_endpoint: str = ""
    s3_bucket: str = ""
    s3_region: str = "auto"
    s3_access_key_id: str = ""
    s3_secret_access_key: str = ""

    # API server
    api_port: int = 8000
    api_shared_secret: str = ""  # Shared secret for clear-api → pipeline calls

    # Observability
    logtail_source_token: str = ""
    sentry_dsn: str = ""
    sentry_env: str = "development"
    log_level: str = "INFO"

    # NOTE: the clear-pipeline `insights` per-Claude-call telemetry is intentionally
    # NOT wired in the Dagster port — LLM calls go through providers/llm.py, which
    # has no insights hook. Call-level telemetry is dropped for the signal pipeline;
    # a per-run guardrail (below) bounds spend instead.

    # Cost guardrail: a single classify_group drain run processes at most this many
    # signals (each grouped signal makes one rewrite LLM call). Prevents a runaway
    # run — up to _MAX_BATCHES * _BATCH_SIZE = 10k signals — from an unbounded spend.
    signal_max_signals_per_run: int = 2000

    # Suppress alert escalation (and the email fan-out it triggers) when the
    # signal's publishedAt is older than this many hours. Backdated Dataminr
    # alerts and replayed signals were firing immediate emails for week-old
    # incidents, which the analyst team experiences as "wrong alert" — the
    # email is technically correct but stale. Set to 0 to disable the gate.
    alert_max_signal_age_hours: int = 48

    # Last-resort default for `events.population_displaced` when neither
    # the signal text nor the admin-2 DTM row provides a value.
    default_population_displaced: int = 1670

    # Last-resort default for `events.population_affected` when neither the
    # raw signal extraction (ACLED has none, GDACS exposure data,
    # Dataminr/manual regex) nor the per-event-type lookup (median pop_1km
    # via acled_event_type_stats.json) produces a value.
    default_population_affected: int = 33_000

    # IOM DTM API — displaced-person data per admin level
    iom_dtm_base_url: str = "https://dtmapi.iom.int/v3"
    iom_dtm_subscription_key: str = ""  # empty disables DTM backfill
    iom_dtm_country_name: str = "Sudan"
    iom_dtm_admin0_pcode: str = "SDN"
    # IOM DTM "Operation" (data-gathering project). The default points at the
    # currently-active Sudan project; older operations exist on the API but
    # are out-of-date and dominate when no filter is applied.
    iom_dtm_operation: str = "Armed Clashes in Sudan (Overview)"
    # Optional lower bound on round number. Leave unset (0/None) to fetch all
    # rounds and let the aggregator pick the newest per pcode — that gives
    # us full backtrack history. Set to a specific round to constrain.
    iom_dtm_from_round: int = 0
    # DTM returns one row per (destination × origin × reason × assessmentType).
    # Comma-separated priority list: BA (Baseline Assessment = current IDP
    # stock) fills each pcode first; FM (Flow Monitoring = transit) fills the
    # remaining gaps. This avoids the BA+FM double-count while picking up
    # districts that only have FM data (Khartoum, At Tina, etc.). Each upserted
    # row records which assessmentType produced it. Set to empty string to
    # disable filtering and pool all types — only safe when the data has no
    # BA/FM overlap.
    iom_dtm_assessment_type: str = "BA,FM"

    # ─── Nominatim geocoder (currently LocationIQ as the backend) ────────────
    # The Nominatim-compatible geocoder client uses these. We talk to
    # LocationIQ's free tier (5,000 req/day, 2 req/sec burst), but the code
    # is named for the protocol so we can switch to MapTiler / self-hosted
    # Nominatim / similar by changing only these two settings.
    locationiq_api_key: str = ""  # empty disables the geocoder entirely
    locationiq_base_url: str = "https://us1.locationiq.com/v1"
    # ISO-3166-1 alpha-2 codes the geocoder may resolve into — the
    # pipeline's supported countries. Biases the geocode query
    # (`countrycodes=`) and filters out any candidate resolving outside the
    # set. Comma-separated. Default covers all three POC countries; a
    # cross-country mis-resolution is still caught downstream by clear-api's
    # same-A2 check against the signal's source coordinates.
    geoparser_country_codes: str = "sd,ve,af"
    # Hybrid geo-resolver: try the offline GeoNames gazetteer in clear-api
    # (`resolveGazetteerLocation`) before LocationIQ. Transliteration-tolerant
    # and quota-free; LocationIQ then only handles the landmarks/POIs the
    # gazetteer lacks. Kill-switch to fall back to LocationIQ-only.
    #
    # Ships DARK (False): the `resolveGazetteerLocation` field must be deployed
    # in clear-api first — until then an unknown GraphQL field is an *error*,
    # not a graceful miss, so every signal would log an error before falling
    # back. Flip to True only after clear-api's gazetteer resolver is live.
    geoparser_use_gazetteer: bool = False
    # Minimum pg_trgm similarity (0–1) for a fuzzy gazetteer match. Lower =
    # more recall but more false matches (caught downstream by the same-A2
    # check against source coords). 1.0 would accept only exact hits.
    geoparser_gazetteer_min_similarity: float = 0.45

    # User-Agent string sent on every geocoder request. Required by both
    # OSMF Nominatim policy and LocationIQ TOS. Identifies the application
    # so the provider can reach us if our traffic looks problematic.
    geocoder_user_agent: str = (
        "clear-pipeline/1.0 (https://clearinitiative.io; ops@clearinitiative.io)"
    )

    # Cache TTLs (seconds) per response status. Successful geocodes are
    # cached aggressively (~6 months) since place names rarely change.
    # Empty results are cached for ~7 days — they might exist later.
    # Bumped down from 30 days after we discovered a burst of poisoned
    # `no_result` writes during a bad-config window (May 25 – June 9, 2026)
    # blocked ~2 weeks of Sudanese geocoding lookups. 7 days keeps the cache
    # useful for repeat-question workloads (same signal-week still hits the
    # cache) while ensuring transient upstream misbehaviour heals within a
    # week instead of a month.
    # Errors are cached briefly so we don't hammer the geocoder while it's
    # degraded but also recover quickly when it comes back.
    geocoder_cache_ttl_ok_seconds: int = 6 * 30 * 24 * 60 * 60  # ~180 days
    geocoder_cache_ttl_no_result_seconds: int = 7 * 24 * 60 * 60  # 7 days
    geocoder_cache_ttl_error_seconds: int = 60 * 60  # 1 hour

    # Rate-limit floor (seconds between calls). LocationIQ free tier allows
    # 2 req/sec burst; we play it safe at 1 req/sec sustained so multiple
    # Celery workers can share the budget without coordination.
    geocoder_min_interval_seconds: float = 1.0

    # Circuit breaker: trip after this many consecutive failures, stay open
    # for this many seconds. While open, the client returns None instead of
    # calling the geocoder — callers fall back to coord-based resolution.
    geocoder_circuit_failure_threshold: int = 3
    geocoder_circuit_open_seconds: int = 5 * 60  # 5 minutes

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        # Tolerate unknown keys in .env so a stray/legacy var doesn't crash boot.
        # The pipeline still warns via missing-field errors for required vars.
        "extra": "ignore",
    }


settings = Settings()
