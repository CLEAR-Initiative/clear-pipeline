from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Dataminr
    dataminr_client_id: str
    dataminr_client_secret: str
    dataminr_auth_url: str = "https://api.dataminr.com/auth/v1/token"
    dataminr_alerts_url: str = "https://api.dataminr.com/firstalert/v1/alerts"
    dataminr_token_ttl: int = 12600  # 3.5 hours in seconds

    # Legacy API fallback (firstalert-api.dataminr.com)
    dataminr_use_legacy: bool = False  # Set to true to force legacy API
    dataminr_legacy_base_url: str = "https://firstalert-api.dataminr.com"
    dataminr_legacy_user_id: str = ""
    dataminr_legacy_password: str = ""
    dataminr_alert_version: int = 19  # Legacy API alert version param

    # GDACS (public API — no auth required)
    gdacs_base_url: str = "https://www.gdacs.org/gdacsapi"
    gdacs_countries: str = "Sudan"  # Comma-separated list of countries
    gdacs_poll_interval_minutes: int = 30
    gdacs_source_name: str = "gdacs"

    # ACLED (Armed Conflict Location & Event Data Project)
    acled_base_url: str = "https://acleddata.com"
    acled_username: str = ""
    acled_password: str = ""  # API key
    acled_countries: str = "Sudan"  # Comma-separated list
    acled_poll_interval_minutes: int = 60
    acled_source_name: str = "acled"
    acled_token_ttl: int = 23 * 3600  # 23 hours (valid 24h)

    # Redis
    redis_url: str = "redis://localhost:6379/0"

    # CLEAR API
    clear_api_url: str = "http://localhost:4000/graphql"
    clear_api_key: str = ""

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

    # Insights dashboard (LLM call telemetry — see clear-pipeline-insights repo)
    insights_api_url: str = "https://clear-pipeline-insights.vercel.app"
    insights_ingest_token: str = ""  # empty disables telemetry
    pipeline_env: str = ""  # empty → derived as local-{whoami} at runtime

    # Grouping algorithm selector.
    #   "v1" (default) — legacy semantic grouping: Claude decides add-vs-create.
    #   "v2"           — new district+type grouping (EventClassifier + Claude rewrite only).
    grouping_algo: str = "v1"

    # Last-resort default for `events.population_displaced` when neither
    # the signal text nor the admin-2 DTM row provides a value.
    default_population_displaced: int = 1670

    # Last-resort default for `events.population_affected` when neither the
    # raw signal extraction (ACLED has none, GDACS exposure data,
    # Dataminr/manual regex) nor the per-event-type lookup (median pop_1km
    # via acled_event_type_stats.json) produces a value.
    default_population_affected: int = 1715

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
    # rounds and let `latest_round_per_pcode` pick the newest per pcode — that
    # gives us full backtrack history. Set to a specific round to constrain.
    iom_dtm_from_round: int = 0

    model_config = {
        "env_file": ".env",
        "env_file_encoding": "utf-8",
        # Tolerate unknown keys in .env so a stray/legacy var doesn't crash boot.
        # The pipeline still warns via missing-field errors for required vars.
        "extra": "ignore",
    }


settings = Settings()
