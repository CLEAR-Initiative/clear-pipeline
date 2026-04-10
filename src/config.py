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
    claude_model: str = "claude-sonnet-4-6"

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

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
