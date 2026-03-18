from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # Dataminr
    dataminr_api_user_id: str
    dataminr_api_password: str
    dataminr_auth_url: str = "https://firstalert-api.dataminr.com/auth/1/userAuthorization"
    dataminr_alerts_url: str = "https://firstalert-api.dataminr.com/alerts/1/alerts"

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

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}


settings = Settings()
