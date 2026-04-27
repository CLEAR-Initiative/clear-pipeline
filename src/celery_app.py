from celery import Celery
from celery.schedules import crontab, timedelta
from celery.signals import (
    after_setup_logger,
    after_setup_task_logger,
    worker_process_init,
)

from src.config import settings


@worker_process_init.connect
def _init_worker(**kwargs):
    """Initialise logging + Sentry in each forked worker process."""
    from src.logging_setup import setup_logging
    setup_logging()


# Celery overrides handlers after worker boot. These signals run AFTER
# Celery's setup, giving us a chance to (re)attach our stdout + Logtail
# handlers so task logger.info() calls from child workers are visible.
@after_setup_logger.connect
def _setup_root_logger(logger, **kwargs):
    from src.logging_setup import attach_handlers_to
    attach_handlers_to(logger)


@after_setup_task_logger.connect
def _setup_task_logger(logger, **kwargs):
    from src.logging_setup import attach_handlers_to
    attach_handlers_to(logger)


app = Celery("clear_pipeline", broker=settings.celery_broker_url)

app.conf.update(
    result_backend=settings.celery_broker_url,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],
    timezone="UTC",
    enable_utc=True,
    task_track_started=True,
    task_acks_late=True,
    worker_prefetch_multiplier=1,
)

app.conf.beat_schedule = {
    "poll-dataminr": {
        "task": "src.tasks.poll.poll_dataminr",
        "schedule": timedelta(seconds=settings.poll_interval_seconds),
    },
    "poll-gdacs": {
        "task": "src.tasks.poll_gdacs.poll_gdacs",
        "schedule": timedelta(minutes=settings.gdacs_poll_interval_minutes),
    },
    "poll-acled": {
        "task": "src.tasks.poll_acled.poll_acled",
        "schedule": timedelta(minutes=settings.acled_poll_interval_minutes),
    },
    # Daily digest — every day at 07:00 UTC
    "daily-alert-digest": {
        "task": "src.tasks.notify.send_daily_digest",
        "schedule": crontab(hour=7, minute=0),
    },
    # Weekly digest — every Monday at 07:00 UTC
    "weekly-alert-digest": {
        "task": "src.tasks.notify.send_weekly_digest",
        "schedule": crontab(hour=7, minute=0, day_of_week=1),
    },
    # Monthly digest — 1st of each month at 07:00 UTC
    "monthly-alert-digest": {
        "task": "src.tasks.notify.send_monthly_digest",
        "schedule": crontab(hour=7, minute=0, day_of_month=1),
    },
    # Daily archival — 03:00 UTC, archive alerts whose event last saw
    # a signal more than 14 days ago.
    "archive-stale-alerts": {
        "task": "src.tasks.archive.archive_stale_alerts",
        "schedule": crontab(hour=3, minute=0),
        "kwargs": {"older_than_days": 14},
    },
    # Weekly IOM DTM backfill — Mondays at 02:00 UTC. Refreshes
    # locationMetadata(type="iom_dtm_displacement") per admin-2.
    "backfill-dtm-displacement": {
        "task": "src.tasks.dtm.backfill_dtm_displacement",
        "schedule": crontab(hour=2, minute=0, day_of_week=1),
    },
}

app.conf.include = [
    "src.tasks.poll",
    "src.tasks.poll_gdacs",
    "src.tasks.poll_acled",
    "src.tasks.process",
    "src.tasks.notify",
    "src.tasks.population",
    "src.tasks.geometries",
    "src.tasks.crisis",
    "src.tasks.archive",
    "src.tasks.dtm",
]
