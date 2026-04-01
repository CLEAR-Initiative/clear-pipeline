from celery import Celery
from celery.schedules import crontab, timedelta

from src.config import settings

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
}

app.conf.include = [
    "src.tasks.poll",
    "src.tasks.poll_gdacs",
    "src.tasks.process",
    "src.tasks.notify",
]
