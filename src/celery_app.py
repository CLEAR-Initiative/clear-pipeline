from celery import Celery
from celery.schedules import timedelta

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
}

app.conf.include = [
    "src.tasks.poll",
    "src.tasks.process",
]
