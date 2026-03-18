FROM python:3.12-slim

WORKDIR /app

RUN pip install --no-cache-dir uv

COPY pyproject.toml .
RUN uv pip install --system --no-cache .

COPY . .

# Default: run Celery worker + beat
CMD ["celery", "-A", "src.celery_app", "worker", "--beat", "--loglevel=info", "--concurrency=4"]
