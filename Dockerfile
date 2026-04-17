FROM python:3.12-slim

WORKDIR /app

# Install system libraries required by rasterio/GDAL for population estimation
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgdal-dev \
    libexpat1 \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

COPY pyproject.toml .
RUN uv pip install --system --no-cache .

COPY . .

# Default: run Celery worker + beat
CMD ["celery", "-A", "src.celery_app", "worker", "--beat", "--loglevel=info", "--concurrency=4"]
