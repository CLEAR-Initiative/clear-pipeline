FROM python:3.12-slim

WORKDIR /app

# System deps:
#   - build-essential: uv sync compiles a few pure-Python packages with C
#     extensions (voyageai, pdfplumber transitive deps).
#   - libmagic1 + poppler-utils: used by pdfplumber / unstructured-family
#     libs to read PDF metadata + fall back to alternative extractors.
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libmagic1 \
    poppler-utils \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir uv

# Install deps first, code second, so incremental code changes hit a warm
# uv cache instead of reinstalling every dependency.
COPY pyproject.toml uv.lock ./
RUN uv pip install --system --no-cache .

COPY . .
# The dagster CLI expects DAGSTER_HOME to point at a writable dir
# containing dagster.yaml. Terraform mounts /opt/dagster from an
# extra_files-materialised directory on the VM.
ENV DAGSTER_HOME=/opt/dagster

# No ENTRYPOINT — compose supplies the full command per service. If we
# left `ENTRYPOINT ["bash", "-lc"]` here (as an earlier version did),
# compose's tokenised `command:` would collapse into positional params
# and only the first token would execute, so `dagster api grpc ...`
# became just `dagster` → usage banner → restart loop.
#
# The default CMD is only a fallback if someone runs the image with
# `docker run` and no override — showing the CLI banner is a fine
# hint that this image is meant to be invoked with a subcommand.
CMD ["dagster", "--help"]
