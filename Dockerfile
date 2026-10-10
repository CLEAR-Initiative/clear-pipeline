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

# Layer-cache trick, part 1: install the LOCKED DEPENDENCIES before src/ is
# copied. pyproject + uv.lock change rarely; src/ changes on most merges.
# `uv sync --no-install-project` resolves straight from uv.lock (so the
# [tool.uv.sources] torch → pytorch-cpu routing and the pins are honoured,
# which a plain `uv export` + `pip install -r` would lose) and installs into
# the image's system interpreter via UV_PROJECT_ENVIRONMENT. The result is a
# layer that only rebuilds when the lock changes — ~1.3 min of torch/dagster/
# great-expectations installs that used to re-run on every src-only build.
COPY pyproject.toml uv.lock README.md ./
RUN UV_PROJECT_ENVIRONMENT=/usr/local uv sync --frozen --no-dev --no-install-project --no-cache

# Part 2: copy the source layout hatchling needs BEFORE the project itself is
# installed. The build backend has `packages = ["src/clear_pipeline"]` in
# pyproject.toml, so `src/` must exist on disk when the wheel is built —
# otherwise hatchling silently produces a wheel containing pyproject metadata
# only, the package is missing at runtime, and Dagster's gRPC server fails
# with `ModuleNotFoundError: clear_pipeline`. `--no-deps` keeps this step to
# the project wheel alone; everything it needs is already in the layer above.
COPY src ./src
RUN uv pip install --system --no-cache --no-deps .

# Copy the rest of the repo (tests, docs, ancillary configs) after the
# install so a change to those doesn't force a wheel rebuild.
COPY . .
# The dagster CLI expects DAGSTER_HOME to point at a writable dir
# containing dagster.yaml. Terraform mounts /opt/dagster from an
# extra_files-materialised directory on the VM.
ENV DAGSTER_HOME=/opt/dagster

# Bake the instance + workspace config into DAGSTER_HOME so hosts WITHOUT a
# mount (Railway, plain `docker run`) pick them up automatically. The VM/
# compose deployment bind-mounts its own dagster.yaml / workspace.yaml over
# these, so this changes nothing there.
RUN mkdir -p /opt/dagster
COPY deploy/dagster.yaml deploy/workspace.yaml /opt/dagster/

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
