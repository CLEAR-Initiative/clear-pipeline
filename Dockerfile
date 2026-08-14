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

# Copy the metadata + source layout hatchling needs BEFORE `uv pip install`
# runs. The build backend has `packages = ["src/clear_context_pipeline"]`
# in pyproject.toml, so `src/` must exist on disk when the wheel is built
# — otherwise hatchling silently produces a wheel containing pyproject
# metadata only, the package is missing at runtime, and Dagster's
# gRPC server fails with `ModuleNotFoundError: clear_context_pipeline`.
#
# Layer-cache trick: pyproject + uv.lock + src/ change at different
# cadences. Splitting these COPYs means a source-only change only
# invalidates the last `uv pip install` — not the earlier apt layer.
COPY pyproject.toml uv.lock README.md ./
COPY src ./src
RUN uv pip install --system --no-cache .

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
