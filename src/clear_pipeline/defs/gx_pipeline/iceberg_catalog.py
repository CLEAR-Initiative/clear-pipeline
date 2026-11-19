"""Shared PyIceberg SQL catalog for the gx pipeline's Iceberg tables: one
catalog, one warehouse. Gold tables (`iceberg_signals.py`,
`iceberg_events.py`) live in the `gold` namespace."""

import os

from pyiceberg.catalog import Catalog, load_catalog

from clear_pipeline.signals.config import settings

NAMESPACE = "gold"

_CHECKSUM_ENV = ("AWS_REQUEST_CHECKSUM_CALCULATION", "AWS_RESPONSE_CHECKSUM_VALIDATION")

# Seconds. Without them pyarrow's AWS SDK gives up when a request sends
# nothing for 3 s (curlCode 28), which failed sudan_witness_gx on dev twice
# while it read table metadata (tickets #1305, #1376).
S3_CONNECT_TIMEOUT_S = 10
S3_REQUEST_TIMEOUT_S = 30


def _require_checksums_when_required() -> None:
    """Default the AWS SDK checksum mode to WHEN_REQUIRED; refuse any other value.

    PyIceberg writes through pyarrow's AWS C++ SDK, which providers/s3.py's boto3
    config doesn't reach. Any other mode streams every UploadPart as aws-chunked,
    which the S3-compatible backend rejects (501). The SDK reads these per client.
    """
    for name in _CHECKSUM_ENV:
        value = os.environ.setdefault(name, "WHEN_REQUIRED")
        if value.upper() != "WHEN_REQUIRED":
            raise RuntimeError(f"{name}={value!r}: the S3 backend needs WHEN_REQUIRED")


def catalog() -> Catalog:
    warehouse = settings.iceberg_warehouse or f"s3://{settings.s3_bucket}/gold-iceberg"
    properties: dict[str, str] = {
        "type": "sql",
        "uri": settings.iceberg_catalog_uri,
        "warehouse": warehouse,
    }
    if warehouse.startswith("s3://"):
        _require_checksums_when_required()
        properties |= {
            "s3.endpoint": settings.s3_endpoint,
            "s3.region": settings.s3_region,
            "s3.access-key-id": settings.s3_access_key_id,
            "s3.secret-access-key": settings.s3_secret_access_key,
            # This pipeline always talks to a custom S3-compatible endpoint
            # (providers/s3.py), never bare AWS S3 — path-style addressing
            # is what those backends (e.g. MinIO) expect.
            "s3.path-style-access": "true",
            "s3.connect-timeout": str(S3_CONNECT_TIMEOUT_S),
            "s3.request-timeout": str(S3_REQUEST_TIMEOUT_S),
        }
    return load_catalog("gx_pipeline", **properties)


def ensure_namespace(cat: Catalog) -> None:
    if NAMESPACE not in [ns[0] for ns in cat.list_namespaces()]:
        cat.create_namespace(NAMESPACE)
