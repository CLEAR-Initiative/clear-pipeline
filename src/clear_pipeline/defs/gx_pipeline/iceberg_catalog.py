"""Shared PyIceberg catalog for the gold layer's two Iceberg tables
(`iceberg_events.py`, `iceberg_signals.py`) — one SQL catalog, one
warehouse, one `gold` namespace for both."""

from pyiceberg.catalog import Catalog, load_catalog

from clear_pipeline.signals.config import settings

NAMESPACE = "gold"


def catalog() -> Catalog:
    warehouse = settings.iceberg_warehouse or f"s3://{settings.s3_bucket}/gold-iceberg"
    properties: dict[str, str] = {
        "type": "sql",
        "uri": settings.iceberg_catalog_uri,
        "warehouse": warehouse,
    }
    if warehouse.startswith("s3://"):
        properties |= {
            "s3.endpoint": settings.s3_endpoint,
            "s3.region": settings.s3_region,
            "s3.access-key-id": settings.s3_access_key_id,
            "s3.secret-access-key": settings.s3_secret_access_key,
            # This pipeline always talks to a custom S3-compatible endpoint
            # (providers/s3.py), never bare AWS S3 — path-style addressing
            # is what those backends (e.g. MinIO) expect.
            "s3.path-style-access": "true",
        }
    return load_catalog("gx_pipeline", **properties)


def ensure_namespace(cat: Catalog) -> None:
    if NAMESPACE not in [ns[0] for ns in cat.list_namespaces()]:
        cat.create_namespace(NAMESPACE)
