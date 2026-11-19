from unittest.mock import patch

import pytest

from clear_pipeline.defs.gx_pipeline import iceberg_catalog
from clear_pipeline.signals.config import Settings


def test_unset_uri_fails_loudly(monkeypatch):
    # The code default, not a forced value: neither the environment nor a
    # local env file may supply ICEBERG_CATALOG_URI.
    monkeypatch.delenv("ICEBERG_CATALOG_URI", raising=False)
    defaults = Settings(_env_file=None)
    with (
        patch.object(iceberg_catalog, "settings", defaults),
        pytest.raises(Exception, match="URI is required"),
    ):
        iceberg_catalog.catalog()
