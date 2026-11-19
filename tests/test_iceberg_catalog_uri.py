from unittest.mock import patch

import pytest

from clear_pipeline.defs.gx_pipeline import iceberg_catalog


def test_unset_uri_fails_loudly():
    with (
        patch.object(iceberg_catalog.settings, "iceberg_catalog_uri", ""),
        pytest.raises(Exception, match="URI is required"),
    ):
        iceberg_catalog.catalog()
