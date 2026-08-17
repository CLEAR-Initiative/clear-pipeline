"""Per-source INGEST defs — generated from the connector registry.

Each polled source's ingest asset + poll sensor come from
``build_source_assets(connector)`` over ``connectors.CONNECTORS``; they're bound to
module-level names so ``load_from_defs_folder`` auto-discovers them. The shared
drain stages (classify_group → alert → translate) are module-level assets in
``stages.py`` and are discovered from there directly.

**Add a data source = add a connector to ``CONNECTORS``** — this module needs no
change.
"""

from clear_context_pipeline.defs.signals.connectors import CONNECTORS
from clear_context_pipeline.defs.signals.factory import build_source_assets

# Flatten [polled connector → its ingest defs] into loose module globals for
# auto-discovery. Manual (non-polled) sources contribute nothing here.
_INGEST_DEFS = [d for connector in CONNECTORS for d in build_source_assets(connector)]

for _i, _def in enumerate(_INGEST_DEFS):
    globals()[f"signal_ingest_def_{_i}"] = _def

del _i, _def
