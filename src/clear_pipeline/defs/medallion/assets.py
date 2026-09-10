"""Per-source medallion defs — generated from the source registry.

Each source's 8 assets + 6 checks + 1 job come from
``build_medallion_assets(source)`` over ``sources.MEDALLION_SOURCES``; bound
to module-level names so ``load_from_defs_folder`` auto-discovers them.

**Add a data source = add a ``MedallionSource`` to ``sources.MEDALLION_SOURCES``**
— this module needs no change. Mirrors ``defs/signals/assets.py``.
"""

from clear_pipeline.defs.medallion.factory import build_medallion_assets
from clear_pipeline.defs.medallion.sources import MEDALLION_SOURCES

_MEDALLION_DEFS = [d for source in MEDALLION_SOURCES for d in build_medallion_assets(source)]

for _i, _def in enumerate(_MEDALLION_DEFS):
    globals()[f"medallion_def_{_i}"] = _def

del _i, _def
