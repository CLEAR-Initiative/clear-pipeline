"""Per-source defs — generated from the source registry.

Each source's 8 assets + 6 checks + 1 job come from
``build_gx_source_assets(source)`` over ``sources.GX_SOURCES``; bound
to module-level names so ``load_from_defs_folder`` auto-discovers them.

**Add a data source = add a ``GXSource`` to ``sources.GX_SOURCES``**
— this module needs no change. Mirrors ``defs/signals/assets.py``.
"""

from clear_pipeline.defs.gx_pipeline.factory import build_gx_source_assets
from clear_pipeline.defs.gx_pipeline.sources import GX_SOURCES

_GX_DEFS = [d for source in GX_SOURCES for d in build_gx_source_assets(source)]

for _i, _def in enumerate(_GX_DEFS):
    globals()[f"gx_pipeline_def_{_i}"] = _def

del _i, _def
