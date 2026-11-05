"""Re-export shim — ``Frame`` now lives in ``defs/analysis/frame.py``.

``defs/analysis`` is the keeper module; ``defs/situation`` is being retired at
parity (analysis-module-agentic-dag.md §15). This shim keeps situation's own
modules and tests that still do ``from .frame import …`` working until
situation is deleted. New code must import from
``clear_pipeline.defs.analysis.frame``.
"""

from clear_pipeline.defs.analysis.frame import (  # noqa: F401  (re-export)
    FALLBACK_SCOPE_LABEL,
    Frame,
    build_rag_filters,
    country_frame,
    scope_label,
)
