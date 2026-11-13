"""Re-export shim — the situation-analysis schemas now live in
``defs/analysis/schemas.py``.

``defs/analysis`` is the keeper module; ``defs/situation`` is being retired at
parity (analysis-module-agentic-dag.md §15). This shim keeps situation's own
modules and tests that still do ``from .schemas import …`` working until
situation is deleted. New code must import from
``clear_pipeline.defs.analysis.schemas``.
"""

from clear_pipeline.defs.analysis.schemas import *  # noqa: F401,F403  (re-export)
