"""Re-export shim — the RAG helper now lives in ``defs/analysis/rag_helper.py``.

``defs/analysis`` is the keeper module; ``defs/situation`` is being retired at
parity (analysis-module-agentic-dag.md §15). This shim keeps situation's own
modules and tests importing ``defs.situation.rag_helper`` working until
situation is deleted. New code must import from
``clear_pipeline.defs.analysis.rag_helper``.
"""

from clear_pipeline.defs.analysis import rag_helper as _canonical

# Transparent re-export of every name (public and private) — see citations.py.
globals().update({k: v for k, v in vars(_canonical).items() if not k.startswith("__")})
