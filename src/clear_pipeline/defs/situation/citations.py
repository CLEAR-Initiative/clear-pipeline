"""Re-export shim — citation resolution now lives in ``defs/analysis/citations.py``.

``defs/analysis`` is the keeper module; ``defs/situation`` is being retired at
parity (analysis-module-agentic-dag.md §15). This shim keeps situation's own
modules and tests importing ``defs.situation.citations`` working until situation
is deleted. New code must import from ``clear_pipeline.defs.analysis.citations``.
"""

from clear_pipeline.defs.analysis import citations as _canonical

# Transparent re-export of every name (public AND private — situation code/tests
# import private helpers like _SENTENCE_RE / _resolve_refs by name) so nothing
# that still imports from here breaks before situation is deleted.
globals().update({k: v for k, v in vars(_canonical).items() if not k.startswith("__")})
