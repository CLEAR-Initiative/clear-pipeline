"""Re-export shim — the prompt builders now live in ``defs/analysis/prompts.py``.

``defs/analysis`` is the keeper module; ``defs/situation`` is being retired at
parity (analysis-module-agentic-dag.md §15). This shim keeps situation's own
modules and tests importing ``defs.situation.prompts`` working until situation
is deleted. New code must import from ``clear_pipeline.defs.analysis.prompts``.
"""

from clear_pipeline.defs.analysis import prompts as _canonical

# Transparent re-export of every name (public and private) — see citations.py.
globals().update({k: v for k, v in vars(_canonical).items() if not k.startswith("__")})
