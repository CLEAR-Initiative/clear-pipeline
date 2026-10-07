"""Task Worker drain (clear-api ADR-0010) — the production Worker.

clear-api keeps one generic Task table with a row-level lease and exposes it
as a Worker protocol over GraphQL: claim, heartbeat, complete, fail. The
``drain_tasks`` asset here is a Worker on that contract, the same queue-drain
shape as the other drains (single-flight Redis lock, interval poll sensor
that ships STOPPED), with one difference from the bespoke queues: a new kind
of work is a **handler** registered in ``worker.HANDLERS``, not a new module
and not a new queue. The first handler is ``event.impact_prior.clear`` (CLEAR
Events + knowledge base, never the web); it also claims the pre-fan-out bare
``event.impact_prior`` for one release, after ``.clear`` — see
``settings.task_impact_prior_kinds``.

Authenticates as the ``worker`` service user (``CLEAR_WORKER_API_KEY``),
never the pipeline user — only ``worker`` may claim, and it can write
nothing but Tasks it holds and ``proposed`` ImpactPriors. Auto-discovered by
``load_from_defs_folder``.
"""
