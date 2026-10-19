"""Task Worker drain (clear-api ADR-0010) — the production Worker.

clear-api keeps one generic Task table with a row-level lease and exposes it
as a Worker protocol over GraphQL: claim, heartbeat, complete, fail. The
``drain_tasks`` asset here is a Worker on that contract, the same queue-drain
shape as the other drains (single-flight Redis lock, interval poll sensor
that ships STOPPED), with one difference from the bespoke queues: a new kind
of work is a **handler** registered in ``worker.HANDLERS``, not a new module
and not a new queue. ``drain_tasks_job`` runs with a ``dagster/priority``
tag so a Task does not queue behind the ingest sensors (``worker.py`` module
docstring).

No kind is registered today. The first handler, ``event.impact_prior.clear``
(with the pre-fan-out bare ``event.impact_prior``), was retired on 2026-10-08:
clear-api now computes the ImpactPrior from accepted history, and the web
Worker (a Claude routine) proposes individual cases instead. The generic drain
stays for future kinds; with nothing registered the sensor skips its ticks.

Authenticates as the ``worker`` service user (``CLEAR_WORKER_API_KEY``),
never the pipeline user — only ``worker`` may claim, and it can write
nothing but the Tasks it holds. Auto-discovered by ``load_from_defs_folder``.
"""
