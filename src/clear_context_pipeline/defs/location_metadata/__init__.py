"""Location-metadata ingests: HAPI (daily) + IOM DTM (monthly).

Periodically refresh the ``locationMetadata`` context/baseline layer in
clear-api from external humanitarian APIs, keyed on admin p-codes and joined to
the locations tree. Two Dagster jobs:

  - ``location_metadata_daily``   — 7 HAPI v2 endpoints (UNHCR refugees /
    returnees, OCHA HPC needs, OCHA FTS funding, IPC food security, WFP food
    prices, OPHI poverty) + OCHA 3W operational presence.
  - ``location_metadata_monthly`` — IOM DTM displacement (admin 0/1/2).

Both iterate ``pipelineCountries`` (Sudan / Afghanistan / Venezuela today) and
write through clear-api's ``upsertLocationMetadataBatch``. See ``assets.py`` for
the graph and ``docs/data-source-specs/hapi.md`` for the endpoint catalogue.

Overlap note: OCHA 3W IS a HAPI endpoint (operational-presence), so it lives on
the HAPI daily job. HAPI here covers UNHCR/OCHA baseline only — IDP displacement
stays with the direct IOM DTM integration (no HAPI IDP endpoint), so the two
never double-count.
"""
