"""Generic bronze -> silver -> gold medallion pipeline, GX-gated at every
promotion. Implements docs/data-quality-<source>-pipeline-map.md for each
registered source (see ``sources.py``).

**Add a data source = add a ``MedallionSource`` adapter to
``sources.MEDALLION_SOURCES``** — ``factory.py`` and ``assets.py`` need no
change, mirroring ``defs/signals``' ``CONNECTORS`` registry pattern.

Deliberately isolated from ``defs/signals/``: this stands up the
Dagster-native target architecture alongside today's production path
(createSignal at poll time), not a replacement for it yet. Nothing here
writes to clear-api until the per-source ``<source>_push`` asset — bronze/
silver/gold are all S3 artifacts. See ``factory.py``'s docstring for the
documented simplifications this first pass makes.
"""
