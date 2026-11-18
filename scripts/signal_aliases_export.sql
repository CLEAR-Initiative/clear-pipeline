-- Deploy step 4 (ADR-0015), part 1 of 2: list Signals whose Postgres id is
-- not the pipeline-minted UUIDv5, i.e. rows created before the switch.
-- Read-only. Run against clear-api's Postgres, per environment:
--
--   psql "<clear-api database url>" --csv -f scripts/signal_aliases_export.sql -o signal_aliases.csv
--
-- Or paste the query into TablePlus, run it, and export the result as CSV
-- with the header row on (columns id, external_id, source).
--
-- Then feed the CSV to scripts/signal_aliases_import.py.
--
-- Filtering by externalId prefix is left to the import script: it reads the
-- prefixes from GX_SOURCES, so a source added later cannot drift from this file.
-- The id is not computed here (no uuid_generate_v5): the import mints it with
-- the pipeline's own `signal_id`, so there is one implementation of the id.
SELECT s.id, s.external_id, ds.name AS source
FROM signals s
JOIN data_sources ds ON ds.id = s.source_id
WHERE s.external_id IS NOT NULL
  AND s.id !~ '^[0-9a-f]{8}-[0-9a-f]{4}-5[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$'
ORDER BY s.external_id, s.id;
