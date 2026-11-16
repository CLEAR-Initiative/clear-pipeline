---
status: accepted
date: 2026-11-16
---

# The gx pipeline mints its Signal ids

The decision is recorded in clear-api
[ADR-0023](https://github.com/CLEAR-Initiative/clear-api/blob/dev/docs/adr/0023-pipeline-owned-signal-ids.md).
This ADR records how clear-pipeline implements it.
Feature: [Exponential "Pipeline-owned Signal ids"](https://www.exponential.im/w/clear/products/clear/features/cmwj5xiyu0001jq08tjoj5qlm).
Ticket: [Exponential #1487](https://www.exponential.im/w/clear/products/clear/tickets/1487).

## Decision

- **Id**: `signal_ids.signal_id(externalId) = uuid5(NAMESPACE, externalId)`.
  NAMESPACE is one hard-coded UUID. externalId is the prefixed key clear-api
  dedupes on (`idmc:123`), not gold's bare row key. A test pins one input and
  its output.
- **Prefixes**: `signal_ids.PREFIXES` holds one per source, and each
  `GXSource` declares its own as `id_prefix`. A test checks that no prefix
  equals or starts another, nor `x:`, `whatsapp:` or `url:`. Never rename
  one: it re-ids the source and breaks clear-api's dedupe.
- **Silver** mints the id once per signal, into the silver JSON on S3 (`id`)
  and the `signalId` column. Gold's `signalInput` does not carry it: the
  column is the only copy, and `_push` adds it to the payload.
- **Gold**: `gold.<source>_signals` gains `signalId`; the row key stays the
  bare externalId. Rows written before the column get it in the same schema
  migration, from their stored `signalInput.externalId` (no DB call). Event
  bundles list `signalId`s.
- **Push** sends the gold row's `signalId` as `id`. When clear-api answers
  with another id (the row predates the switch, so Postgres keeps its cuid),
  `_push` upserts `(signalId, externalId, legacyId)` into
  `gold.signal_id_aliases`, keyed by `signalId`. It writes the aliases
  before marking the rows pushed: a failed alias write leaves the rows
  unpushed, and the next run re-creates them and records the alias again.
- **Reading Postgres ids**: `iceberg_signals.postgres_ids(source)` gives
  `legacyId` when aliased, else `signalId`. Lake consumers use it, never
  `signalId` alone.
- **`CONFLICT`** from `createSignal` (the id is held by another
  `(sourceId, externalId)`): `_push` logs it, counts the row as `failed`,
  writes no alias and leaves the row unpushed for the next run.
- **Scope**: every row that reaches gx gold. Only `idmc`, `sudan_witness`,
  `iom_dtm_flash_alerts` and `inso` reach it in production. The legacy
  poller and `_create_signal` send no id and keep cuids.

## Deferred

- A row whose externalId is missing or unprefixed is **dropped** in `_silver`,
  logged and counted as `id_failed`. It is lost: the watermark has moved on.
  Quarantining it, or gating it with a row-level GX expectation, is a
  follow-up: [Exponential #1491](https://www.exponential.im/w/clear/products/clear/tickets/1491)
  (`TODO` in `factory.py`). GX checks here act per batch, not per
  row, so they cannot filter it today.

## Consequences

- Deploy order per environment: shape query clean (clear-api ADR-0023),
  clear-api released, then this pipeline, then a one-off export of aliases
  for gx rows pushed before the switch. Rollback runs in reverse. A pipeline
  that sends `id` to an older clear-api fails every create.
- `gold.signal_id_aliases` is shared by every source, while the gold pool is
  per source. Two sources' `_push` writing aliases at once can fail one
  Iceberg commit; that run fails and retries cleanly. Aliases are only
  written for pre-switch rows, so this is rare.
- Rebuilding silver or gold from S3 gives the same ids: the id is a pure
  function of the externalId.
