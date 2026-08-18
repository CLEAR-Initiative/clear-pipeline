# CLEAR Pipeline — Dagster Migration Proposal

> **UPDATE 2026-08-13 — CONSOLIDATION.** The design below (a separate Dagster code
> location in clear-pipeline) was superseded: the signal pipeline is being **ported
> INTO clear-context-pipeline** (one Dagster project for KB/situation **and**
> signals→events→alerts), and **clear-pipeline is deprecated at the end of the
> migration**. Rationale: two repos force cross-repo duplication when a source needs
> utilities from both. The signal domain now lives under
> `src/clear_context_pipeline/signals/` (config/models/sources/services/prompts) with
> assets under `defs/signals/`, reusing the shared providers (`clear_api._execute`,
> `providers/s3`, `make_llm_provider`). **Cutover is BIG-BANG, not shadow/dual-run:**
> build all sources → test locally → deploy with event schedules `default_status=STOPPED`
> → stop clear-pipeline Celery → enable all Dagster event jobs at once. The design
> principles below (raw-only lake, eager queue-drain, add-a-source=add-a-connector,
> durable clear-api status markers) still hold — only the destination + cutover changed.

**Status:** Design settled · consolidation in progress · **Related:**
[ARCHITECTURE.md](./ARCHITECTURE.md), the clear-context-pipeline Dagster patterns.

This document proposes migrating clear-pipeline from Celery to Dagster, and re-homing
raw source payloads into an S3 data lake. It captures a settled design; it is not an
implementation plan (phasing is sketched in §7, but each phase gets its own ticket).

---

## 1. Motivation

clear-pipeline today runs entirely as **Celery workers + an embedded beat scheduler
against Redis** (see [ARCHITECTURE.md](./ARCHITECTURE.md)). Orchestration is implicit:
beat fires pollers, each poller fans out per-signal via `.delay()`, and tasks coordinate
through Redis locks + clear-api as the shared store. There is no declared DAG, no central
run history, and beat is embedded in a single worker (so the service can't scale past one
replica without double-scheduling).

We want to move to Dagster for four reasons, all of which apply:

1. **Ops consolidation** — clear-context-pipeline already runs on Dagster. One orchestrator
   across both pipelines retires the separate Celery + beat + Redis-broker operational surface.
2. **Observability & lineage** — run history, asset lineage, and a UI over the pipeline.
   (Per-Claude-call telemetry stays in the pipeline-insights dashboard — see §9.)
3. **Backfills & reprocessing** — today the backfill scripts are hand-run argparse tools with
   no retries, partitioning, or observability. Reprocessing after a prompt change is unmet.
4. **Reliability & retries** — a central scheduler (no single-replica beat), first-class retry
   and alerting.

**Two facts make this a clean fit rather than a fight with the framework:**

- **No sub-minute polling.** Poll cadences are minutes-to-an-hour (Dataminr **60 min in prod** —
  the authoritative value; the local `.env`'s `POLL_INTERVAL_SECONDS=120` is dev-only and ignored
  for sizing. GDACS 30 min; ACLED 60 min). There is **no real-time hot path** — every source sits
  comfortably in Dagster's scheduling range. Celery can be retired entirely.
- **Raw is already persisted** (`signal.raw_data`). The lake **relocates** an existing durable
  store to S3; it is not net-new capture.

---

## 2. Constraints & non-goals

- **Latency:** signal→alert tolerates ~1 min *processing* lag. (Data freshness is bounded by the
  poll cadence regardless, so a one-daemon-tick delay downstream is immaterial.)
- **The lake holds raw blobs only.** Every processed entity (signal, event, alert, crisis) stays
  in the clear-api database, which remains the **system of record**.
- **No streaming/real-time requirement** — confirmed by the poll cadences above.
- **Not** re-modelling clear-api's entity graph. The only clear-api change is adding durable
  status markers (§4 D5) and swapping the trigger mechanism (§6).

---

## 3. Target architecture

```
Dataminr / ACLED / GDACS / <future>
        │  poll (periodic, cheap — the ONLY time-driven component)
        ▼
  S3 raw blobs (bronze)                +   clear-api: createSignal(status=NEW, s3_ptr)
  layout: raw/<source>/<date>/…            (raw blob → S3; DB row keeps a pointer)
        │
        │  materialize raw_<source> ONLY when the poll returned new rows
        ▼  (AutomationCondition.eager on everything below)
  drain NEW signals ▶ classify ▶ group into events ▶ escalate to alerts
        │            (dynamic fan-out over the pending batch; idempotent)
        ▼
  clear-api (system of record — mutable entities: signal / event / alert / crisis)
        │  pending flag ▶ sensor
        ▼
  enrich_crisis · translate · process_manual_signal   (clear-api-triggered)

  Backfill (separate, on-demand): date-partitioned job reads the SAME S3 raw for a
  range and replays the stateless stages (normalize / classify). See D7.
```

**Reading of the flow:** the only thing on a timer is a cheap HTTP poll. An empty poll produces
no materialization, so no downstream work runs. When a poll lands new data, `eager` automation
propagates through classify → group → alert within a daemon tick (~30 s). End-to-end
poll→alert ≈ poll interval + tick + processing, well inside the ~1 min processing budget.

---

## 4. Key design decisions

**D1 — Lake = raw only; clear-api = system of record.**
S3 holds append-only raw payloads (`raw/<source>/<date>/…`). All processed/mutable entities stay
in clear-api. `createSignal` stores an **S3 pointer** instead of the raw blob. This is the single
most important boundary: do not model events/alerts/crises as lake assets — they are stateful,
order-dependent transactional entities.

**D2 — Poll assets are the only scheduled components.**
One ingest asset per source, driven by a sensor with a cursor-editable interval. It writes raw to
S3 and creates signals. Polling is irreducibly periodic (these are pull APIs); if a source ever
offers a webhook/stream, that source drops the poll and becomes push-driven.

> **As shipped:** `raw_<source>` materializes on *every* tick (Dagster assets always emit a
> `MaterializeResult`), so the eager `classify_group → alert → translate` chain fires on empty polls
> too — but each stage drains a queue that's empty and returns immediately, with **no LLM spend**.
> The cost claim ("empty poll → zero LLM cost") holds; the earlier "only materializes on new data /
> downstream does not fire" wording did not. Run *count* is therefore ~one drain per source tick
> plus its eager children (not "~24–48/day") — cheap no-op runs, but noisier in the run list than
> §9 implied. If that noise matters, gate the eager condition on a non-empty result later.

**D3 — Downstream is event-driven via `AutomationCondition.eager()`, plus a single-flight sensor.**
Every asset below the poll layer runs when an upstream materializes. classify_group is also ticked
by an interval sensor (to drain analyst `manual` signals, which have no ingest asset) and is
guarded by a global drain lock so overlapping runs can't double-process. Empty queue → immediate
no-op, no LLM spend.

> **Cutover:** the shipped strategy is **big-bang** (build all sources, test locally, stop Celery,
> flip the STOPPED sensors on). The shadow/dual-run described in §7/§9 was an earlier option and is
> NOT what ships — treat those sections as superseded by the big-bang header.

**D4 — Queue-drain over durable status, NOT "latest asset value".**
With unpartitioned eager assets, if two polls land before downstream runs, Dagster would
materialize once against the latest state and **silently skip the earlier batch**. To avoid that,
downstream **drains all `status=NEW` signals from clear-api** (clear-api is the work queue), rather
than consuming a value passed between materializations. Bursts never skip; retries just re-drain;
it self-heals.

**D5 — Status split by durability need.**
Today all step status lives in Redis — a Celery-era choice, and Redis here is ephemeral/flaky
(the `broker_heartbeat`/`acks_late` knobs exist because hosted Redis drops connections). The new split:

| State | New home | Why |
|---|---|---|
| Per-step completion (classified / grouped / …) | **Dagster materialization history** | Native; drop most Redis step-keys |
| Per-entity drain marker (`status`/`processed_at`) | **clear-api Postgres, on the entity** | Durable, queryable for drain + backfill, no entity/status divergence |
| Dedup locks, watermarks/cursors, short caches | **Redis (unchanged)** | Genuinely ephemeral, high-churn |

**D6 — Add-a-source = add-a-connector.** A `SourceConnector` protocol + an asset factory (§5) so a
new source is a registry entry (an API client `poll` + a `normalize`), not bespoke wiring.

**D7 — Backfill = date-partitioned job over the same S3 raw.**
Raw is laid out by `source/date` independently of the live path's (unpartitioned) automation, so a
separate partitioned job can replay a date range on demand. **Stateless stages (normalize, classify)
replay cleanly** — this covers "reprocess after a prompt change," the high-value case. **Rebuilding
the event graph** (re-grouping historical signals) is order-dependent and mutates live state; it is
**out of scope** for the first cut.

**D8 — Idempotency preserved.** Keep the existing `(source, external_id)` unique constraint in
clear-api + Redis per-entity locks. Do **not** rely on Dagster run/partition identity for streaming
dedup.

**D9 — Cross-repo trigger swap.** See §6.

---

## 5. The connector contract

The only per-source code is `poll` (the API client) and `normalize` (raw → canonical signal).
Everything else — lake writes, cursors, partition wiring, downstream dispatch — is generic.

```python
class SourceConnector(Protocol):
    source_id: str
    schedule: str                                   # cron / interval (2–60 min)
    def poll(self, cursor) -> tuple[list[RawRecord], Cursor]: ...   # API client
    def normalize(self, raw: RawRecord) -> Signal: ...             # raw → canonical

CONNECTORS = [DataminrConnector(), AcledConnector(), GdacsConnector()]  # ← add a source here

def build_ingest_asset(c: SourceConnector):
    @asset(name=f"raw_{c.source_id}")
    def _ingest(context):
        raw, cur = c.poll(load_cursor(c.source_id))
        if not raw:
            return                                  # no materialization → no downstream
        write_raw_to_lake(c.source_id, raw)         # generic (S3)
        create_signals(c, raw)                      # generic → clear-api status=NEW + s3_ptr
        advance_cursor(c.source_id, cur)            # generic (Redis)
    return _ingest

ingest_assets = [build_ingest_asset(c) for c in CONNECTORS]           # generic
```

---

## 6. Cross-repo contract change (clear-api)

Two clear-api changes are required; both are gating and coordinated across repos:

1. **Durable status markers (D4/D5)** — add `status`/`processed_at` to the signal entity and a
   pending flag to crisis/translation entities, so Dagster can drain pending work. **This is the
   Phase-1 blocker.**
2. **Trigger swap** — today clear-api enqueues `enrich_crisis`, `translate_*`, and
   `process_manual_signal` by pushing Celery messages onto the shared Redis broker *by task name*
   (an undocumented cross-repo API). These become Dagster-triggered — preferred: clear-api sets a
   **pending flag** that a Dagster sensor drains (same pattern as D4); alternative: clear-api calls
   Dagster's API to launch a run. Resolved in Phase 3, when Celery is removed.

---

## 7. Phasing

Each phase is independently shippable and leaves the pipeline working. We start with
**Dataminr** — it is **>70% of signal volume** and exercises the *full* pipeline (Claude
classify → group → assess), so Phase 1 proves the hardest, highest-value path end-to-end rather
than a simplified one. Because it is the majority of traffic, run the Dagster path in **shadow /
dual-run** against Celery and diff outputs before cutting over.

- **Phase 1 — Dataminr spine.** `DataminrConnector` + bronze lake + eager queue-drain + the full
  classify → group → alert path. Requires the clear-api status markers (§6.1). Cut over from
  Celery-Dataminr only after shadow validation.
- **Phase 2 — remaining polls (ACLED, GDACS).** Additional connectors in the registry — simpler,
  since they skip Claude classification (built from feed metadata). Migrate existing
  `signal.raw_data` → lake (one-time backfill + dual-write during cutover).
- **Phase 3 — event-driven layer, remaining periodic tasks, and Celery retirement.** The
  clear-api-triggered work (`enrich_crisis` / `translate_*` / `process_manual_signal`) moves to
  clear-api pending-flag sensors (§6.2); the remaining periodic beat tasks (daily/weekly/monthly
  digests, `archive_stale_alerts`, weekly DTM, monthly logistics) become Dagster schedules.
  **Celery is retired here.**

**Out of scope:** the one-off backfill scripts (`backfill_location_population`,
`backfill_admin_geometries`, `*_pcodes`, admin geometries) stay as local argparse tools — they are
run by hand, not on any schedule, and are not migrated.

---

## 8. Prerequisites & dependencies

- **clear-api:** status/`processed_at` on signal; pending flags on crisis/translation entities (§6.1).
- **Dagster deployment:** clear-pipeline ships as a **new code location inside the existing
  clear-context-pipeline Dagster instance** (decided — one daemon/UI across both pipelines, directly
  advancing the consolidation driver). Blast-radius isolation is handled at the code-location level.
- **S3 lake:** bucket + `raw/<source>/<date>/…` layout (can reuse the context-pipeline bucket
  under a distinct prefix).

---

## 9. Risks & open questions

- **Run volume.** At 30–60 min cadences, ingest is only ~24–48 runs/day/source — modest. Prefer a
  **sensor with cursor** (no empty runs) and prune run history to keep the UI clean.
- **Stateful re-grouping backfill** is out of scope (D7) — confirm that's acceptable for the first cut.
- **Dataminr cutover risk** — Dataminr is >70% of volume and the full Claude path, so Phase 1 must
  **shadow/dual-run** against Celery and diff before cutover (§7).
- **Insights overlap** — pipeline-insights owns *per-Claude-call* telemetry; Dagster owns *run/asset*
  lineage. Complementary, not redundant — do not rebuild call-level telemetry in Dagster.

  > **Port deviation (2026-08):** clear-pipeline's `call_claude()` chokepoint (which fed
  > pipeline-insights) is NOT carried into the Dagster port — signal-pipeline LLM calls go through
  > the shared `providers/llm.py`, which has no insights hook. **Call-level telemetry is dropped for
  > the signal pipeline**; the dead `insights_*` settings were removed. If per-call telemetry is
  > wanted later, add one reporting hook in `providers/llm.py` (the single chokepoint) rather than at
  > each call site. In the meantime, LLM spend is bounded by a per-run guardrail
  > (`signal_max_signals_per_run`, enforced in `classify_group`) — the counterpart to the KB
  > pipeline's `KB_MAX_COST_USD_PER_RUN`.
