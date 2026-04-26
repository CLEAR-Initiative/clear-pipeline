# CLEAR Pipeline Insights — Proposal

> **Status:** Draft proposal, for James + Prajava review. Owner: James. 2026-04-23.
> Written following the meeting on pipeline performance and cost tracking. This is a proposal, not a decision — please mark it up.

## 1. What we said in the meeting

We agreed that the pipeline is currently "guarded by specialty knowledge" — to know what is happening you have to read the database or trawl Railway logs that vanish on restart. Concretely we identified four overlapping needs:

1. **Cost visibility.** We are burning through Anthropic credits faster than expected. We do not currently know how many Claude calls per signal, per day, per environment. We suspect dev and prod are sharing a key and double-billing.
2. **A factory-line dashboard.** Prajava's analogy: every productive factory has one big board showing each station green/yellow/blocked plus throughput. We want the same for the pipeline so anyone — not just whoever wrote the code — can see what is happening, where it is breaking, and at what cost.
3. **Prompt iteration with feedback.** We have three Claude calls (classify, group, assess). We want to browse calls, rate them, see how prompt changes move the numbers. This is the only honest way to improve them.
4. **A way to compare Claude vs Nikita's eventual model.** Nikita is supposed to replace Claude with a proprietary model. We need a substrate to run both side-by-side on the same inputs and look at the difference, not vibes.

Prajava's caveat is real and worth keeping front and centre: **nobody on the team has yet defined "good"** for any of these stages. So this proposal aims at *enabling measurement first*, not at picking a metric. Once we can see the calls and rate them, the metric will reveal itself.

## 2. The shape I am proposing

Three components, one new repo, one minimal change to this repo.

```
┌─────────────────────────────┐         ┌──────────────────────────┐         ┌─────────────────────────────┐
│  clear-pipeline             │         │  Insights DB (Postgres)  │         │  clear-pipeline-insights    │
│  (this repo)                │         │                          │         │  (new, separate repo)       │
│                             │  write  │  llm_call                │  read   │                             │
│  Wrap call_claude() to      │ ──────► │  pipeline_run            │ ◄────── │  Streamlit dashboard:       │
│  emit one row per Claude    │         │  rating                  │         │  • Factory line health      │
│  call + one row per signal  │         │  signal_processing       │         │  • Cost per stage / env     │
│  processed.                 │         │                          │         │  • Browse + rate calls      │
│                             │         │                          │         │  • Prompt A/B + diffs       │
│  Same wrapper for           │         │                          │         │  • Run-vs-run comparison    │
│  Nikita's classifier when   │         │                          │         │                             │
│  it lands.                  │         │                          │         │                             │
└─────────────────────────────┘         └──────────────────────────┘         └─────────────────────────────┘
       prod / staging / local                  one shared DB,                        runs locally + deployed
       all write here                          tagged by env                         (Railway, Streamlit Cloud,
                                                                                    or anywhere)
```

### Why this shape

- **One shared DB, tagged by env.** Local dev, staging, and prod all write to the same Postgres, every row tagged with `env`. This is the only way to actually compare staging vs prod, or "Nikita on the same signals as Claude". Splitting the DB per env defeats the purpose.
- **Dashboard in its own repo.** It serves multiple pipelines (this one, future ones, possibly IFRC's), is a different language stack from production code potentially, and has a different user group (us as engineers, not field teams). Prajava made this point explicitly in the meeting and I agree.
- **Instrumentation in this repo.** It is one wrapper file. Putting it elsewhere is over-engineering.

## 3. Concrete data model

Four tables. Plain Postgres, no exotic dependencies.

### `pipeline_run`

A run is a logical batch of signal processings under one configuration. The default in production is one open-ended "live" run per env. Experiments create new runs.

```sql
CREATE TABLE pipeline_run (
  id              uuid PRIMARY KEY,
  name            text NOT NULL,           -- 'live-prod', 'live-staging', 'nikita-classifier-v1', 'claude-prompt-v3'
  env             text NOT NULL,           -- 'prod' | 'staging' | 'local-james' | 'experiment'
  pipeline_repo   text NOT NULL,           -- 'clear-pipeline' (so a future IFRC pipeline can also write)
  git_sha         text,
  started_at      timestamptz NOT NULL DEFAULT now(),
  ended_at        timestamptz,             -- null = still running
  config          jsonb NOT NULL           -- model, thresholds, prompt versions in use
);
```

### `llm_call`

One row per Claude (or future model) call. This is the workhorse.

```sql
CREATE TABLE llm_call (
  id                  uuid PRIMARY KEY,
  run_id              uuid NOT NULL REFERENCES pipeline_run(id),
  stage               text NOT NULL,       -- 'classify' | 'group' | 'assess'
  prompt_version      text NOT NULL,       -- e.g. git sha of prompts/classify.py at call time, or a tagged version
  model               text NOT NULL,       -- 'claude-sonnet-4-6', 'nikita-v1'
  signal_id           text,                -- CLEAR signal id (nullable: assess is per-event)
  event_id            text,
  system_prompt       text NOT NULL,
  user_prompt         text NOT NULL,
  raw_response        text NOT NULL,
  parsed_response     jsonb,               -- null if parse failed
  parse_error         text,                -- non-null = failure
  input_tokens        int,
  output_tokens       int,
  cache_read_tokens   int,
  cache_create_tokens int,
  cost_usd            numeric(10,6),       -- computed from tokens × model price
  latency_ms          int,
  created_at          timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX ON llm_call (run_id, stage, created_at);
CREATE INDEX ON llm_call (signal_id);
```

### `signal_processing`

One row per (signal, run). Lets us see the full trajectory of a signal — what stages it cleared, where it dropped out — without joining four tables.

```sql
CREATE TABLE signal_processing (
  id              uuid PRIMARY KEY,
  run_id          uuid NOT NULL REFERENCES pipeline_run(id),
  signal_id       text NOT NULL,
  ingested_at     timestamptz NOT NULL,
  classified_at   timestamptz,
  relevance       numeric,
  severity        int,
  passed_relevance_gate boolean,
  event_id        text,                    -- null if dropped
  event_action    text,                    -- 'add_to_existing' | 'create_new' | null
  alert_id        text,                    -- null if not escalated
  should_alert    boolean,
  total_cost_usd  numeric(10,6),
  total_latency_ms int,
  failed_at_stage text,                    -- 'classify' | 'group' | 'assess' | null
  error_message   text
);
CREATE UNIQUE INDEX ON signal_processing (run_id, signal_id);
```

### `rating`

For when we sit down and grade calls. Multiple raters per call is fine.

```sql
CREATE TABLE rating (
  id           uuid PRIMARY KEY,
  llm_call_id  uuid NOT NULL REFERENCES llm_call(id),
  rater        text NOT NULL,              -- email or handle
  stars        smallint NOT NULL CHECK (stars BETWEEN 1 AND 5),
  notes        text,
  created_at   timestamptz NOT NULL DEFAULT now()
);
```

### Cost storage note

We compute cost at write time from a lookup table of model prices kept in the dashboard repo (cheap to update). Storing `cost_usd` directly means historical costs do not retroactively change when prices do.

## 4. The instrumentation change to this repo

Today every Claude call goes through one function — [src/clients/claude.py:22](src/clients/claude.py#L22). That is the entire surface area we need to instrument. The change is:

1. `call_claude()` accepts an optional `stage` and contextual ids (signal_id, event_id).
2. Before the call: capture system+user prompt, prompt version, model, run_id (from a contextvar set per Celery task).
3. After the call: capture response, tokens (`response.usage`), latency, parse outcome, compute cost.
4. Insert one `llm_call` row. On failure, still insert a row with `parse_error`.
5. In `process_signal` ([src/tasks/process.py:48](src/tasks/process.py#L48)) wrap the body in a `signal_processing` row that gets updated as each stage completes.

The wrapper is fire-and-forget asynchronous (own thread or a small queue) so it cannot slow down the pipeline or fail the task. Worst case we lose a few rows on crash, which is acceptable for telemetry.

**Single point of change.** Once the wrapper exists, Nikita's classifier just calls the same wrapper with `model='nikita-v1'` and gets the same observability for free.

## 5. The dashboard

A small Streamlit app. Streamlit because:
- It is Python (matches the team's stack and Nikita's stack).
- We can build the first useful page in an afternoon.
- It runs locally with `streamlit run app.py` *and* deploys to Streamlit Community Cloud or Railway with no rewrite. Same code path local and deployed — addresses the "should it be local or shared" question by being both.

Pages, in priority order:

1. **Pipeline health (the factory line).** For each env, a strip showing the last hour: signals in → classified → passed gate → grouped → escalated. Each station shows count, error rate, p50 latency, $/hour. Green/yellow/red. This is the one Prajava actually asked for.
2. **Cost dashboard.** $/day per env, per stage, per model. Stacked bars. Anomaly callout when today's run rate exceeds yesterday's by some %. Answers "are we burning credits faster than we think" without anyone running a SQL query.
3. **Call browser.** Filterable list of `llm_call` rows. Click one to see the full system+user prompt, the response, and a 5-star rating widget. This is the prompt-iteration surface. Filter by stage, env, prompt version, rated/unrated, low/high relevance, etc.
4. **Prompt versions.** Diff prompt versions side by side. For each version, aggregate stats: avg relevance, avg severity, classification distribution, % passing the gate, $/call.
5. **Run comparison.** Pick two `pipeline_run` rows. For overlapping signal_ids, show where they agree and disagree on classification, grouping, alert decision. This is how we'll evaluate Nikita's model when it lands.

Read-only for the dashboard, with one exception: writing ratings. Auth: behind whatever single sign-on we use (we can lean on the existing auth or just basic-auth for the v0).

## 6. Where it lives

### Repo

A new repo, `clear-pipeline-insights`. Independent deploy, no coupling to the production pipeline. This matches Prajava's "global super admin for everything we do here" framing — it is a different user group, different deploy cadence, and could in future serve a second pipeline.

### Database

I propose a **new Postgres database** dedicated to insights, separate from the clear-api production DB. Reasons:
- A telemetry firehose growing unbounded should not share a DB with the production app DB.
- It can have a different backup / retention policy (we can drop rows older than 90 days without worrying).
- It can be hosted on the cheapest Postgres on Railway / Neon / Supabase free tier.

Connection string lives in env. The same DB is reachable from prod, staging, and our laptops. We tag rows with `env` and never delete based on env (so we can compare).

### Privacy

Every `llm_call` row contains the full prompt, which contains signal text from Dataminr. This is the same data already in the CLEAR DB, so it does not change our exposure. But the insights DB is reachable by everyone on the engineering team — fine for us, but worth flagging if we ever want to share dashboard access more widely.

## 7. Build plan

Phased so we get value fast and can stop after any phase.

**Phase 1 — the leak detector (1–2 days).** Just `llm_call` table + the `call_claude` wrapper + a tiny "cost per env per day" page. This alone solves the meeting's most acute pain (we suspect we are over-spending and don't know where).

**Phase 2 — the factory line (2–3 days).** Add `signal_processing` table + the health-strip page. Now anyone on the team can see the pipeline running.

**Phase 3 — call browser + ratings (2–3 days).** Add the rating table + the browse-and-rate page. Now we can start to define "good" by example.

**Phase 4 — prompt versioning + run comparison (3–5 days).** Add prompt-version surfaces and the run-vs-run comparison page. This is what we'll need when Nikita's model is ready to compare against.

Phases 1 + 2 together address the meeting's immediate asks. Phases 3 + 4 are the longer-game investment.

## 8. Things I am explicitly not doing

- Not building a labelling tool sophisticated enough for ML training — that is Nikita's domain when he's ready.
- Not introducing a metrics stack (Prometheus, Grafana, OpenTelemetry). Premature; Postgres + Streamlit gives us 90% of the value at 10% of the operational cost.
- Not changing the production pipeline beyond the one wrapper. The instrumentation is additive and reversible.
- Not defining what "good" looks like for any of the three Claude stages. That has to come from us looking at real calls in the dashboard.

## 9. Open questions back to the team

These are decisions I would like input on before starting Phase 1.

1. **Where does the insights DB live?** New Railway Postgres? Neon? Supabase? I lean Railway for consistency, Neon for cheapness. Either works.
2. **Should Nikita be in the loop on the data model now?** If his classifier writes to `llm_call` with `model='nikita-v1'`, the schema needs to fit his outputs too. Worth a 20-minute call before I write the schema.
3. **One run per env, or one run per pipeline restart?** I lean "one open-ended live run per env, that we manually rotate when we make a meaningful config change". Lower noise.
4. **Prompt versioning — git sha of the prompt file, or an explicit version string in the prompt module?** I lean explicit version string (e.g. `CLASSIFY_PROMPT_VERSION = "v3"` in [src/prompts/classify.py](src/prompts/classify.py)) so we can deliberately mark a change rather than track every whitespace edit.
5. **Auth on the dashboard — do we wire it to existing CLEAR auth, or stand up something basic for now?** Standing up basic-auth gets us moving today.

## 10. What I am proposing to do today

Given Auto mode and the meeting outcome, my plan unless you push back:

1. Create the `clear-pipeline-insights` repo skeleton (Streamlit + Postgres + a config for the prices table).
2. Add the `llm_call` table migration.
3. Wrap `call_claude()` in this repo to write to it (dual-write — no behaviour change, just instrumentation).
4. Ship a Phase 1 dashboard with one page: $/day per env per stage per model.

That is enough to know whether the credit burn is a dev-vs-prod key sharing problem, a per-signal call count problem, or an actual prompt size problem — which is the question we left the meeting with.

---

### Appendix — Why not the alternatives I considered

- **Just put it in the existing CLEAR app.** Tempting, but the user group is different (engineers vs field teams), the data shape is different (high-volume telemetry vs curated alerts), and coupling them means every dashboard tweak is a production deploy.
- **Use Langsmith / Helicone / Langfuse.** Real options. Langfuse self-hosted is the closest fit. The argument *for* it: we get the call-browser and rating surface for free. The argument *against*: it doesn't model `pipeline_run` or signal-level processing the way we need to compare against Nikita's classifier, so we'd still build half of this on top. Worth revisiting after Phase 1 if the bespoke dashboard feels heavy.
- **OpenTelemetry + Grafana.** Right answer if we had ten pipelines. Wrong answer for two engineers wanting to look at Claude calls tomorrow.
