---
trunk: dev
featureBase: dev
deployTrigger: prod
promotionChain:
  - dev
  - prod
---

# Git flow for this repo

**Model**: simple-develop-trunk (`dev` is the integration branch, `prod` is production)

**Promotion chain**: `dev` → `prod`

- **featureBase** (`dev`) — new feature PRs (the output of `/ship-ticket`) target this branch.
- **deployTrigger** (`prod`) — work is live in production only when it reaches `prod`. When a PR merges into this branch, the GitHub Action scaffolded by `/setup-merge-hook` acts on any linked Tickets: transitioning them from `QA` to `DONE`. A merge into `dev` is not done: it ships to dev only.

## How skills use this file

- `/ship-ticket` reads `featureBase` to set the base branch of new PRs.
- `/setup-merge-hook` reads `deployTrigger` to set the `on.pull_request.branches` filter for the GitHub Action.
- The Action scans **Rollup PRs** (PRs that promote work between chain nodes) for child PR references in commit messages so Tickets linked to feature PRs are still promoted when their work reaches the deployTrigger through the chain. Here the Rollup PRs are the "Sync prod with dev" PRs (`dev` → `prod`).

## Branches

| Branch | Role |
|---|---|
| `dev` | Trunk and featureBase. GitHub's default branch. A push runs `release-dev.yml`, which checks dev's clear-api data source rows (below) and then builds and pushes the dev image. |
| `prod` | Production and deployTrigger. Advanced by "Sync prod with dev" PRs and by prod hotfix PRs, both merged by a person (no auto-merge). A push runs `release-prod.yml`, which checks prod's clear-api data source rows (below), then tags a MAJOR version and builds the prod image. |
| `master` | **Legacy — do not target.** History only. |
| `feat/*`, `fix/*`, `<ticket>-*` | Feature branches. Branch from `dev`, PR back into `dev`. |

### Prod sync and hotfixes

- **Sync:** a PR from `dev` into `prod`, titled "Sync prod with dev". It ships every PR merged into `dev` since the last sync, so it must carry their **Manual deploy steps**, and nobody merges it until each prod step is done (clear [#1525](https://www.exponential.im/w/clear/products/clear/tickets/1525)). `sync-pr-body.yml` writes the body and holds the PR at `BLOCKED` with a *changes requested* review while any `prod` box is unticked.
- **Hotfix:** branch from `prod`, PR into `prod` (for example #182, #184), then merge the fix back into `dev` the same day so the next sync doesn't revert it. A hotfix now needs one approving review like any prod merge, and nothing checks its **Manual deploy steps** (clear [#1550](https://www.exponential.im/w/clear/products/clear/tickets/1550)).

### `prod` branch protection

Set 2026-11-20 (clear [#1556](https://www.exponential.im/w/clear/products/clear/tickets/1556)): one approving review required, no force pushes, no deletions, and **no required status checks** — `check.yml` here runs only on PRs into `dev`, so requiring a check on `prod` would leave every prod PR pending on a check that never reports. clear-api's protection is identical, deliberately, even though its `check.yml` does run on prod PRs.

`enforce_admins` is **off** on purpose: an administrator can merge without an approval, so an emergency hotfix is never wedged behind an absent reviewer. The gate is loud, not absolute, and an override shows in the PR timeline.

### Deploy gate: a clear-api `data_sources` row per ingested source

Both release workflows run one job before the image is built (clear [#1523](https://www.exponential.im/w/clear/products/clear/tickets/1523)):

```bash
uv run --frozen python -m clear_pipeline.checks.data_sources
```

It reads every source that resolves a clear-api `DataSource` id by name out of `CONNECTORS` and `GX_SOURCES`, reads `dataSources` out of the environment the image is about to run against, and lists **every** name that environment is missing. A failure means no new image, so the environment keeps serving the last good one while the rows are added — the running pipeline is never left part-deployed.

| | dev | prod |
|---|---|---|
| URL | `vars.CLEAR_API_URL_DEV`, default `https://dev-api.clearinitiative.io/graphql` | `vars.CLEAR_API_URL_PROD`, default `https://api.clearinitiative.io/graphql` |
| key | `secrets.CLEAR_API_KEY_DEV` | `secrets.CLEAR_API_KEY_PROD` |

Exit codes are distinct on purpose: `1` is "rows are missing, write a clear-api migration", `2` is "clear-api could not be read, which is not a verdict on the rows".

It is **not** a code-location-load check. Raising on import would turn "one source cannot ingest" into "nothing runs and the Dagster UI is down", and would fire on a clear-api blip as readily as on a real missing row — see the module docstring in `src/clear_pipeline/checks/data_sources.py`, and `CONTRIBUTING.md`'s "a missing credential must not stop the definitions loading".

The override is a manual run with `skip_data_source_check` ticked (`gh workflow run release-prod.yml -f skip_data_source_check=true`). Like `enforce_admins: false` above, it is loud rather than absolute: the input is recorded in the run for good.

The model changed from trunk-based (`dev` as deployTrigger) on 2026-11-20 ([clear #1530](https://www.exponential.im/w/clear/products/clear/tickets/1530)): with `dev` as the trigger, Tickets read `DONE` before their code was live, which hid that the iom_dtm_flash_alerts source never worked on prod ([postmortem](https://www.exponential.im/w/clear/pages/cmwop8nqh0001jk07nek6zwjn)).

## Conventions

- Branch from `dev`, named `feat/<short-slug>` (matching existing `feat/situation-analysis`, `feat/datapoint-extraction`).
- Merge via PR, not direct push.
- Record the branch and PR on the Ticket:

  ```bash
  exponential tickets update --id <ticket-cuid> --branch feat/<slug> --pr <pr-url>
  ```

  ⚠️ **The reverse lookup is broken in `exponential-cli` 1.6.0.** `tickets list --branch <name>` and `--pr <url>` silently ignore the filter — a nonexistent branch returns all 266 tickets in the product rather than zero, with no error (verified 2026-07-15). Don't build automation on it; a caller gets an arbitrary ticket and no signal that anything went wrong. Storing the values is still worthwhile — they show up in `tickets get` and the Exponential UI, and the lookup should start working once the API filter is fixed.

## Ticket status on merge

| Event | Ticket status |
|---|---|
| `/start-ticket` — branch checked out, work begins | `IN_PROGRESS` |
| `/ship-ticket` — PR open, awaiting review/QA | `QA` |
| PR merged into `dev` (running on dev) | stays `QA` |
| Its commit reaches `prod` in a sync PR (deployTrigger) | `DONE` |

`/setup-merge-hook` automates the final transition. It has not been run for this repo, so `QA → DONE` is manual: move a Ticket to `DONE` only once the sync PR that carries its commit has merged into `prod`. When the hook is set up it must filter on `prod` and read the sync PR's commits, which name each feature PR as `(#<n>)` (feature PRs are squash-merged into `dev`).
