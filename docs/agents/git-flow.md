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
| `dev` | Trunk and featureBase. GitHub's default branch. A push runs `release-dev.yml`, which builds and pushes the dev image. |
| `prod` | Production and deployTrigger. Advanced by "Sync prod with dev" PRs and by prod hotfix PRs, both merged by a person (no auto-merge). A push runs `release-prod.yml`, which tags a MAJOR version and builds the prod image. |
| `master` | **Legacy — do not target.** History only. |
| `feat/*`, `fix/*`, `<ticket>-*` | Feature branches. Branch from `dev`, PR back into `dev`. |

### Prod sync and hotfixes

- **Sync:** a PR from `dev` into `prod`, titled "Sync prod with dev". It ships every PR merged into `dev` since the last sync, so it must carry their **Manual deploy steps**, and nobody merges it until each prod step is done (clear [#1525](https://www.exponential.im/w/clear/products/clear/tickets/1525)). `sync-pr-body.yml` writes the body and holds the PR at `BLOCKED` with a *changes requested* review while any `prod` box is unticked.
- **Hotfix:** branch from `prod`, PR into `prod` (for example #182, #184), then merge the fix back into `dev` the same day so the next sync doesn't revert it. A hotfix needs one approving review like any prod merge, and its **Manual deploy steps** section is now checked: `check.yml` runs on PRs into `dev` *and* `prod`, with its manual-steps gate on every such PR whose head branch is not `dev` (clear [#1550](https://www.exponential.im/w/clear/products/clear/tickets/1550)). The sync PR (head `dev`) is exempt — its body is generated and its prod boxes are gated by `sync-pr-body.yml`'s review instead. On a hotfix the gate **warns rather than blocks**, by design: it reds the PR, but `prod` requires no status checks, so anyone with merge rights can merge over it and an administrator can merge without the approval too. A hard block on a prod fix at 2am is its own incident; writing the step down is what costs seconds, and the author owns it until the `prod` box is ticked.

### `prod` branch protection

Set 2026-11-20 (clear [#1556](https://www.exponential.im/w/clear/products/clear/tickets/1556)): one approving review required, no force pushes, no deletions, and **no required status checks**. The original reason was that `check.yml` here ran only on PRs into `dev`, so requiring a check on `prod` would have left every prod PR pending on one that never reported. That reason is gone as of #1550 — `check.yml` now runs on prod PRs in both repos — so requiring it is a decision to take rather than an impossibility; it is still not required, which is what makes the hotfix gate a warning (below).

`enforce_admins` is **off** on purpose: an administrator can merge without an approval, so an emergency hotfix is never wedged behind an absent reviewer. The gate is loud, not absolute, and an override shows in the PR timeline.

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
