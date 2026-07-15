---
trunk: dev
featureBase: dev
deployTrigger: dev
promotionChain:
  - dev
---

# Git flow for this repo

**Model**: trunk-based

**Promotion chain**: `dev` (single node — no intermediate branches)

- **featureBase** (`dev`) — new feature PRs (the output of `/ship-ticket`) target this branch.
- **deployTrigger** (`dev`) — when a PR merges into this branch, the GitHub Action scaffolded by `/setup-merge-hook` transitions any linked Tickets from `QA` to `DONE`.

## How skills use this file

- `/ship-ticket` reads `featureBase` to set the base branch of new PRs.
- `/setup-merge-hook` reads `deployTrigger` to set the `on.pull_request.branches` filter for the GitHub Action.
- The Action scans **Rollup PRs** (PRs that promote work between chain nodes) for child PR references in commit messages so Tickets linked to feature PRs are still promoted when their work reaches the deployTrigger through the chain. This repo has a single-node chain, so there are no Rollup PRs.

## Branches

| Branch | Role |
|---|---|
| `dev` | Trunk, featureBase, and deployTrigger. GitHub's default branch. |
| `master` | **Legacy — do not target.** Strictly behind `dev` (0 ahead, 8 behind; last touched 2026-07-10, verified 2026-07-15). History only. |
| `feat/*` | Feature branches. Branch from `dev`, PR back into `dev`. |

`master` is not part of the promotion chain. The trunk-based model was chosen deliberately: none of the intermediate branch names the heuristic scans for (`develop`, `staging`, `release`, `qa`, `uat`, `preprod`) exist here. If `master` is ever revived as a release branch, re-run `/setup-git-flow` — the chain would become `dev → master` and `deployTrigger` would move to `master`.

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
| PR merged into `dev` (deployTrigger) | `DONE` |

`/setup-merge-hook` automates the final transition. It has not been run for this repo — `QA → DONE` is currently manual.
