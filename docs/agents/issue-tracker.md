# Issue tracker: Exponential

Issues and PRDs for this repo live in [Exponential](https://www.exponential.im). Use the `exponential` CLI for all operations. Add `--json` to any command (or pipe it) to get machine-readable output.

Requires `exponential-cli` **>= 1.6.0** — earlier versions lack the `tickets`, `features`, and `products` commands entirely. Note the CLI's `--version` misreports `1.0.0`; check `npm ls -g exponential-cli` instead.

## This repo's coordinates

- **Workspace**: `clear` (`cmown5nfc0001l704yrpo0efz`)
- **Product**: `clear` (`cmppd9hd10003l804t36m0471`)
- **Default feature**: _(none)_ — this repo's tickets roll up under several features; attach `--feature` per ticket.

The `clear` product spans every CLEAR repo, not just this one. Filter by feature or label when you need repo-scoped views.

`--workspace clear` is passed explicitly in the commands below because no default workspace is set on this machine (there are five). Either set one with `exponential workspaces set-default clear`, or pass the product CUID instead of the slug — `--workspace` is only required when `--product` is a slug.

## Hierarchy

`workspace → product → feature → ticket`. Epics are workspace-scoped and can group tickets across products.

A **feature** is the PRD-shaped unit (an outcome with a vision). A **ticket** is a unit of work (bug, feature slice, chore, etc.).

## Conventions

- **Create a ticket**: `exponential tickets create --product clear --workspace clear --type <TYPE> --status <STATUS> -t "<title>" -b "<body>" [--feature <feature-cuid>] [--epic <epic-cuid>] --json`. Use a heredoc for multi-line bodies.
- **Read a ticket**: `exponential tickets get <ticket-cuid> --json` (returns dependencies, actions, and comments).
- **List tickets**: `exponential tickets list --product clear --workspace clear [--status <STATUS>] [--type <TYPE>] [--feature <cuid>] [--assignee <user-id>] --json`. Status filtering is server-side — prefer that to client-side filtering.
- **Find a ticket by branch or PR**: ⚠️ **Broken in CLI 1.6.0 — do not use.** `--branch` and `--pr` are accepted and silently ignored: a nonexistent branch returns the full product list rather than zero results, so a caller gets an arbitrary ticket back and no error. Their `--help` text also wrongly claims they make `--product` optional; the API rejects the call without it. Until this is fixed, find tickets by `--status`/`--feature` and match the title yourself.
- **Comment on a ticket**: `exponential tickets comment add --id <ticket-cuid> -m "<body>"`.
- **Change a ticket's status**: `exponential tickets update --id <ticket-cuid> --status <STATUS>`.
- **Archive (close)**: `exponential tickets update --id <ticket-cuid> --status ARCHIVED`.
- **Create a feature (for PRDs)**: `exponential features create --product clear --workspace clear -n "<name>" -d "<description>" --vision "<target outcome>" --status DEFINED --json`.

### Ticket types

`BUG`, `FEATURE`, `CHORE`, `IMPROVEMENT`, `SPIKE`, `RESEARCH`.

### Ticket statuses

`BACKLOG`, `NEEDS_REFINEMENT`, `READY_TO_PLAN`, `COMMITTED`, `IN_PROGRESS`, `BLOCKED`, `QA`, `DONE`, `DEPLOYED`, `ARCHIVED`.

### Feature statuses

`IDEA`, `DEFINED`, `IN_PROGRESS`, `SHIPPED`, `ARCHIVED`.

## Triage role → ticket status mapping

The `/triage` skill routes by `ticket.status` alone — no body markers or sentinel comments needed. See `triage-labels.md` for the same mapping.

| Triage role | `ticket.status` | Notes |
|---|---|---|
| `needs-triage` | `BACKLOG` | Default landing state for new tickets |
| `needs-info` | `NEEDS_REFINEMENT` | + a comment carrying the actual clarifying question |
| `ready-for-agent` | `READY_TO_PLAN` | Agent picks these up |
| `ready-for-human` | `BLOCKED` | Semantic: blocked on human availability or judgement |
| `wontfix` | `ARCHIVED` | Terminal |

So each triage queue is a single `tickets list --status <STATUS>` call:

```bash
exponential tickets list --product clear --workspace clear --status BACKLOG --json          # needs-triage
exponential tickets list --product clear --workspace clear --status NEEDS_REFINEMENT --json # needs-info
exponential tickets list --product clear --workspace clear --status READY_TO_PLAN --json    # ready-for-agent
exponential tickets list --product clear --workspace clear --status BLOCKED --json          # ready-for-human
```

## When a skill says "publish to the issue tracker"

- If the source is **feature work** (a PRD-shaped plan for a product capability): follow the registry flow — `/to-prd` (human PRD page + native EARS requirement rows on the Feature) → `/to-robo-prd` (Agent PRD on the same page) → `/to-tickets` (few tickets, default one per scope, with the vertical slices as ordered actions). Do NOT route feature work to `/to-expo`.
- If the source is a **loose plan that doesn't belong to a registry feature** (a cross-product epic, standalone chores): invoke `/to-expo`. It handles vertical slicing, dependency wiring, and decision comments.
- If the source is a **single ticket** (e.g. a one-off bug): run `exponential tickets create ...` directly.

## When a skill says "fetch the relevant ticket"

Run `exponential tickets get <ticket-cuid> --json`. The output includes the ticket body, status, dependencies, linked actions, and the full comment thread.

## GitHub Issues

Not used for this repo. Issues are enabled on `CLEAR-Initiative/clear-pipeline` but empty — treat Exponential as the source of truth.


## Wayfinding operations

Used by `/wayfinder`. The **map** is a Feature; its tickets are the map's children.

- **Map**: a Feature named `Wayfinder: <destination>` (`exponential features create --product clear -n "Wayfinder: <destination>" -d "<one-line destination>" --status DEFINED --json`). The map body (Notes / Decisions-so-far / Fog) lives on a linked Knowledge page: `exponential pages create -t "Wayfinder map: <destination>" --body-file <path> --json`, then `exponential features link-page --feature <id> --page <id>`. Use an **epic** instead of a feature only when the destination spans products.
- **Child ticket**: a ticket under the map feature (`exponential tickets create --product clear --feature <map-feature-cuid> ...`). Ticket types map as: `research` → `RESEARCH`, `grilling` → `RESEARCH`, `prototype` → `SPIKE`, `task` → `CHORE`. HITL types (`grilling`, `prototype`) get `--status NEEDS_REFINEMENT`; AFK types (`research`, and `task` when an agent can do it) get `--status READY_TO_PLAN`.
- **Blocking**: native edges — `exponential tickets block <child-cuid> --by <blocker-cuid>`. A ticket is unblocked when `openBlockerCount` is 0.
- **Frontier query**: `exponential tickets list --feature <map-feature-cuid> --json`, keep open tickets with `openBlockerCount == 0` and no assignee; first in map order wins.
- **Claim**: `exponential tickets update --id <cuid> --assignee <user-id>` — the session's first write. The assignee is the claim.
- **Resolve**: `exponential tickets comment add --id <cuid> -m "<the decision>"`, then `exponential tickets update --id <cuid> --status DONE`, then update the map page's Decisions-so-far with a one-line gist + the ticket CUID (`exponential pages update`).
