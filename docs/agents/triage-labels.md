# Triage Labels

The skills speak in terms of five canonical triage roles. This repo tracks issues in **Exponential**, which has no label-based triage — the roles map to `ticket.status` instead, and `/triage` routes by status alone.

| Label in mattpocock/skills | Status in our tracker | Meaning                                  |
| -------------------------- | --------------------- | ---------------------------------------- |
| `needs-triage`             | `BACKLOG`             | Maintainer needs to evaluate this ticket |
| `needs-info`               | `NEEDS_REFINEMENT`    | Waiting on reporter for more information |
| `ready-for-agent`          | `READY_TO_PLAN`       | Fully specified, ready for an AFK agent  |
| `ready-for-human`          | `BLOCKED`             | Requires human implementation            |
| `wontfix`                  | `ARCHIVED`            | Will not be actioned                     |

When a skill mentions a role (e.g. "apply the AFK-ready triage label"), transition the ticket to the corresponding status:

```bash
exponential tickets update --id <ticket-cuid> --status READY_TO_PLAN
```

For `needs-info`, also add a comment carrying the actual clarifying question — the status alone doesn't tell the reporter what's missing:

```bash
exponential tickets comment add --id <ticket-cuid> -m "<the question>"
```

## Note on Exponential labels

Exponential does have a separate `labels` concept (`exponential labels --help`) — orthogonal tags applied to tickets, features, and epics. These are **not** used for triage. Use them for other cross-cutting concerns if you want; `/triage` will ignore them.

## Note on GitHub labels

`CLEAR-Initiative/clear-context-pipeline` carries GitHub's default label set (`bug`, `enhancement`, `wontfix`, …). Those are unused — GitHub Issues is not this repo's tracker. Don't map triage roles onto them.
