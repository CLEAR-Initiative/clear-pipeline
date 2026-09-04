# clear-pipeline

A pipeline for extracting humanitarian datapoints from ReliefWeb.

## Agent skills

### Issue tracker

Issues and PRDs live in **Exponential** (workspace `clear`, product `clear`) via the `exponential` CLI — not GitHub Issues. No default feature; attach one per ticket. External PRs are not a triage surface (private repo). Requires `exponential-cli >= 1.6.0`. See `docs/agents/issue-tracker.md`.

### Triage labels

Exponential has no triage labels — the five canonical triage roles map to `ticket.status` (`BACKLOG`, `NEEDS_REFINEMENT`, `READY_TO_PLAN`, `BLOCKED`, `ARCHIVED`). See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` + `docs/adr/` at the root. Neither exists yet; skills should proceed silently without them. See `docs/agents/domain.md`.

### Git flow

Trunk-based on `dev` — `dev` is both the default branch and the deploy trigger. `master` is legacy and strictly behind; do not target it. See `docs/agents/git-flow.md`.

### Installed skills

Configured via `/setup-matt-pocock-skills` on 2026-07-15. All are symlinked into `~/.claude/skills`, so they're available in every repo on this machine — not just this one.

Source of truth is [`positonic/skills`](https://github.com/positonic/skills) (MIT), installed into `.agents/skills/` via `skills.sh` and pinned in `skills-lock.json`:

| | |
|---|---|
| Ticket lifecycle | `setup-git-flow`, `start-ticket`, `ship-ticket`, `setup-merge-hook`, `to-expo`, `to-prd` |
| Review | `review`, `pr-review`, `pr-fix-all` |
| Process | `triage`, `grill-with-docs`, `setup-matt-pocock-skills` |

Add more with `npx skills@latest add positonic/skills@<name> -y -p` — this keeps `skills-lock.json` correct. Don't copy skill directories by hand; the lock file is how vintages stay consistent.

Two skills come from **upstream [`mattpocock/skills`](https://github.com/mattpocock/skills)** instead, vendored as real directories in `~/.claude/skills` with `PROVENANCE.md`: `domain-modeling` (byte-identical to positonic's) and `grilling` (positonic's is an older, thinner variant). Prefer positonic for anything new.

**Not installed**, though available from `positonic/skills` if wanted: `improve-codebase-architecture` (needs `codebase-design`), `diagnosing-bugs`, `tdd`, `implement`, `to-tickets`, `to-issues`, and ~28 others.

`positonic/skills` is a fork of Matt Pocock's set with Exponential support added — upstream has none, and has since renamed `to-prd` to `to-spec`. Expect drift if you mix the two.

### Known tooling issues

- **`exponential tickets list --branch` / `--pr` are silently broken** in CLI 1.6.0 (verified 2026-07-15). Filters are ignored, so a nonexistent branch returns the whole product's tickets instead of none, with no error. Don't build automation on them. This affects `/setup-merge-hook`, whose GitHub Action promotes tickets by looking them up from the merged PR — if the Action calls the API directly rather than via the CLI it may be unaffected, but verify before trusting it.
- `exponential --version` misreports `1.0.0` regardless of the installed version. Use `npm ls -g exponential-cli`.
