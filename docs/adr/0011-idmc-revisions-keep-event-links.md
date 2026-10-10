---
status: proposed
---

# IDMC revisions keep their event links

## Context

IDMC revises IDU rows in place: same `idu_id`, so the same `externalId`. gx
pushes each revision with `updateSignalContent`, which bumps the signal's
revision and sets `NEEDS_RECOMPUTE`. The drain's `recompute_event` then
re-aggregates the event(s) the signal is **already** linked to. It never calls
`group_signal`, so a revision cannot move a signal to another event.

`group_signal` groups on two keys, which trace back to these IDU fields:

| Key | Signal field | IDU field |
|---|---|---|
| admin-2 district | point location clear-api resolves from `lat`/`lng` (PostGIS) | `latitude`, `longitude` |
| Jev `type_level_2` | `title`, `description` (and `severity` as a classifier input) | `event_name`, `standard_popup_text`, (`figure`) |

`event_id` is **not** a grouping key: it is the supersession key gx uses to
pick between role-tagged rows (`providers/idmc.py` `resolve_group`).
`displacement_type` only filters out record types IDMC isn't allowed to send.

If a revision moved the coordinates to another district, or rewrote the text so
Jev would classify it differently, the revised figures would be folded into an
event of the wrong district or type.

How IDMC revises: a corrected figure is issued as a **new** `idu_id`, which
enters the NEW lane and is grouped like any new signal; the superseded row
changes its `role` in place and is retracted by supersession. In-place revisions
also change `standard_popup_text`, so the type input does move.

Evidence so far: a 2-week observation saw only light in-place text changes.
Not yet established: counts, whether coordinates or `event_name` ever change in
place, behaviour over longer periods, and whether a light change can flip Jev's
type. **TODO — a gold snapshot diff over a longer window, and the type-drift
measurement (issue `idmc-text-revision-type-drift`).**

## Decision

**An in-place revision never moves a signal between events: links are sticky.**

Revised content propagates through recompute into the event the signal is
already linked to. Nothing is regrouped and nothing is blocked. A change of
grouping key is **detected**, not prevented: gx `_gold` compares each revised
row's new `(districtKey, eventType)` with the copy gold stored and reports the
difference. If a real crossing ever has to be repaired, it is repaired as
**retract + new signal**, never as a relink.

## Scope: small steps

This PR deliberately changes as little as it can: it keeps the existing link
behaviour and adds detection only. Relinking, blocking or repairing would each
touch grouping, supersession and clear-api at once, on a case not yet observed.
The approach is to measure first and improve step by step, each step in its own
change:

1. Detection on gx gold (this PR).
2. Evidence: per-field in-place change counts over a longer window, and Jev's
   type stability on identical vs revised text.
3. A check on the admin-2 clear-api resolves, in `recompute_event`.
4. Re-resolving the old supersession group when a row changes `event_id`.
5. Retract + new signal, only if steps 1–3 show real crossings.

## Why not relink on recompute

- **Wrong period.** `group_signal` matches events active *now*
  (`_get_active_events`, a 7-day window anchored on the current time), not at the
  signal's own time. A revised old signal would join whatever event is open
  today in that district and type.
- **Noisy trigger.** Deciding a crossing by re-deriving the key re-runs Jev (an
  LLM) and the geoparser. Their output can shift on unchanged input, so signals
  would hop between events when nothing changed.
- **Event identity.** Alerts, escalations and references follow event ids.
  Moving members rewrites events after the fact and can leave them empty; sent
  alerts cannot be recalled.
- **Cost.** About 3 LLM calls per regroup (classify + two rewrites) instead of
  0, drawn from the drain's `llm_budget`, which also pays for grouping new signals.
- **Missing infrastructure.** clear-api has no unlink mutation, and once a link
  is removed `pendingRecomputes` can no longer find the old event.

## Why not block the revision

Blocking crossing revisions at `_bronze`, before the blob write, was considered
and rejected:

- **Bronze sees only proxies.** The type is known only after `_classify`, and
  admin-2 only after clear-api resolves the point; raw coordinates also move
  within a district. Blocking on proxies blocks legitimate revisions.
- **No release.** IDU is re-scanned over 180 days on every poll, so a blocked
  row is found and blocked again on each run. A false positive freezes it.
- **Stuck supersession.** A revision that flips `role` and touches a gated field
  is blocked whole: the superseded figure stays live beside its replacement.
- **Blocking trades one wrong value for another.** A blocked row keeps its stale
  figure in its old event; a let-through row has the new figure in that event.

## Detection

The `grouping_key_drift` asset check on `<source>_gold`, for sources that
revise rows in place (`content_hash` hook, i.e. IDMC):

- Fails (severity **WARN**) when a revised row's `districtKey` or `eventType`
  differs from gold's stored value; metadata lists the rows as
  `{externalId: {field: [stored, new]}}`, and each is logged.
- Metrics on `_gold`: `signals_revised`, `district_drift`, `type_drift`,
  `drift_unresolved` (a side has no district or type, so it can't be compared).
- Costs no LLM call: gx already re-classifies every revised row.

## Consequences

- Figure and text corrections made in place propagate through recompute.
- A drifted row is reported, not repaired: it stays in its old event until
  someone acts. If the check fires on real crossings, build retract + new
  signal from those cases.
- `districtKey` is gx's heuristic from the geoparser's place name, not the
  admin-2 clear-api resolves; the two can disagree near borders. A check on the
  resolved admin-2 in `recompute_event` is the precise follow-up.
- `type_drift` includes classifier noise. Its rate is unknown until the drift
  measurement runs; until then a failed check is a lead, not a verdict.
- Nothing alerts on a failed asset check: the check has to be read.
- A revision that moves a row to another `event_id` leaves its old supersession
  group unresolved in `_reconcile` (tracked separately).
- Supersession only retracts a Triangulation row beside a Recommended figure, or
  a non-latest row in an all-Triangulation group. If a superseded row takes any
  other role, both figures stay live: confirm IDMC's role vocabulary.
