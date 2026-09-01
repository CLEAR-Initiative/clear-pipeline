# IDMC records: in-place updates observed in the wild

**Branch:** `feat/idmc-integration`
**Context:** IDMC's IDU (Internal Displacement Updates) API has no `updated_at` —
entries can be revised in place upstream (same row `id`, changed `role`, `figure`,
dates, location, or classification), and `createSignal` in clear-api is
get-or-create, so a revision to an already-ingested signal is silently dropped
today. Before building the update-propagation path, checked empirically whether
IDMC actually does this in practice, and how reliably it can be detected.

## Method

Fetched the full live IDU feed and ran it through `idmc.py`'s own filter/split
logic (same country/type scope as production, Redis seen-set bypassed so
already-"seen" rows aren't hidden), then diffed the result against all 2,504
`idmc`-source signals currently stored in Postgres, matched by `external_id`.
This compares each signal's ingested `rawData` against what IDMC's API returns for
that same row right now — i.e. what changed in IDMC's own data since it was last
ingested.

## What changed

| category | count |
|---|---|
| byte-identical | 835 |
| coordinate float-jitter only (not a real change — see below) | 177 → 6 after a fix |
| substantive content change | 1,492 |
| in live feed, not yet ingested | 1 |
| in DB, no longer in live feed | 0 |

**IDMC does revise records in place.** All 1,492 substantive changes carry a
`type`/`subtype` classification backfill — `None` at ingest time to a populated
value fresh, e.g. `idmc:10457:0`: `None` → `"Non-International armed conflict
(NIAC)"`. This looks like IDMC filling in classification some time after a row's
initial publication, across the board.

One of them, **`idmc:252112`**, additionally carries a genuine `role` revision on
top of its type/subtype backfill: `"Recommended figure"` → `"Triangulation"` — the
same-`id`-changed-classification case a revision-propagation feature needs to
handle.

Two checks validated the comparison itself wasn't an artifact of how one raw IDU
row can fan out into multiple signals (`_split_by_location` assigns suffixes like
`idu_id:0`, `idu_id:1` by enumeration order, not a stable per-location key):
- Suffix identity (`locations_name`/`locations_type` under each suffix) checked
  across all 801 currently-stored multi-location signals (250 source rows) —
  0 drift since ingest.
- `locations_coordinates` (the field that actually drives origin/destination
  placement) — independently re-verified across all 1,953 base source rows:
  0/1,953 real content mismatches.

## The false positive: `latitude`/`longitude`/`centroid` noise

`_content_hash` hashes the full raw payload, which includes `latitude`/`longitude`/
`centroid` — fields IDMC's backend recomputes independently on every poll with
float noise around 1e-11 to 1e-14 degrees (sub-nanometer on the ground), e.g.
`idmc:25421`: `latitude` `13.537865537962455` → `13.537865537962457` between two
otherwise-identical fetches. Confirmed this is IDMC's own noise, not the pipeline's:
two live fetches back-to-back are byte-identical, and there's no float truncation
anywhere in the write path (Python or TypeScript). Without accounting for it, 177
of the 2,504 signals (7%) would have been spuriously flagged as "revised" on every
single poll.

Fix: round `latitude`/`longitude`/`centroid` to 6 decimals (~11cm — far finer than
IDU's own admin/settlement-level accuracy) before hashing; `rawData` itself is
never touched. Found and fixed a real bug in the fix along the way — `round()`
preserves the input's int/float type, so a value stored as `9` (int) vs fetched
fresh as `9.0` (float) is numerically equal but serializes differently (`"9"` vs
`"9.0"`), still flipping the hash; fixed by casting to `float()` first.

False-positive count dropped 177 → 29 → **6** after that fix. The residual 6 are
all splits of one source row (`idmc:30536`) sitting almost exactly on the
6-decimal rounding boundary — same noise magnitude as everywhere else, just
unlucky positioning. Truncation was tested as an alternative and rejected —
empirically worse (15 residual across 7 rows), since it fails whenever a value is
meant to be exactly at a clean decimal but carries a whisker of negative float
noise, which turned out to be a more common shape in this data than values sitting
near a half-way point. Kept rounding; the residual is an understood, accepted
limitation.
