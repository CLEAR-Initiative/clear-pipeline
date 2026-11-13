# Place-name backfill

Locate past signals again with the locator of today, and seed the unresolved place-name queue (clear-api ADR-0019) from them.

Code: [`src/clear_pipeline/backfill/place_names.py`](../src/clear_pipeline/backfill/place_names.py). Ticket: [#1317](https://www.exponential.im/w/clear/products/clear/tickets/1317).

## Why

The drain locates a signal once, when it arrives. A signal that arrived before the locator improved stays at its state or country. The names it could not place were never recorded. New signals fill the queue; old signals do not.

## Terms

| Term | Meaning |
|---|---|
| Plan | A file with one line per signal: where the signal is, where the locator puts it now, and the names it could not place. |
| Move | A change of a signal's location to a finer place inside its present location. |
| Unplaced name | A geocode miss (see `CONTEXT.md`). |
| Threshold | The number of signals that must name an unplaced name before it enters the queue. Default 2. |

## What it does

The command has three steps.

| Step | Reads | Writes | Model calls |
|---|---|---|---|
| `plan` | Signals and locations from clear-api | The plan file. The geocoder cache in clear-api, as the drain does. | About two per signal |
| `summary` | The plan file | Nothing | None |
| `apply` | The plan file, and each signal again | Signal locations, landmark locations, queue rows | None |

`plan` takes a signal when all of these are true:

- Its source is one of `--source` (default `sudan-war-x`).
- It was published in the date range.
- It is not retracted.
- Its location is a state or a country.
- It has no origin or destination location.
- No person corrected its location, and it has no open Location challenge.

`apply` moves a signal only when all of these are true:

- The planned place is finer than the present location and inside it.
- The signal is still at the location the plan saw.
- The checks of `plan` still pass.

When the locator now reads another state, the signal stays. The plan marks it `differs`. The backfill refines a location; it does not overrule one.

`apply` records an unplaced name with the date of the post. The queue then shows when the name was first and last seen.

clear-api applies a resolved name to each signal it records. When a reviewer resolved the name already, the signal moves to the alias location. This is the rule of the queue, in the drain too. `apply` cannot record a name and switch that rule off, so it has no "names only" option.

`apply` tells clear-api that each move is a replay. clear-api then adds a `REPLAY` row to its location Outcome log (clear-api ADR-0020). The row holds the pipeline version and the run id `place-names-backfill:<plan file name>`. The location precision chart on `/metrics` keeps its history: a move does not change the week or the level a signal had.

## What it does not do

- It does not re-group events. A moved signal stays in its event (ticket #1316).
- It does not touch a signal that a person corrected.
- It does not read sources that have no locator (Dataminr, ACLED, GDACS).

## Run it

Run it on the Dagster VM, in a container of its own. Two reasons:

- The plan must come from the same settings as the drain (model, gazetteer, LocationIQ). A plan made on a laptop without LocationIQ records names that the drain would place.
- Watchtower replaces the Dagster containers on each deploy. A run inside one of them (`docker exec`) stops, and its files in `/tmp` go with it. A container that you start yourself has no Watchtower label, so a deploy leaves it alone.

Run on dev first. A plan belongs to one clear-api: `apply` refuses a plan made against another host.

The commands below run on the VM (`ssh dev-dagster`). They keep the files in `~/backfill`.

1. Set the command once for this shell.

   ```bash
   mkdir -p ~/backfill
   run() { docker run --rm --network clear --env-file /srv/app/dagster.env -v "$HOME/backfill:/work" --memory 2g ghcr.io/clear-initiative/clear-pipeline:dev python -m clear_pipeline.backfill.place_names "$@"; }
   ```

2. Count the signals. This calls no model.

   ```bash
   run plan --since 2026-08-01 --out /work/count.jsonl --count-only
   ```

3. Make the plan. Put the date in the file name: the file name is the id of the replay run in clear-api, and each plan must be a run of its own. It takes about 25 minutes for 2,000 signals. Use `tmux`, or add `-d` to the `docker run`, so that a lost SSH connection does not stop it.

   ```bash
   run plan --since 2026-08-01 --out /work/plan-2026-11-11.jsonl
   ```

4. Read the summary. Change `--min-signals` to see how the threshold changes the queue.

   ```bash
   run summary --plan /work/plan-2026-11-11.jsonl --min-signals 2
   ```

5. Read the moves before you apply them. Look for a name that is a region, not a place ("Darfur", "Kordofan"): the locator gives it the region's centre point (ticket #1429). To hold a move back, change its `outcome` in the plan file from `move` to `held`.

   ```bash
   jq -r 'select(.outcome=="move") | .proposed.name' ~/backfill/plan-2026-11-11.jsonl | sort | uniq -c | sort -rn | less
   ```

6. Apply 5 signals, and check them in CLEAR.

   ```bash
   run apply --plan /work/plan-2026-11-11.jsonl --limit 5
   ```

7. Apply the rest.

   ```bash
   run apply --plan /work/plan-2026-11-11.jsonl
   ```

8. Look at the result on `/metrics`. Choose "At district or finer", then choose the run in "Replay run". The run id is `place-names-backfill:<plan file name>`. The dashed line shows what the backfill did with the signals it moved. The other lines do not change: past weeks keep what the pipeline delivered at the time. Do not add a metric annotation for the backfill.

Options:

- `plan --limit N` and `apply --limit N` take N signals at most.
- `plan --source NAME` takes another source. Repeat it for more sources.

## Stop and continue

Both `plan` and `apply` continue where they stopped. Run the same command again.

- `plan` skips the signals that the plan file holds. It tries a failed signal again.
- `apply` keeps a log beside the plan (`<plan>.applied`). The log holds each move it tried and each name it recorded. The next run does only what is left.
- A stop in the middle of a write leaves a last line that is cut short. The next run removes that line and continues.

A second `apply` of a whole plan changes nothing. A later `apply` with a lower `--min-signals` records only the names that are new.

## Cost

`plan` makes one location call per signal, and one or two small calls more when it geocodes a name. Dev held 2,192 such sudan-war-x signals on 10 November 2026 (September to November).

## Outcomes in the plan

| Outcome | Meaning |
|---|---|
| `move` | The locator gives a finer place inside the present location. |
| `held` | A person held the move back in the plan file. `apply` does not move the signal. |
| `same` | The locator gives the present location again. |
| `differs` | The locator gives a place outside the present location, or a coarser one. Nothing moves. |
| `no_location` | The locator gives nothing. |
| `skipped` | The signal changed after it was listed (`reason` says how). |
| `error` | The locate call failed. The next `plan` run tries it again. |
