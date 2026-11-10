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

## What it does not do

- It does not re-group events. A moved signal stays in its event (ticket #1316).
- It does not touch a signal that a person corrected.
- It does not read sources that have no locator (Dataminr, ACLED, GDACS).

## Run it

Run it where the pipeline's settings are: inside a Dagster container. The plan must come from the same settings as the drain (model, gazetteer, LocationIQ). A plan made on a laptop without LocationIQ records names that the drain would place.

Run on dev first. A plan belongs to one clear-api: `apply` refuses a plan made against another host.

1. Open a shell in the container.

   ```bash
   ssh dev-dagster
   docker exec -it app-dagster-webserver-1 bash
   ```

2. Count the signals. This calls no model.

   ```bash
   python -m clear_pipeline.backfill.place_names plan --since 2026-08-01 --out /tmp/place-names.jsonl --count-only
   ```

3. Make the plan.

   ```bash
   python -m clear_pipeline.backfill.place_names plan --since 2026-08-01 --out /tmp/place-names.jsonl
   ```

4. Copy the plan out of the container. Watchtower replaces the container on each deploy, and `/tmp` goes with it. Run this on the VM, not in the container.

   ```bash
   docker cp app-dagster-webserver-1:/tmp/place-names.jsonl ~/place-names.jsonl
   ```

5. Read the summary. Change `--min-signals` to see how the threshold changes the queue.

   ```bash
   python -m clear_pipeline.backfill.place_names summary --plan /tmp/place-names.jsonl --min-signals 2
   ```

6. Apply the plan.

   ```bash
   python -m clear_pipeline.backfill.place_names apply --plan /tmp/place-names.jsonl --min-signals 2
   ```

7. Add a metric annotation on `/metrics`. The location precision series of past weeks changes.

Options:

- `apply --skip-moves` records the names and moves no signal.
- `plan --limit N` and `apply --limit N` take N signals at most.
- `plan --source NAME` takes another source. Repeat it for more sources.

## Stop and continue

Both `plan` and `apply` continue where they stopped. Run the same command again.

- `plan` skips the signals that the plan file holds. It tries a failed signal again.
- `apply` keeps a log beside the plan (`<plan>.applied`) and skips the signals in it.

A second `apply` of a whole plan changes nothing: a moved signal is no longer where the plan saw it, and clear-api counts a signal once per queue row.

## Cost

`plan` makes one location call per signal, and one or two small calls more when it geocodes a name. Dev held 2,192 such sudan-war-x signals on 10 November 2026 (September to November).

## Outcomes in the plan

| Outcome | Meaning |
|---|---|
| `move` | The locator gives a finer place inside the present location. |
| `same` | The locator gives the present location again. |
| `differs` | The locator gives a place outside the present location, or a coarser one. Nothing moves. |
| `no_location` | The locator gives nothing. |
| `skipped` | The signal changed after it was listed (`reason` says how). |
| `error` | The locate call failed. The next `plan` run tries it again. |
