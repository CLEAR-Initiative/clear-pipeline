"""Interval poll sensor with a dashboard-editable, cursor-stored cadence.

Replaces a cron ``ScheduleDefinition`` for source polling. The interval
**defaults from env** (code) but is **overridable live from the Dagster UI** via
the sensor's *Edit cursor* button — no redeploy:

    open the sensor → Edit cursor → {"interval_minutes": 5}

Clearing the cursor reverts to the code default. The sensor is evaluated by the
daemon every ``_TICK_SECONDS`` (the resolution of the interval check, NOT the
poll cadence) and launches the ingest job once the configured interval has
elapsed since the last launch — both values live in the cursor JSON
(``{"interval_minutes": N, "last_run": <epoch>}``).

No ``from __future__ import annotations`` here — Dagster inspects the sensor's
``context`` annotation, and stringifying it trips validation (same reason the
asset modules omit it).
"""

import json
import time

import dagster as dg

# How often the daemon evaluates the sensor. This bounds how promptly a due poll
# fires and how quickly a cursor edit takes effect — it is NOT the poll interval.
_TICK_SECONDS = 30


def build_poll_sensor(*, name: str, job, default_interval_minutes: int):
    """A sensor that launches ``job`` every ``interval_minutes`` (default from
    env, overridable via the sensor cursor in the Dagster UI). Ships STOPPED so
    the big-bang cutover enables it alongside the eager drain."""

    @dg.sensor(
        name=name,
        job=job,
        minimum_interval_seconds=_TICK_SECONDS,
        default_status=dg.DefaultSensorStatus.STOPPED,
    )
    def _poll_sensor(context: dg.SensorEvaluationContext):
        state = json.loads(context.cursor) if context.cursor else {}
        interval_minutes = int(state.get("interval_minutes", default_interval_minutes))
        last_run = float(state.get("last_run", 0.0))

        now = time.time()
        wait = interval_minutes * 60 - (now - last_run)
        if wait > 0:
            # Not due yet — record nothing (keep last_run) and report why. Must
            # be YIELDed, not returned: this fn is a generator, so a bare return
            # value is swallowed as StopIteration instead of surfacing the skip.
            yield dg.SkipReason(
                f"{int(wait)}s until next poll (interval={interval_minutes}m)"
            )
            return

        # Due: advance last_run (preserving the configured interval) and launch.
        context.update_cursor(
            json.dumps({"interval_minutes": interval_minutes, "last_run": now})
        )
        # run_key dedupes if two ticks land in the same second.
        yield dg.RunRequest(run_key=str(int(now)))

    return _poll_sensor
