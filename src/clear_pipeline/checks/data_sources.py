"""Fail the deploy when a registered source has no clear-api ``data_sources`` row.

Every source the pipeline ingests resolves its clear-api ``DataSource`` id by
*name* (``providers/clear_api.py``'s ``get_source_id_by_name``). The row is
created by a clear-api migration, in a different repo on a different deploy
cadence, so the two can disagree — and when they do, the pipeline finds out
only when that source's job next runs:

    RuntimeError: Data source 'idmc' not found in CLEAR API.

That is how both the 2026-10-20 ``idmc`` and the 2026-11-13
``iom_dtm_flash_alerts`` incidents were found: by someone noticing the signals
had stopped, days later. A scheduled source fails inside its own run, green
code location and all, so nothing about the deploy looks wrong.

This check asks the question at deploy time instead, when a human is watching
and a fix is cheap: it reads the source registries out of the code being
shipped, reads ``dataSources`` out of the environment that code is about to
run against, and reports **every** name the environment is missing.

Where it runs, and where it deliberately does not:

- ``release-dev.yml`` / ``release-prod.yml``, before the image is built. A
  failure means no new image, so the running pipeline is untouched: the
  environment keeps serving the last good image while the rows are added.
- **Not** at code-location load. ``CONTRIBUTING.md`` ("a missing credential
  must not stop the definitions loading") is the rule, and the arithmetic
  backs it: raising on import would turn "one source cannot ingest" into
  "no source can ingest, and the Dagster UI is down" — strictly worse than
  the incident it is meant to prevent, and it would fire on a clear-api blip
  as readily as on a real missing row.

Run it by hand against any environment before you merge::

    CLEAR_API_URL=https://dev-api.clearinitiative.io/graphql \
    CLEAR_API_KEY=… uv run --frozen python -m clear_pipeline.checks.data_sources
"""

from __future__ import annotations

import sys
from collections.abc import Callable, Iterable

#: Exit codes. Separate, because the two failures want different responses: a
#: missing row needs a clear-api migration, an unreachable API needs a retry.
EXIT_OK = 0
EXIT_MISSING = 1
EXIT_UNREACHABLE = 2


def required_source_names() -> frozenset[str]:
    """Every source name *this build* resolves to a clear-api id, from the
    registries themselves.

    Taken from ``CONNECTORS`` (``defs/signals/connectors.py``) and
    ``GX_SOURCES`` (``defs/gx_pipeline/sources.py``) — the two lists whose
    docstrings both say "add a data source = add one here" — filtered to the
    sources that declare ``api_source_id``, i.e. the ones that call
    ``get_source_id_by_name`` and so cannot ingest without the row. A source
    added the documented way is checked without anyone remembering a list in
    this file; ``tests/test_data_sources_check.py`` pins the resulting set, so
    adding one fails that test with the new name in the diff — the reminder
    that it needs a clear-api migration too.

    What this leaves out, on purpose: the analyst-facing names
    (``manual``, ``field_officer``, ``partner``, ``government``) and
    ``sudan-war-x``. The pipeline never resolves those — clear-api creates
    those signals, from an analyst or from ``POST /api/x/ingest`` — so a
    missing row there breaks a clear-api mutation, not a deploy of this
    repo, and clear-api owns both the check and the migration. Measured
    2026-11-21: dev has no ``manual`` row at all, and clear-api's
    ``createManualSignal`` does not accept ``manual`` either
    (``TRUSTED_SOURCE_NAMES`` is ``field_officer``/``partner``/
    ``government``), so requiring it here would have blocked every dev
    deploy over a row nothing wants. A gate with a false positive is a gate
    someone turns off.

    The check stays one-directional either way: extra rows in an environment
    (``whatsapp``, and the hundreds of report-attribution names) are fine.

    Imported lazily: this module is a CI entry point, and the registries pull
    in every provider.
    """
    from clear_pipeline.defs.gx_pipeline.sources import GX_SOURCES
    from clear_pipeline.defs.signals.connectors import CONNECTORS

    return frozenset(
        src.source for src in (*CONNECTORS, *GX_SOURCES) if hasattr(src, "api_source_id")
    )


def missing_source_names(present: Iterable[str], required: Iterable[str]) -> list[str]:
    """The required names absent from ``present``, sorted.

    Sorted and complete rather than first-failure: one deploy failure naming
    five sources beats five consecutive deploys each naming one.
    """
    return sorted(frozenset(required) - frozenset(present))


def _fetch_present_names() -> list[str]:
    from clear_pipeline.providers.clear_api import get_data_sources

    return [row["name"] for row in get_data_sources()]


def _api_url() -> str:
    from clear_pipeline.signals.config import settings

    return settings.clear_api_url


def main(
    argv: list[str] | None = None,
    *,
    fetch: Callable[[], list[str]] | None = None,
) -> int:
    """Print the verdict and return the exit code. ``fetch`` is for tests."""
    del argv  # no options: the environment is the whole input
    fetch = fetch or _fetch_present_names
    url = _api_url()
    required = required_source_names()

    try:
        present = fetch()
    except Exception as exc:  # noqa: BLE001 — any failure here is "cannot answer"
        print(
            f"Could not read dataSources from clear-api at {url}: "
            f"{type(exc).__name__}: {exc}\n"
            "The check could not run, so this is NOT a verdict on the rows. "
            "Confirm the URL and the API key, then re-run.",
            file=sys.stderr,
        )
        return EXIT_UNREACHABLE

    missing = missing_source_names(present, required)
    if not missing:
        print(f"All {len(required)} registered data sources exist in clear-api at {url}.")
        return EXIT_OK

    print(
        f"clear-api at {url} is missing {len(missing)} of "
        f"{len(required)} registered data_sources rows:\n"
        + "".join(f"  - {name}\n" for name in missing)
        + "\nEach of these is registered in CONNECTORS "
        "(src/clear_pipeline/defs/signals/connectors.py) or GX_SOURCES "
        "(src/clear_pipeline/defs/gx_pipeline/sources.py), so its ingest will "
        'fail at run time with "Data source \'<name>\' not found in CLEAR API" '
        "— silently, inside a scheduled run.\n"
        "Add the rows with a clear-api migration and deploy it first, then "
        "re-run this deploy.",
        file=sys.stderr,
    )
    return EXIT_MISSING


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
