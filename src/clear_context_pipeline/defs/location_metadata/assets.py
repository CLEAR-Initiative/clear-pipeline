"""Dagster assets/jobs/schedules for the location-metadata ingests.

Graph (group ``location_metadata``)::

  location_pcode_index ─┬─> hapi_refugees ─────────┐
                        ├─> hapi_returnees          │
                        ├─> hapi_humanitarian_needs │
                        ├─> hapi_funding            ├─> (daily job)
                        ├─> hapi_food_security      │
                        ├─> hapi_food_prices        │
                        ├─> hapi_poverty_rate       │
                        ├─> hapi_operational_presence (ocha_3w) ┘
                        └─> iom_dtm_displacement ──────> (monthly job)

``location_pcode_index`` fetches the clear-api locations tree once (levels 0/1/2)
and builds the pcode→id maps every ingest reuses; each ingest asset then loops
``pipelineCountries``, fetches its source, resolves admin pcodes, and upserts one
``locationMetadata`` row per location via ``upsertLocationMetadataBatch``.
"""

import json
import os
import re
from typing import Any, Callable

import dagster as dg
from dagster import AssetExecutionContext

from clear_context_pipeline.providers import acaps, clear_api, hapi, iom_dtm

# ────────────────────────────────────────────────────────────────────
# pcode → location-id resolution (shared index)
# ────────────────────────────────────────────────────────────────────


def _normalise_name(name: str | None) -> str:
    """Lowercase + strip punctuation/prefixes for lenient name matching
    (e.g. "Republic of Sudan" vs "Sudan", "El Gezira" vs "Al Jazirah")."""
    s = (name or "").strip().lower()
    for prefix in ("republic of ", "the ", "el-", "el ", "al-", "al "):
        if s.startswith(prefix):
            s = s[len(prefix):]
            break
    return re.sub(r"[^a-z0-9]+", " ", s).strip()


@dg.asset(group_name="location_metadata")
def location_pcode_index(context: AssetExecutionContext) -> dict[str, Any]:
    """Build pcode→id / iso2→id / name→id maps per admin level from clear-api.

    One fetch of the whole locations tree (levels 0/1/2) that every ingest asset
    downstream reuses, instead of resolving pcodes one at a time. The iso2 map
    (level 0 only) absorbs the ISO2/ISO3 drift between sources — clear-api stores
    Sudan as "SD" while HAPI/DTM return "SDN".
    """
    pcode_to_id: dict[int, dict[str, str]] = {}
    name_to_id: dict[int, dict[str, str]] = {}
    iso2_to_id: dict[str, str] = {}
    counts: dict[str, int] = {}

    for level in (0, 1, 2):
        rows = clear_api.get_locations_by_level(level)
        counts[f"level_{level}"] = len(rows)
        p_map: dict[str, str] = {}
        n_map: dict[str, str] = {}
        for loc in rows:
            pcode = loc.get("pCode")
            if pcode:
                p_map[pcode] = loc["id"]
                if level == 0:
                    iso2_to_id[pcode[:2].upper()] = loc["id"]
            if loc.get("name"):
                n_map[_normalise_name(loc["name"])] = loc["id"]
        pcode_to_id[level] = p_map
        name_to_id[level] = n_map

    context.add_output_metadata({f"locations_{k}": dg.MetadataValue.int(v) for k, v in counts.items()})
    # JSON-string the int-keyed dicts so the default (pickle) IO manager and any
    # JSON metadata stay happy; consumers go through _resolve() below.
    return {
        "pcode_to_id": {str(k): v for k, v in pcode_to_id.items()},
        "name_to_id": {str(k): v for k, v in name_to_id.items()},
        "iso2_to_id": iso2_to_id,
    }


def _resolve(index: dict[str, Any], pcode: str, level: int, name: str | None) -> str | None:
    """Resolve one admin pcode to a clear-api location id: exact pcode, then
    (level 0 only) ISO2-prefix, then normalised name."""
    loc_id = index["pcode_to_id"].get(str(level), {}).get(pcode)
    if loc_id:
        return loc_id
    if level == 0 and pcode:
        loc_id = index["iso2_to_id"].get(pcode[:2].upper())
        if loc_id:
            return loc_id
    if name:
        loc_id = index["name_to_id"].get(str(level), {}).get(_normalise_name(name))
    return loc_id


def _to_batch(
    blobs: dict[str, dict],
    *,
    type_: str,
    level: int,
    index: dict[str, Any],
) -> tuple[list[dict], int]:
    """Turn ``{pcode: blob}`` into an upsert batch, resolving each pcode to a
    location id. Returns ``(batch, unmatched_count)``."""
    batch: list[dict] = []
    unmatched = 0
    for pcode, blob in blobs.items():
        loc_id = _resolve(index, pcode, level, blob.get("admin_name"))
        if not loc_id:
            unmatched += 1
            continue
        batch.append({"locationId": loc_id, "type": type_, "data": blob})
    return batch, unmatched


# ────────────────────────────────────────────────────────────────────
# HAPI endpoint assets (generic) + OCHA 3W (bespoke)
# ────────────────────────────────────────────────────────────────────


def _run_hapi_endpoint(
    context: AssetExecutionContext,
    spec: hapi.EndpointSpec,
    index: dict[str, Any],
) -> dict[str, Any]:
    countries = clear_api.get_pipeline_countries()
    per_country: dict[str, Any] = {}
    total_upserted = 0
    for country in countries:
        iso3 = country.get("iso3")
        if not iso3:
            continue
        rows = hapi.fetch(spec.path, location_code=iso3, location_filter=spec.location_filter)
        blobs = hapi.build_blobs(rows, spec)
        batch, unmatched = _to_batch(blobs, type_=spec.type_, level=spec.key_level, index=index)
        written = clear_api.upsert_location_metadata_batch(batch)
        total_upserted += len(written)
        per_country[iso3] = {
            "fetched_rows": len(rows),
            "locations": len(blobs),
            "matched": len(batch),
            "unmatched_pcode": unmatched,
            "upserted": len(written),
        }
    context.add_output_metadata({
        "type": dg.MetadataValue.text(spec.type_),
        "total_upserted": dg.MetadataValue.int(total_upserted),
        **{iso3: dg.MetadataValue.json(v) for iso3, v in per_country.items()},
    })
    return {"type": spec.type_, "total_upserted": total_upserted, "countries": per_country}


def _make_hapi_asset(spec: hapi.EndpointSpec):
    @dg.asset(name=spec.type_, group_name="location_metadata")
    def _asset(
        context: AssetExecutionContext, location_pcode_index: dict[str, Any],
    ) -> dict[str, Any]:
        return _run_hapi_endpoint(context, spec, location_pcode_index)

    _asset.__doc__ = (
        f"Ingest HAPI ``{spec.path}`` → locationMetadata type ``{spec.type_}`` "
        f"(admin{spec.key_level}) for every pipeline country."
    )
    return _asset


# One module-level asset per generic endpoint (names come from each spec.type_).
hapi_refugees = _make_hapi_asset(hapi.HAPI_ENDPOINTS[0])
hapi_returnees = _make_hapi_asset(hapi.HAPI_ENDPOINTS[1])
hapi_humanitarian_needs = _make_hapi_asset(hapi.HAPI_ENDPOINTS[2])
hapi_funding = _make_hapi_asset(hapi.HAPI_ENDPOINTS[3])
hapi_food_security = _make_hapi_asset(hapi.HAPI_ENDPOINTS[4])
hapi_food_prices = _make_hapi_asset(hapi.HAPI_ENDPOINTS[5])
hapi_poverty_rate = _make_hapi_asset(hapi.HAPI_ENDPOINTS[6])

_HAPI_ASSETS = [
    hapi_refugees, hapi_returnees, hapi_humanitarian_needs, hapi_funding,
    hapi_food_security, hapi_food_prices, hapi_poverty_rate,
]


@dg.asset(name=hapi.OCHA_3W_TYPE, group_name="location_metadata")
def hapi_operational_presence(
    context: AssetExecutionContext, location_pcode_index: dict[str, Any],
) -> dict[str, Any]:
    """Ingest HAPI operational-presence → ``ocha_3w`` (admin2) — the OCHA 3W
    "who does what where" roll-up, per pipeline country. Same bespoke sector/org
    blob the clear-api ingest-sudan-3w.ts script produced."""
    countries = clear_api.get_pipeline_countries()
    per_country: dict[str, Any] = {}
    total_upserted = 0
    for country in countries:
        iso3 = country.get("iso3")
        if not iso3:
            continue
        rows = hapi.fetch(hapi.OPERATIONAL_PRESENCE_PATH, location_code=iso3)
        blobs = hapi.build_operational_presence_blobs(rows)
        batch, unmatched = _to_batch(
            blobs, type_=hapi.OCHA_3W_TYPE, level=2, index=location_pcode_index,
        )
        written = clear_api.upsert_location_metadata_batch(batch)
        total_upserted += len(written)
        per_country[iso3] = {
            "fetched_rows": len(rows),
            "localities": len(blobs),
            "matched": len(batch),
            "unmatched_pcode": unmatched,
            "upserted": len(written),
        }
    context.add_output_metadata({
        "type": dg.MetadataValue.text(hapi.OCHA_3W_TYPE),
        "total_upserted": dg.MetadataValue.int(total_upserted),
        **{iso3: dg.MetadataValue.json(v) for iso3, v in per_country.items()},
    })
    return {"type": hapi.OCHA_3W_TYPE, "total_upserted": total_upserted, "countries": per_country}


# ────────────────────────────────────────────────────────────────────
# IOM DTM displacement asset (monthly)
# ────────────────────────────────────────────────────────────────────

_DTM_TYPE = "iom_dtm_displacement"
_DTM_LEVEL_FETCH: dict[int, Callable[..., list[dict]]] = {
    0: iom_dtm.fetch_admin0_displacement,
    1: iom_dtm.fetch_admin1_displacement,
    2: iom_dtm.fetch_admin2_displacement,
}


def _parse_assessment(raw: str) -> str | list[str] | None:
    """"" → None (no filter); "BA" → "BA"; "BA,FM" → ["BA","FM"] (priority)."""
    parts = [t.strip() for t in raw.split(",") if t.strip()]
    if not parts:
        return None
    return parts[0] if len(parts) == 1 else parts


@dg.asset(name=_DTM_TYPE, group_name="location_metadata")
def iom_dtm_displacement(
    context: AssetExecutionContext, location_pcode_index: dict[str, Any],
) -> dict[str, Any]:
    """Ingest IOM DTM displacement (admin 0/1/2) → ``iom_dtm_displacement`` for
    every pipeline country. Sums IDPs per destination across origins/reasons,
    latest round wins, filtered to one assessmentType to avoid BA/FM
    double-counting. HAPI does not cover IDPs, so this is the sole source for
    ``population_displaced``."""
    if not os.environ.get("IOM_DTM_SUBSCRIPTION_KEY"):
        raise dg.Failure(
            description=(
                "IOM_DTM_SUBSCRIPTION_KEY is not set — cannot call the IOM DTM "
                "API. Get one from https://dtm-apim.developer.iom.int/ and add "
                "it to the environment."
            ),
        )

    assessment = _parse_assessment(os.environ.get("IOM_DTM_ASSESSMENT_TYPE", "BA,FM"))
    # Per-ISO3 DTM Operation overrides, e.g. {"SDN": "Armed Clashes in Sudan (Overview)"}.
    # Unlisted countries fetch across all operations (operation=None).
    try:
        operations: dict[str, str] = json.loads(os.environ.get("IOM_DTM_OPERATIONS", "{}"))
    except json.JSONDecodeError:
        context.log.warning("IOM_DTM_OPERATIONS is not valid JSON — ignoring, using all operations")
        operations = {}

    countries = clear_api.get_pipeline_countries()
    per_country: dict[str, Any] = {}
    total_upserted = 0
    for country in countries:
        iso3 = country.get("iso3")
        name = country.get("name")
        if not iso3:
            continue
        operation = operations.get(iso3) or None
        per_level: dict[str, Any] = {}
        for level, fetch in _DTM_LEVEL_FETCH.items():
            records = fetch(
                country_name=name, admin0_pcode=iso3, operation=operation, from_round=None,
            )
            latest = iom_dtm.aggregate_displacement_by_destination(
                records, admin_level=level, assessment_type_filter=assessment,
            )
            batch: list[dict] = []
            unmatched = skipped = 0
            for pcode, agg in latest.items():
                if agg["population_displaced"] <= 0:
                    skipped += 1
                    continue
                loc_id = _resolve(location_pcode_index, pcode, level, agg["admin_name"])
                if not loc_id:
                    unmatched += 1
                    continue
                batch.append({
                    "locationId": loc_id,
                    "type": _DTM_TYPE,
                    "data": {
                        "population_displaced": agg["population_displaced"],
                        "origin_breakdown": agg["origin_breakdown"],
                        "round_number": agg["round_number"],
                        "reporting_date": agg["reporting_date"],
                        "operation": agg["operation"],
                        "admin_level": level,
                        "admin_name": agg["admin_name"],
                        "admin_pcode": pcode,
                        "assessment_type": agg["assessment_type"],
                        "source": "iom_dtm_v3",
                    },
                })
            written = clear_api.upsert_location_metadata_batch(batch)
            total_upserted += len(written)
            per_level[f"admin{level}"] = {
                "fetched_rows": len(records),
                "destinations": len(latest),
                "matched": len(batch),
                "unmatched_pcode": unmatched,
                "skipped_no_value": skipped,
                "upserted": len(written),
            }
        per_country[iso3] = per_level

    context.add_output_metadata({
        "type": dg.MetadataValue.text(_DTM_TYPE),
        "total_upserted": dg.MetadataValue.int(total_upserted),
        **{iso3: dg.MetadataValue.json(v) for iso3, v in per_country.items()},
    })
    return {"type": _DTM_TYPE, "total_upserted": total_upserted, "countries": per_country}


# ────────────────────────────────────────────────────────────────────
# ACAPS seasonal events calendar asset (monthly)
# ────────────────────────────────────────────────────────────────────

_ACAPS_TYPE = "acaps_seasonal_calendar"


def _acaps_blob(*, iso3: str, admin_level: int, admin_name: str | None,
                scope: str, events: list[dict]) -> dict:
    return {
        "source": "acaps_seasonal_calendar",
        "endpoint": acaps.SEASONAL_CALENDAR_URL,
        "iso": iso3,
        "admin_level": admin_level,
        "admin_name": admin_name,
        "scope": scope,
        "event_count": len(events),
        "events": events,
    }


@dg.asset(name=_ACAPS_TYPE, group_name="location_metadata")
def acaps_seasonal_calendar(
    context: AssetExecutionContext, location_pcode_index: dict,
) -> dict:
    """Ingest the ACAPS Seasonal Events Calendar → ``acaps_seasonal_calendar``.

    Recurring/expected events (lean/rainy seasons, harvests, outbreak seasons)
    per country. Country-wide entries land on the admin0 location; subnational
    entries land on each named admin1 (matched by name — ACAPS ``adm1`` is GADM,
    not COD). Background context for situation analysis, not observed events."""
    if not os.environ.get("ACAPS_API_KEY"):
        raise dg.Failure(
            description=(
                "ACAPS_API_KEY is not set — cannot call the ACAPS API. Use the "
                "same key CLEAR holds for INFORM Severity / Protection Risks."
            ),
        )

    entries = acaps.fetch_seasonal_calendar()
    countries = clear_api.get_pipeline_countries()
    per_country: dict[str, Any] = {}
    total_upserted = 0
    for country in countries:
        iso3 = country.get("iso3")
        name = country.get("name")
        if not iso3:
            continue
        split = acaps.build_blobs(entries, iso3)
        batch: list[dict] = []
        unmatched = 0

        if split["country"]:
            loc_id = _resolve(location_pcode_index, iso3, 0, name)
            if loc_id:
                batch.append({
                    "locationId": loc_id, "type": _ACAPS_TYPE,
                    "data": _acaps_blob(iso3=iso3, admin_level=0, admin_name=name,
                                        scope="country", events=split["country"]),
                })
            else:
                unmatched += 1

        for adm1_name, events in split["admin1"].items():
            loc_id = _resolve(location_pcode_index, "", 1, adm1_name)
            if loc_id:
                batch.append({
                    "locationId": loc_id, "type": _ACAPS_TYPE,
                    "data": _acaps_blob(iso3=iso3, admin_level=1, admin_name=adm1_name,
                                        scope="admin1", events=events),
                })
            else:
                unmatched += 1

        written = clear_api.upsert_location_metadata_batch(batch)
        total_upserted += len(written)
        per_country[iso3] = {
            "country_wide_events": len(split["country"]),
            "admin1_locations": len(split["admin1"]),
            "matched": len(batch),
            "unmatched": unmatched,
            "skipped_no_adm1_name": split["skipped_no_adm1_name"],
            "upserted": len(written),
        }

    context.add_output_metadata({
        "type": dg.MetadataValue.text(_ACAPS_TYPE),
        "entries_fetched": dg.MetadataValue.int(len(entries)),
        "total_upserted": dg.MetadataValue.int(total_upserted),
        **{iso3: dg.MetadataValue.json(v) for iso3, v in per_country.items()},
    })
    return {"type": _ACAPS_TYPE, "total_upserted": total_upserted, "countries": per_country}


# ────────────────────────────────────────────────────────────────────
# Jobs + schedules
# ────────────────────────────────────────────────────────────────────

# The daily job's 8 assets all hit the SAME HAPI rate limit, so running them
# fully parallel triggers 429s. Cap how many run at once (the HTTP clients also
# retry 429s with backoff, so this is burst-reduction, not the sole guard).
# Tune with LOCATION_METADATA_MAX_CONCURRENT (default 2). The dependency on
# location_pcode_index already serialises the first hop.
_MAX_CONCURRENT = int(os.environ.get("LOCATION_METADATA_MAX_CONCURRENT", "2"))
_LIMITED_EXECUTION = {
    "execution": {"config": {"multiprocess": {"max_concurrent": _MAX_CONCURRENT}}},
}

location_metadata_daily_job = dg.define_asset_job(
    name="location_metadata_daily",
    selection=[location_pcode_index, *_HAPI_ASSETS, hapi_operational_presence],
    config=_LIMITED_EXECUTION,
)

location_metadata_monthly_job = dg.define_asset_job(
    name="location_metadata_monthly",
    selection=[location_pcode_index, iom_dtm_displacement, acaps_seasonal_calendar],
    config=_LIMITED_EXECUTION,
)

# HAPI updates land throughout the day; 04:00 UTC catches the prior day's
# refresh across UNHCR/OCHA/IPC/WFP/OPHI feeds. IOM DTM rounds are monthly-ish,
# so the DTM job runs on the 1st. Crons live on the schedules (not the assets)
# so an ad-hoc run can target either job by name.
location_metadata_daily_schedule = dg.ScheduleDefinition(
    name="location_metadata_daily_schedule",
    job=location_metadata_daily_job,
    cron_schedule="0 4 * * *",
    execution_timezone="UTC",
)

location_metadata_monthly_schedule = dg.ScheduleDefinition(
    name="location_metadata_monthly_schedule",
    job=location_metadata_monthly_job,
    cron_schedule="0 3 1 * *",
    execution_timezone="UTC",
)
