"""Backfill: locate past signals again, and seed the unresolved place-name queue.

The drain locates a signal once, when it arrives. A signal that arrived before
the locator learned to read a place stays at its state or country, and the
names it could not place were never recorded. This command runs the locator of
today over those signals. It has three steps, each its own command:

    python -m clear_pipeline.backfill.place_names plan --since 2026-08-01 --out plan.jsonl
    python -m clear_pipeline.backfill.place_names summary --plan plan.jsonl
    python -m clear_pipeline.backfill.place_names apply --plan plan.jsonl

``plan`` reads the signals and locates each one. It writes one line per signal
to the plan file. It changes no signal, creates no location and records no
queue row. (The geocoder still fills clear-api's geocoder cache, as it does in
the drain.) It pays for the model calls: about two per signal.

``summary`` prints what the plan holds: how many signals would move, and which
names would enter the queue. It reads only the file.

``apply`` writes what the plan holds, and calls no model:

- It moves a signal to the planned location. It moves only a signal that is
  still where the plan saw it, that no person corrected, and that has no open
  Location challenge.
- It records each unplaced name in the queue, with the signal's own date. A
  name enters the queue only when ``--min-signals`` signals of the plan name
  it (default 2), so that one-off spellings do not fill the queue.

clear-api applies a resolved name to each signal it records, here as in the
drain: when a reviewer resolved the name already, the signal moves to the
alias location. ``apply`` cannot record a name and switch that rule off.

Which signals ``plan`` takes: those of ``--source`` (default sudan-war-x)
published in the date range, not retracted, located at a state or a country,
with no origin or destination location, and not corrected by a person.

A signal moves only to a place inside its present location and finer than it.
When the locator now reads another state, the signal stays (outcome
``differs``): the backfill refines a location, it does not overrule one.

A moved signal stays in its event. Events are not re-grouped here (ticket
#1316).

A plan belongs to one clear-api: location ids differ between dev and prod.
``apply`` refuses a plan made against another host. Both ``plan`` and ``apply``
can be stopped and run again: each continues after the last line it wrote.
``apply`` keeps a log beside the plan of each move it made and each name it
recorded, so a later run with a lower ``--min-signals`` records only the names
that are new.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import threading
import unicodedata
import uuid
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

from clear_pipeline.defs.signals.connectors import LocatingSource
from clear_pipeline.defs.signals.stages import _project
from clear_pipeline.providers import location
from clear_pipeline.providers.clear_api import (
    find_or_create_landmark_l4,
    get_locations,
    get_signal_for_relocate,
    record_unresolved_place_name,
    signals_in_location,
    update_signal_location,
)
from clear_pipeline.providers.location import collect_geocode_misses
from clear_pipeline.signals.config import settings

logger = logging.getLogger(__name__)

#: The id of a point that ``plan`` found and did not create.
PLANNED_POINT_PREFIX = "planned-point:"
#: A signal at this level or coarser is located again (0 country, 1 state).
MAX_LEVEL = 1
DEFAULT_MIN_SIGNALS = 2


# ── The country's locations ──────────────────────────────────────────────────

class LocationIndex:
    """Level, name and parent of every location, read once."""

    def __init__(self, locations: list[dict]):
        self._by_id = {
            loc["id"]: {
                "name": loc.get("name"),
                "level": loc.get("level"),
                "parent": (loc.get("parent") or {}).get("id"),
            }
            for loc in locations
            if loc.get("id")
        }

    def level(self, location_id: str | None) -> int | None:
        return (self._by_id.get(location_id or "") or {}).get("level")

    def name(self, location_id: str | None) -> str | None:
        return (self._by_id.get(location_id or "") or {}).get("name")

    def _chain(self, location_id: str | None):
        seen: set[str] = set()
        while location_id and location_id in self._by_id and location_id not in seen:
            seen.add(location_id)
            yield location_id
            location_id = self._by_id[location_id]["parent"]

    def inside(self, location_id: str | None, area_id: str | None) -> bool:
        """Whether ``location_id`` is ``area_id`` or lies inside it."""
        return bool(area_id) and area_id in self._chain(location_id)

    def state_of(self, location_id: str | None) -> str | None:
        """The state (level 1) that holds the location, or None for a country."""
        return next((i for i in self._chain(location_id) if self.level(i) == 1), None)


# ── Which signals ────────────────────────────────────────────────────────────

def parse_time(value: Any) -> datetime | None:
    """A clear-api date-time (ISO-8601, or epoch milliseconds) as aware UTC."""
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)) or (isinstance(value, str) and value.isdigit()):
        return datetime.fromtimestamp(int(value) / 1000, tz=timezone.utc)
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


def skip_reason(row: dict) -> str | None:
    """Why a signal must not be located again, or None. Read from the signal
    as it is now: ``plan`` asks before it pays for a model call, ``apply``
    asks again before it writes."""
    if row.get("retracted"):
        return "retracted"
    if row.get("locationCorrectedAt"):
        return "corrected_by_a_person"
    if row.get("locationChallenge"):
        return "open_location_challenge"
    if row.get("originLocation") or row.get("destinationLocation"):
        # `updateSignalLocation` writes the general location only, and the
        # origin stays the signal's primary location.
        return "has_origin_or_destination"
    general = row.get("generalLocation")
    if not general:
        return "no_location"
    if (general.get("level") or 0) > MAX_LEVEL:
        return "district_or_finer"
    return None


def choose_candidates(
    rows: list[dict], *, sources: set[str], since: datetime, until: datetime | None,
) -> tuple[list[dict], Counter]:
    """The signals to locate again, newest first, and why the others of the
    same sources and dates were left out."""
    chosen: list[dict] = []
    left_out: Counter = Counter()
    for row in rows:
        if ((row.get("source") or {}).get("name") or "") not in sources:
            continue
        published = parse_time(row.get("publishedAt"))
        if published is None or published < since or (until is not None and published >= until):
            continue
        reason = skip_reason(row)
        if reason:
            left_out[reason] += 1
        else:
            chosen.append(row)
    chosen.sort(key=lambda r: parse_time(r.get("publishedAt")) or since, reverse=True)
    return chosen, left_out


# ── plan: locate, and write nothing ──────────────────────────────────────────

@dataclass(frozen=True)
class _Hit:
    """One name that geocoded inside an area."""

    location_id: str
    area_id: str
    name: str


_geocode_hits: ContextVar[list[_Hit] | None] = ContextVar("backfill_geocode_hits", default=None)


@contextmanager
def planning():
    """Inside the block the locator creates no location: a point it would
    promote to a landmark is kept in the returned dict instead, under an id
    that starts with ``PLANNED_POINT_PREFIX``. The block also notes the area
    each geocoded name was found in. For the whole process, not one thread."""
    real_geocode = location._geocode_within
    real_promote = location.find_or_create_landmark_l4
    points: dict[str, dict] = {}

    def planned_point(*, name: str, lat: float, lng: float, kind: str, **_: Any) -> dict:
        point_id = f"{PLANNED_POINT_PREFIX}{uuid.uuid4().hex}"
        points[point_id] = {"name": name, "lat": lat, "lng": lng, "kind": kind}
        return {"locationId": point_id, "reused": False}

    def traced_geocode(place_name: str, area_id: str, country_code: str, *, text: str = ""):
        found = real_geocode(place_name, area_id, country_code, text=text)
        hits = _geocode_hits.get()
        if found and hits is not None:
            hits.append(_Hit(found, area_id, place_name))
        return found

    location._geocode_within = traced_geocode
    location.find_or_create_landmark_l4 = planned_point
    try:
        yield points
    finally:
        location._geocode_within = real_geocode
        location.find_or_create_landmark_l4 = real_promote


def decide(current: dict, located: str | None, hits: list[_Hit], points: dict, index: LocationIndex):
    """The outcome for one signal, and the location proposed for it.

    ``move``: the proposed location is finer than the present one and inside
    it. ``same``: the locator gives the present location again. ``differs``:
    it gives a place that is not inside the present location, or not finer.
    ``no_location``: it gives nothing."""
    if not located:
        return "no_location", None
    hit = next((h for h in hits if h.location_id == located), None)
    if hit is None:
        # The area the model chose, and no name geocoded inside it.
        proposed = {
            "kind": "area", "locationId": located,
            "name": index.name(located), "level": index.level(located),
        }
        inside, level = index.inside(located, current["id"]), index.level(located)
    elif located in points:
        proposed = {
            "kind": "point", "point": points[located], "name": hit.name, "level": 4,
            "areaId": hit.area_id, "areaName": index.name(hit.area_id),
        }
        # The point lies inside the area the model chose.
        inside, level = index.inside(hit.area_id, current["id"]), 4
    else:
        # A place alias: a CLEAR location a person mapped the spelling to.
        level = index.level(located)
        proposed = {
            "kind": "alias", "locationId": located, "name": index.name(located) or hit.name,
            "level": level, "areaId": hit.area_id, "areaName": index.name(hit.area_id),
        }
        inside = index.inside(hit.area_id, current["id"])
        level = 4 if level is None else level  # a landmark made after the index was read
    if located == current["id"]:
        return "same", proposed
    if inside and level is not None and level > (current.get("level") or 0):
        return "move", proposed
    return "differs", proposed


def plan_signal(signal_id: str, points: dict, index: LocationIndex) -> dict:
    """Locate one signal again and say what would change. Writes nothing."""
    row = get_signal_for_relocate(signal_id)
    if row is None:
        return {"type": "signal", "signalId": signal_id, "outcome": "skipped", "reason": "not_found"}
    entry: dict = {
        "type": "signal",
        "signalId": signal_id,
        "source": (row.get("source") or {}).get("name"),
        "publishedAt": row.get("publishedAt"),
        "current": {k: (row.get("generalLocation") or {}).get(k) for k in ("id", "name", "level")},
    }
    reason = skip_reason(row)
    if reason is None:
        projected = _project(row)
        if isinstance(projected, str):
            reason = projected
        elif not isinstance(projected[0], LocatingSource):
            reason = "source_has_no_locator"
    if reason:
        return {**entry, "outcome": "skipped", "reason": reason}
    connector, view = projected

    hits: list[_Hit] = []
    token = _geocode_hits.set(hits)
    try:
        with collect_geocode_misses() as misses:
            located = connector.locate(view)
    finally:
        _geocode_hits.reset(token)

    outcome, proposed = decide(entry["current"], located, hits, points, index)
    return {
        **entry,
        "outcome": outcome,
        "proposed": proposed,
        "misses": [
            {
                "name": m.name, "nameAr": m.name_ar, "countryCode": m.country_code,
                "areaId": m.area_id, "areaName": index.name(m.area_id),
                "stateId": index.state_of(m.area_id), "reason": m.reason,
            }
            for m in misses
        ],
    }


# ── The plan file ────────────────────────────────────────────────────────────

def api_host() -> str:
    return urlparse(os.environ.get("CLEAR_API_URL") or settings.clear_api_url).hostname or ""


def read_lines(path: Path, *, repair: bool = False) -> list[dict]:
    """The JSON lines of a file this command wrote.

    A stop in the middle of a write leaves a last line with no newline. That
    line is left out when it is cut short, and kept when it is whole. With
    ``repair`` the file is also put right, so the next line appended starts on
    a line of its own. A bad line anywhere else is damage: the command stops."""
    raw = path.read_bytes()
    whole, _, tail = raw.rpartition(b"\n")
    records: list[dict] = []
    for number, line in enumerate(whole.split(b"\n") if whole else [], 1):
        if not line.strip():
            continue
        try:
            records.append(json.loads(line))
        except ValueError:
            raise SystemExit(f"{path}, line {number}: not valid JSON. The file is damaged.") from None
    if tail.strip():
        try:
            records.append(json.loads(tail))
            cut_short = False
        except ValueError:
            cut_short = True
        if repair and cut_short:
            with path.open("r+b") as handle:
                handle.truncate(len(raw) - len(tail))
        elif repair:
            with path.open("ab") as handle:
                handle.write(b"\n")
    return records


def read_plan(path: Path, *, repair: bool = False) -> tuple[dict, dict[str, dict]]:
    """The header and the last line written for each signal."""
    header: dict = {}
    entries: dict[str, dict] = {}
    for item in read_lines(path, repair=repair):
        if item.get("type") == "header":
            header = item
        elif item.get("signalId"):
            entries[item["signalId"]] = item
    if not header:
        raise SystemExit(f"{path} has no header line: it is not a plan file")
    return header, entries


class _Lines:
    """Append JSON lines to a file, one at a time, from several threads."""

    def __init__(self, path: Path):
        self._path = path
        self._lock = threading.Lock()

    def write(self, item: dict) -> None:
        with self._lock, self._path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(item, ensure_ascii=False) + "\n")


def run_plan(args: argparse.Namespace) -> int:
    out = Path(args.out)
    since = parse_time(args.since)
    until = parse_time(args.until) if args.until else None
    if since is None or (args.until and until is None):
        raise SystemExit("--since and --until take a date: YYYY-MM-DD")
    sources = sorted(set(args.source or [settings.sudan_war_x_source_name]))
    header = {
        "type": "header", "host": api_host(), "country": args.country, "sources": sources,
        "since": since.isoformat(), "until": until.isoformat() if until else None,
    }

    done: set[str] = set()
    # An empty file is a run that stopped before its first line.
    continues = out.exists() and out.stat().st_size > 0
    if continues:
        old_header, entries = read_plan(out, repair=True)
        same = all(old_header.get(k) == header[k] for k in ("host", "country", "sources", "since", "until"))
        if not same:
            raise SystemExit(f"{out} is a plan for other signals or another host. Use another --out.")
        # A signal that failed is tried again.
        done = {i for i, e in entries.items() if e.get("outcome") != "error"}
    lines = _Lines(out)
    if not continues:
        lines.write({**header, "createdAt": datetime.now(timezone.utc).isoformat()})

    country_id = location.resolve_country_only_location(args.country)
    if not country_id:
        raise SystemExit(f"{args.country!r} is not a country in clear-api")
    print(f"Reading the signals in {args.country} from {header['host']} …", flush=True)
    rows = signals_in_location(country_id)
    candidates, left_out = choose_candidates(rows, sources=set(sources), since=since, until=until)
    todo = [row for row in candidates if row["id"] not in done]
    in_plan = len(candidates) - len(todo)
    if args.limit is not None:
        todo = todo[: args.limit]
    print(
        f"{len(rows)} signals read. {len(candidates)} to locate again "
        f"({in_plan} are in the plan already). This run locates {len(todo)}.",
    )
    for reason, count in left_out.most_common():
        print(f"  left out, {reason}: {count}")
    if args.count_only or not todo:
        return 0

    index = LocationIndex(get_locations())
    failed = 0
    with planning() as points, ThreadPoolExecutor(max_workers=args.workers) as pool:
        futures = {pool.submit(plan_signal, row["id"], points, index): row["id"] for row in todo}
        for number, future in enumerate(as_completed(futures), 1):
            signal_id = futures[future]
            try:
                lines.write(future.result())
            except Exception as exc:  # noqa: BLE001 — one signal; the run carries on
                failed += 1
                logger.warning("signal %s failed: %s", signal_id, exc)
                lines.write({"type": "signal", "signalId": signal_id, "outcome": "error", "error": str(exc)[:500]})
            if number % 50 == 0 or number == len(futures):
                print(f"  {number}/{len(futures)} located", flush=True)

    print_summary(*read_plan(out), min_signals=args.min_signals, top=args.top)
    if failed:
        print(f"\n{failed} signals failed. Run the same command again to try them again.")
    return 1 if failed else 0


# ── summary ──────────────────────────────────────────────────────────────────

def name_key(name: str) -> str:
    """A spelling as clear-api normalises it, near enough to count the signals
    that share a queue row: no accents, no case, no punctuation. clear-api
    decides the real row when the name is recorded."""
    plain = "".join(c for c in unicodedata.normalize("NFKD", name) if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9ء-ي ]", " ", plain.lower())).strip()


def queue_key(miss: dict) -> tuple[str, str, str | None]:
    return miss["countryCode"], name_key(miss["name"]), miss.get("stateId")


def signals_per_name(entries: dict[str, dict]) -> dict[tuple, set[str]]:
    """The signals of the plan that name each unplaced name."""
    named: dict[tuple, set[str]] = defaultdict(set)
    for entry in entries.values():
        for miss in entry.get("misses") or []:
            named[queue_key(miss)].add(entry["signalId"])
    return named


def print_summary(header: dict, entries: dict[str, dict], *, min_signals: int, top: int) -> None:
    outcomes = Counter(e.get("outcome") for e in entries.values())
    print(f"\nPlan for {header.get('host')}: {', '.join(header.get('sources') or [])}, "
          f"from {(header.get('since') or '')[:10]}"
          + (f" to {header['until'][:10]}" if header.get("until") else "")
          + f". {len(entries)} signals.")
    for outcome in ("move", "same", "differs", "no_location", "skipped", "error"):
        if outcomes.get(outcome):
            print(f"  {outcome}: {outcomes[outcome]}")

    moves = [e for e in entries.values() if e.get("outcome") == "move"]
    by_step = Counter(
        (e["current"].get("level"), (e.get("proposed") or {}).get("level")) for e in moves
    )
    for (frm, to), count in sorted(by_step.items(), key=lambda kv: (kv[0][0] or 0, kv[0][1] or 0)):
        print(f"    level {frm} → level {to}: {count}")

    named = signals_per_name(entries)
    kept = {k: v for k, v in named.items() if len(v) >= min_signals}
    labels: dict[tuple, dict] = {}
    for entry in entries.values():
        for miss in entry.get("misses") or []:
            labels.setdefault(queue_key(miss), miss)
    print(f"\nUnplaced names: {len(named)}, named by {len({s for v in named.values() for s in v})} signals.")
    print(f"  named by {min_signals} signals or more: {len(kept)} names, "
          f"{len({s for v in kept.values() for s in v})} signals. These enter the queue.")
    print(f"  named by fewer: {len(named) - len(kept)} names. These do not.")
    for key, signal_ids in sorted(kept.items(), key=lambda kv: -len(kv[1]))[:top]:
        miss = labels[key]
        arabic = f" ({miss['nameAr']})" if miss.get("nameAr") else ""
        print(f"  {len(signal_ids):>4}  {miss['name']}{arabic}, {miss.get('areaName') or '?'}  [{miss['reason']}]")


def run_summary(args: argparse.Namespace) -> int:
    print_summary(*read_plan(Path(args.plan)), min_signals=args.min_signals, top=args.top)
    return 0


# ── apply: write the plan ────────────────────────────────────────────────────

def apply_move(entry: dict) -> str:
    """Move one signal to its planned location. Returns ``moved``, or why not."""
    row = get_signal_for_relocate(entry["signalId"])
    if row is None:
        return "not_found"
    reason = skip_reason(row)
    if reason:
        return reason
    if row["generalLocation"]["id"] != entry["current"]["id"]:
        return "location_changed_since_the_plan"
    proposed = entry["proposed"]
    if proposed["kind"] == "point":
        promo = find_or_create_landmark_l4(**proposed["point"])
        location_id = None if promo.get("abortedReason") else promo.get("locationId")
        if not location_id:
            return "point_gave_no_location"
    else:
        location_id = proposed["locationId"]
    update_signal_location(entry["signalId"], location_id)
    return "moved"


def apply_entry(entry: dict, *, move: bool, misses: list[dict]) -> dict:
    """Write one signal: its move when ``move``, then each of ``misses``.
    The result is the line for the log: what was done, so that the next run
    does not do it again."""
    result: dict = {"type": "applied", "signalId": entry["signalId"]}
    if move:
        result["move"] = apply_move(entry)
    recorded = []
    for miss in misses:
        row = record_unresolved_place_name(
            signal_id=entry["signalId"],
            name=miss["name"],
            name_ar=miss.get("nameAr"),
            country_code=miss["countryCode"],
            area_location_id=miss["areaId"],
            reason=miss["reason"],
            # The date of the post: the queue shows when a name was first and last seen.
            seen_at=(parse_time(entry.get("publishedAt")) or datetime.now(timezone.utc)).isoformat(),
        )
        if row is None:
            raise SystemExit("clear-api has no recordUnresolvedPlaceName. Deploy clear-api #1307 first.")
        recorded.append({"name": miss["name"], "key": list(queue_key(miss)), "status": row.get("status")})
    if recorded:
        result["recorded"] = recorded
    return result


def read_applied(log_path: Path) -> tuple[set[str], set[tuple[str, tuple]]]:
    """What earlier runs did: the signals whose move was tried, and the names
    recorded for each signal."""
    moved: set[str] = set()
    recorded: set[tuple[str, tuple]] = set()
    if log_path.exists():
        for line in read_lines(log_path, repair=True):
            if "move" in line:
                moved.add(line["signalId"])
            for item in line.get("recorded") or []:
                recorded.add((line["signalId"], tuple(item["key"])))
    return moved, recorded


def run_apply(args: argparse.Namespace) -> int:
    plan = Path(args.plan)
    header, entries = read_plan(plan)
    if header.get("host") != api_host():
        raise SystemExit(
            f"This plan was made against {header.get('host')}, and CLEAR_API_URL is {api_host()}. "
            "Location ids differ between hosts: make a plan against this host."
        )
    log_path = plan.with_suffix(plan.suffix + ".applied")
    moved, recorded = read_applied(log_path)
    log = _Lines(log_path)

    # What is left for each signal: its move, and the names that pass the
    # threshold and are not recorded yet. A name counts once per signal.
    named = signals_per_name(entries)
    work: list[tuple[dict, bool, list[dict]]] = []
    for entry in entries.values():
        move = entry.get("outcome") == "move" and entry["signalId"] not in moved
        misses = list({
            queue_key(m): m
            for m in entry.get("misses") or []
            if len(named[queue_key(m)]) >= args.min_signals
            and (entry["signalId"], queue_key(m)) not in recorded
        }.values())
        if move or misses:
            work.append((entry, move, misses))
    if args.limit is not None:
        work = work[: args.limit]
    print(
        f"Applying {len(work)} signals to {header['host']} "
        f"({len(moved)} moves and {len(recorded)} names done before).",
        flush=True,
    )

    moves: Counter = Counter()
    statuses: Counter = Counter()
    failed = 0
    for number, (entry, move, misses) in enumerate(work, 1):
        try:
            result = apply_entry(entry, move=move, misses=misses)
        except Exception as exc:  # noqa: BLE001 — one signal; run again to try it again
            failed += 1
            logger.warning("signal %s failed: %s", entry["signalId"], exc)
            continue
        log.write(result)
        if "move" in result:
            moves[result["move"]] += 1
        statuses.update(r["status"] for r in result.get("recorded") or [])
        if number % 100 == 0 or number == len(work):
            print(f"  {number}/{len(work)} applied", flush=True)

    print("\nSignals:")
    for outcome, count in moves.most_common():
        print(f"  {outcome}: {count}")
    print(f"Names recorded: {sum(statuses.values())}")
    for status, count in statuses.most_common():
        print(f"  on a row that is {status}: {count}")
    if statuses.get("RESOLVED"):
        print("  clear-api applies a resolved name to the signal: it may have moved to the alias location.")
    if failed:
        print(f"\n{failed} signals failed. Run the same command again to try them again.")
    return 1 if failed else 0


# ── Command line ─────────────────────────────────────────────────────────────

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m clear_pipeline.backfill.place_names",
        description="Locate past signals again, and seed the unresolved place-name queue.",
    )
    parser.add_argument("-v", "--verbose", action="store_true", help="show the locator's log")
    commands = parser.add_subparsers(dest="command", required=True)

    def add_summary_options(command: argparse.ArgumentParser) -> None:
        command.add_argument("--min-signals", type=int, default=DEFAULT_MIN_SIGNALS,
                             help="a name enters the queue when this many signals name it (default 2)")
        command.add_argument("--top", type=int, default=40, help="how many names to print (default 40)")

    plan = commands.add_parser("plan", help="locate the signals and write the plan file; changes nothing")
    plan.add_argument("--since", required=True, help="first day, YYYY-MM-DD (UTC)")
    plan.add_argument("--until", help="the day after the last day, YYYY-MM-DD (UTC)")
    plan.add_argument("--source", action="append",
                      help=f"signal source; repeat for more (default {settings.sudan_war_x_source_name})")
    plan.add_argument("--country", default=settings.sudan_war_x_default_country,
                      help="the country that holds the signals (default Sudan)")
    plan.add_argument("--out", required=True, help="the plan file; an existing one is continued")
    plan.add_argument("--limit", type=int, help="locate this many signals at most, newest first")
    plan.add_argument("--workers", type=int, default=4, help="signals located at once (default 4)")
    plan.add_argument("--count-only", action="store_true",
                      help="count the signals and stop: no model call")
    add_summary_options(plan)
    plan.set_defaults(run=run_plan)

    summary = commands.add_parser("summary", help="print what a plan file holds")
    summary.add_argument("--plan", required=True)
    add_summary_options(summary)
    summary.set_defaults(run=run_summary)

    apply = commands.add_parser("apply", help="write a plan: move the signals, record the names")
    apply.add_argument("--plan", required=True)
    apply.add_argument("--min-signals", type=int, default=DEFAULT_MIN_SIGNALS,
                       help="a name enters the queue when this many signals name it (default 2)")
    apply.add_argument("--limit", type=int, help="apply this many signals at most")
    apply.set_defaults(run=run_apply)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    logging.basicConfig(
        level=logging.INFO if args.verbose else logging.WARNING,
        format="%(levelname)s %(name)s: %(message)s",
    )
    return args.run(args)


if __name__ == "__main__":
    sys.exit(main())
