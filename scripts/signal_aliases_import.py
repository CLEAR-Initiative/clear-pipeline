"""Deploy step 4 (ADR-0015), part 2 of 2: record a `gold.signal_id_aliases`
row for every pre-switch gx Signal, so `postgres_ids` resolves its minted
`signalId` to the id Postgres actually holds.

Input is the CSV from scripts/signal_aliases_export.sql (columns id,
external_id, source), exported through psql or TablePlus. The pipeline never connects to Postgres: the export runs
through psql, this script only writes the lake. Iceberg settings come from the
pipeline's own config. Run from the repo root, per environment:

    uv run python scripts/signal_aliases_import.py signal_aliases.csv          # dry run
    uv run python scripts/signal_aliases_import.py signal_aliases.csv --apply  # write

Rows whose externalId carries no gx prefix (legacy-only, x:, whatsapp:, url:,
cases) are skipped. The script aborts without writing on a conflict: two
Postgres rows for one minted id, or a stored alias pointing at another
legacyId. Re-running is safe: stored aliases are not rewritten.
"""

import argparse
import csv
import sys
from collections import Counter, defaultdict

from clear_pipeline.defs.gx_pipeline.signal_ids import signal_id
from clear_pipeline.defs.gx_pipeline.sources import GX_SOURCES


def _source_for(external_id: str) -> str | None:
    """The gx source whose prefix this externalId carries (prefixes are disjoint)."""
    for s in GX_SOURCES:
        if external_id.startswith(s.id_prefix) and external_id != s.id_prefix:
            return s.source
    return None


def plan(rows: list[dict], existing: dict[str, str]) -> tuple[list[dict], Counter, list[str]]:
    """``(aliases to write, counts, conflicts)`` for the exported ``rows``,
    given the stored ``{signalId: legacyId}``. Nothing is written if conflicts
    is non-empty."""
    counts: Counter = Counter()
    by_id: dict[str, set[str]] = defaultdict(set)
    external: dict[str, str] = {}
    for row in rows:
        source = _source_for(row["external_id"])
        if source is None:
            counts["skipped: no gx prefix"] += 1
            continue
        sid = signal_id(row["external_id"])
        if row["id"] == sid:
            counts["skipped: already the minted id"] += 1
            continue
        by_id[sid].add(row["id"])
        external[sid] = row["external_id"]
        counts[f"source: {source}"] += 1

    aliases, conflicts = [], []
    for sid, legacy_ids in sorted(by_id.items()):
        if len(legacy_ids) > 1:
            conflicts.append(f"{external[sid]}: several Postgres rows {sorted(legacy_ids)}")
            continue
        (legacy,) = legacy_ids
        stored = existing.get(sid)
        if stored == legacy:
            counts["unchanged: alias already stored"] += 1
        elif stored is not None:
            conflicts.append(f"{external[sid]}: stored legacyId {stored}, Postgres has {legacy}")
        else:
            aliases.append({"signalId": sid, "externalId": external[sid], "legacyId": legacy})
    counts["to write"] = len(aliases)
    return aliases, counts, conflicts


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("csv_path")
    parser.add_argument("--apply", action="store_true", help="write the aliases (default: dry run)")
    args = parser.parse_args()

    # utf-8-sig: a GUI export (TablePlus) may start with a BOM, which would rename `id`.
    with open(args.csv_path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    missing = {"id", "external_id"} - set(rows[0] if rows else {"id", "external_id"})
    if missing:
        print(f"CSV lacks columns {sorted(missing)}", file=sys.stderr)
        return 2

    from clear_pipeline.defs.gx_pipeline import iceberg_signals

    stored = iceberg_signals.aliases_df()
    existing = dict(stored[["signalId", "legacyId"]].itertuples(index=False))
    aliases, counts, conflicts = plan(rows, existing)

    print(f"{len(rows)} exported rows")
    for key, n in sorted(counts.items()):
        print(f"  {key}: {n}")
    if conflicts:
        print(f"{len(conflicts)} conflicts, nothing written:", file=sys.stderr)
        for c in conflicts:
            print(f"  {c}", file=sys.stderr)
        return 1
    if not args.apply:
        print("dry run: re-run with --apply to write")
        return 0
    written = iceberg_signals.upsert_aliases(aliases)
    print(f"wrote {written} aliases")
    return 0


if __name__ == "__main__":
    sys.exit(main())
