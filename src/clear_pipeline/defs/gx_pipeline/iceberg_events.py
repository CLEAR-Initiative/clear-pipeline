"""STUBBED — gold events (SCD2) persistence is paused pending a decision on
how to keep it synchronous with clear-api's own Event table (today, both
would be written independently with no single source of truth). See
docs/data-quality-medallion-implementation.md §6.

Every function here is a no-op: no Iceberg table is created or written.
`factory.py`'s temporal/match/gold stages still run their in-memory
event-clustering logic (so `eventId`/`districtKey`/etc. keep flowing through
gold), but nothing about events is persisted or pushed to clear-api —
`_push` only pushes signals now.

To restore the real SCD2 implementation once the sync design lands, see
this file's git history (the commit that introduced this stub).
"""

import pandas as pd

_EVENTS_COLUMNS = [
    "eventId", "version", "effectiveFrom", "effectiveTo", "isCurrent", "contentHash",
    "districtKey", "eventType", "glideCode", "title", "description", "severity",
    "casualties", "signalIds", "startedAt", "firstSignalCreatedAt", "lastSignalCreatedAt",
]


def get_events_table(source: str):
    return None


def current_event(table, event_id: str) -> dict | None:
    return None


def current_events_df(table) -> pd.DataFrame:
    return pd.DataFrame(columns=_EVENTS_COLUMNS)


def merge_event(table, event: dict) -> dict:
    """No persistence — shaped like a fresh version-1 current row so
    callers' bookkeeping doesn't need a stub-aware branch."""
    return {
        **event, "version": 1, "effectiveFrom": None, "effectiveTo": None,
        "isCurrent": True, "contentHash": None,
    }
