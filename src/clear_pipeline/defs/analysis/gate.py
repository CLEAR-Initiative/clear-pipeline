"""Analysis regeneration gate (ADR-0008) — pure decision logic.

Both drains (on-demand + automation) call :func:`decide_generation` BEFORE
spending an LLM generation. A regeneration proceeds only if there is new evidence
in the frame since the live analysis was generated AND that analysis is older
than the 24h floor — unless the request is ``force``'d (admin escape hatch).

When the gate says skip, the caller bumps the analysis's ``lastSyncedAt`` ("we
checked") without writing a new version, and (for automations) still advances the
schedule. The 24h floor is also enforced authoritatively in clear-api's
``upsertAnalysis``; this gate is the cheaper pre-check that avoids the LLM cost.

Pure (no I/O) so it is unit-testable without a live clear-api.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone

MIN_GAP_HOURS = 24


@dataclass(frozen=True)
class GateDecision:
    generate: bool
    reason: str


def _parse_iso(ts: str | None) -> datetime | None:
    """Parse a clear-api DateTime (ISO-8601) to an aware UTC datetime, or None.
    A naive timestamp is assumed UTC so the subtraction below never mixes
    aware/naive."""
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except ValueError:
        return None
    return dt.replace(tzinfo=timezone.utc) if dt.tzinfo is None else dt


def decide_generation(
    *,
    current: dict | None,
    latest_evidence_at: str | None,
    now: datetime,
    force: bool = False,
    min_gap_hours: int = MIN_GAP_HOURS,
) -> GateDecision:
    """Decide whether to (re)generate a frame's analysis.

    ``current`` is the live analysis row (or None if none exists) — only its
    ``generatedAt`` is read. ``latest_evidence_at`` is the frame's evidence
    watermark (max knowledgebase ingestion time). ``now`` must be timezone-aware.
    """
    if force:
        return GateDecision(True, "forced")
    if not current:
        return GateDecision(True, "no-existing-analysis")

    generated = _parse_iso(current.get("generatedAt"))
    if generated is not None:
        gap_hours = (now - generated).total_seconds() / 3600
        if gap_hours < min_gap_hours:
            return GateDecision(False, "within-24h-floor")

    # Freshness: skip when nothing has been ingested for the frame since the last
    # generation (no watermark, or it's not newer than generatedAt).
    latest = _parse_iso(latest_evidence_at)
    if latest is None:
        return GateDecision(False, "no-new-evidence")
    if generated is not None and latest <= generated:
        return GateDecision(False, "no-new-evidence")
    return GateDecision(True, "new-evidence")
