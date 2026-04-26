"""Smoke test for the insights instrumentation.

Makes one real Claude call through the instrumented `call_claude()` and reports
what telemetry was emitted. Does NOT require Dataminr, Redis, or clear-api —
only the Anthropic API key and (optionally) the insights ingest token.

Usage:
  # A. Live test — sends one row to the deployed dashboard.
  #    Requires INSIGHTS_INGEST_TOKEN in .env.
  uv run python scripts/test_insights.py

  # B. Failure-mode test — points telemetry at a dead URL and verifies the
  #    pipeline call still succeeds (just logs a warning).
  uv run python scripts/test_insights.py --break-insights

  # C. Disabled test — empty token, telemetry should be a no-op.
  INSIGHTS_INGEST_TOKEN= uv run python scripts/test_insights.py
"""

import argparse
import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Bypass any pre-existing .env drift in dataminr fields by injecting dummies
# BEFORE config loads. This script only needs Anthropic + insights env vars.
os.environ.setdefault("DATAMINR_CLIENT_ID", "smoke-test")
os.environ.setdefault("DATAMINR_CLIENT_SECRET", "smoke-test")

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")

parser = argparse.ArgumentParser()
parser.add_argument("--break-insights", action="store_true",
                    help="Point INSIGHTS_API_URL at a dead host to test failure swallowing")
args = parser.parse_args()

if args.break_insights:
    os.environ["INSIGHTS_API_URL"] = "http://127.0.0.1:1"
    os.environ.setdefault("INSIGHTS_INGEST_TOKEN", "smoke-test-bad-url")

from src.config import settings
from src.clients import insights
from src.clients.claude import call_claude

print(f"\n{'='*60}")
print(f"insights URL:   {settings.insights_api_url}")
print(f"insights token: {'set' if settings.insights_ingest_token else 'EMPTY (telemetry disabled)'}")
print(f"resolved env:   {insights._resolve_env()}")
print(f"git sha:        {insights._git_sha()}")
print(f"claude model:   {settings.claude_model}")
print(f"{'='*60}\n")

if not settings.anthropic_api_key:
    print("ERROR: ANTHROPIC_API_KEY not set. Cannot make a real Claude call.")
    sys.exit(1)

system = "You return JSON only. No prose, no markdown."
user = 'Return exactly this JSON: {"ok": true, "n": 42, "msg": "smoke"}'

print("Calling call_claude(stage='classify', prompt_version='smoke-v1')…")
try:
    result = call_claude(
        system, user,
        stage="classify",
        prompt_version="smoke-v1",
        signal_id="smoke-test-signal",
    )
    print(f"\nClaude returned: {result}")
except Exception as exc:
    print(f"\nClaude call FAILED: {type(exc).__name__}: {exc}")
    sys.exit(2)

if args.break_insights:
    print("\n[--break-insights mode] If you saw a '[insights] POST /api/calls failed' warning ")
    print("above AND this script reached this line, failure swallowing works correctly.")
elif settings.insights_ingest_token:
    print("\nNow open https://clear-pipeline-insights.vercel.app and look for:")
    print(f"  - run named 'live' in env '{insights._resolve_env()}'")
    print("  - one llm_call with stage='classify', prompt_version='smoke-v1', signal_id='smoke-test-signal'")
    print("  - non-zero cost_usd (computed from input/output tokens)")
else:
    print("\nINSIGHTS_INGEST_TOKEN was empty — telemetry was a no-op (nothing posted).")
    print("Set it in .env and re-run to verify the live path.")
