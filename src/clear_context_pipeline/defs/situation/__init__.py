"""Weekly situation-analysis generation.

Downstream of ``reliefweb_weekly_datapoint_aggregations``, this
package produces one pre-computed snapshot per (country × year)
covering:

  1. datapoints                  (deterministic, from aggregated_datapoints)
  2. ai_summary                  (LLM — Phase C)
  3. context_risks               (LLM — Phase C)
  4. hazards_and_vulnerabilities (LLM — Phase C)
  5. displacement                (LLM — Phase C)
  6. sectors                     (LLM — Phase D)
  7. sources                     (deterministic, from contributing_report_ids)

Phase B (this initial cut) ships only the deterministic components 1
and 7; the LLM-heavy components are stubbed out so the dashboard can
render an empty state for them while the write path proves out.
"""
