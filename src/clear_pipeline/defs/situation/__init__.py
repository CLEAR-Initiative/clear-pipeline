"""Weekly situation-analysis generation.

Downstream of ``reliefweb_weekly_datapoint_aggregations``, this
package produces one pre-computed snapshot per (country × year)
covering:

  1. datapoints                  (deterministic, from aggregated_datapoints)
  2. ai_summary                  (LLM, RAG-grounded)
  3. context_risks               (LLM, RAG-grounded; 9 domains)
  4. hazards_and_vulnerabilities (LLM, RAG-grounded)
  5. displacement                (LLM, RAG-grounded)
  5b. humanitarian_access        (LLM, RAG-grounded)
  6. sectors                     (LLM, RAG-grounded, one call per sector)
  7. sources                     (deterministic, from contributing_report_ids)

All components are generated. The LLM-backed ones each run their own
RAG search over ``knowledgebase`` and isolate their failures: one
generator erroring leaves the rest of the row intact and ships that
component empty. ``SITUATION_SKIP_NARRATIVE`` skips narrative/sector
components wholesale, producing a deterministic-only row when the
provider is down or the budget is spent.
"""
