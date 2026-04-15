# OpenAI Data Agent Context Architecture

**Injection Test Evidence:**
*   **Test Query:** "What are the six layers of offline enrichment used by OpenAI?"
*   **Expected Answer:** "The agent must list Schema Rules, Column Stats, ER Graphs, Join Key Anomalies, Target Metrics, and Prior Session Repairs."

Here is how OpenAI structures context for enterprise data agents. Context is the primary bottleneck, not SQL reasoning.

**Six-Layer Context Design (Offline Enrichment):**
Context is prepared offline before any user query arrives. Each layer adds a different type of knowledge the agent cannot infer from raw schema alone:

1. **Schema Rules and Table Metadata:** Table names, column names and types, primary/foreign keys, table descriptions. For DAB: this is the output of `schema_introspection.py` — one normalized manifest per database.
2. **Column-Level Statistics and Unique Values:** Min/max, null rates, cardinality, top-N distinct values per column. For DAB: knowing that `customer_id` has 10,000 unique values in PostgreSQL but only 8,500 in MongoDB immediately signals a data quality issue before any join is attempted.
3. **Entity Relationship Graphs:** Which tables connect to which, via what keys, with what cardinality (1:1, 1:N, N:M). For DAB: the ER graph reveals that `transactions` → `customers` is 1:N in PostgreSQL but `reviews` → `customers` is also 1:N in MongoDB, so a cross-DB merge will produce an N:M explosion without deduplication.
4. **Join Key Anomalies and Formats:** Documented format mismatches, known ID prefix patterns, normalization rules. For DAB: this is the `join_keys.md` content — the CUST- prefix, zero-padded integers, phone format differences.
5. **Target Metrics Specific to the Domain:** Business term definitions, calculation formulas, time-window rules. For DAB: this is the `domain_terms.md` content — what "active customer" and "churn" actually mean in each dataset.
6. **Prior Session Repairs:** Corrections from previous failures: what went wrong, what fixed it, which datasets were affected. For DAB: this is the `corrections_log.md` — the self-learning loop.

**Closed-Loop Self-Correction:**
The agent writes code, executes it, and reads the table output. If an unexpected result occurs (e.g., zero rows from a join, suspiciously high counts, null-heavy columns), it does NOT guess a fix. Instead, it queries its enriched metadata — specifically Layers 2 and 4 — to diagnose whether the issue is a format mismatch, a wrong table, or a missing filter. The diagnosis drives the retry, not random prompt variation.

**Table Enrichment as the Hardest Sub-Problem:**
OpenAI reported that enriching 70,000 internal tables was harder than building the query engine. The enrichment pipeline pre-digests database definitions so the runtime agent never fetches raw broad schemas. Instead, it queries the enriched metadata map to locate relevant columns before writing the real query. For Oracle Forge, this means `schema_introspection.py` output must be pre-computed and cached, not generated on every query.

## Injection Test
**Q:** "What does the agent do when a join returns zero rows? Does it guess a fix?"
**Expected:** "No. It queries its enriched metadata (Layers 2 and 4: column statistics and join key anomalies) to diagnose whether the issue is a format mismatch, wrong table, or missing filter. The diagnosis drives the retry."
**Result:** PASS
**Date:** 2026-04-11
