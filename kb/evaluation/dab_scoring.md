# DataAgentBench Evaluation & Scoring (pass@1)

Here is how DataAgentBench evaluates an agent.

**Scoring Requirements:**
The benchmark measures "pass@1". This means the very first final answer returned by the agent is the single outcome evaluated. The agent MUST internally self-correct; once it yields a final structured output, the trial is complete and graded.

**Submission Requirements:**
- 54 Queries across 12 datasets (retail, healthcare, finance).
- Each query must run over n >= 50 trials to prove deterministic capability.
- Output constraint: Must return `{answer, query_trace, confidence}`.
- The final PR submission includes the `results.json` and `AGENT.md`.

**Four Failure Categories Targeted by the Harness:**
1. Multi-database routing failure
2. Ill-formatted key mismatch
3. Unstructured text extraction failure
4. Domain knowledge gap

**Required Output JSON Schema:**
```json
{
  "query_id": "yelp_q03",
  "answer": "The top 3 segments are...",
  "query_trace": ["inspect_schema(postgres, yelp)", "run_sql_postgres(...)", "run_mongo_pipeline(...)"],
  "confidence": 0.85
}
```

Do not attempt a standard benchmark run without verifying that the evaluation harness correctly captures the exact sequence of tool/scratchpad boundaries for the `query_trace`.

## Injection Test
**Q:** "How many trials per query does DAB require and what scoring metric is used?"
**Expected:** "DAB requires n ≥ 50 trials per query and uses pass@1 scoring — only the first final answer per trial is graded."
**Result:** PASS
**Date:** 2026-04-11
