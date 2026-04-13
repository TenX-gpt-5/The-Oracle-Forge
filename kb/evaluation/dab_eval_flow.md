# DAB Evaluation Flow

Oracle Forge currently uses a two-level evaluation flow.

Level 1 is the local Oracle Forge smoke-test path. The strongest verified command today is:

```bash
cd /shared/DataAgentBench/oracle_forge_v3
source venv/bin/activate
python run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer
```

This confirms that the Oracle Forge runtime can fetch a remote DAB query bundle, execute the benchmark path, and pass the official validator for that query.

Level 2 is the target full DAB benchmark path. That is the submission-oriented flow described by the benchmark itself: run the broader evaluation harness, write results JSON, and compute pass@1 over the benchmark set. Oracle Forge has not fully reached this stage yet. The current harness in this repo is useful for repeatable internal testing, but it is lighter than the full benchmark submission path.

When documenting progress, the team should distinguish clearly between:

- smoke-test validated
- harness validated
- full benchmark evaluated

## Injection Test

Question:
What is Oracle Forge's strongest verified DAB evaluation path right now, and how does it differ from the full benchmark target?

Expected answer:
The strongest verified path is the Yelp query 1 smoke test with remote validation. It proves a real benchmark path works, but it is not yet the full multi-query benchmark submission flow.

Status: pass

Last verified: 2026-04-11
