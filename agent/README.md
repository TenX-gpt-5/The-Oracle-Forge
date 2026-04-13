# Agent Directory

This directory holds the benchmark-submission-facing agent description.

Current code still lives under [src](/Users/gersumasfaw/week8_9/src). This folder exists to satisfy the submission-oriented repo shape expected by the programme manual.

Contents:

- [AGENT.md](/Users/gersumasfaw/week8_9/agent/AGENT.md)
- [tools.yaml](/Users/gersumasfaw/week8_9/agent/tools.yaml) (submission-facing MCP config covering PostgreSQL, SQLite, MongoDB, DuckDB)
- [requirements.txt](/Users/gersumasfaw/week8_9/agent/requirements.txt)
- [SOURCE_INDEX.md](/Users/gersumasfaw/week8_9/agent/SOURCE_INDEX.md) (maps to runtime files in `src/`)

Runtime entry points used in practice:

- `run_agent.py`
- `run_benchmark_query.py`
