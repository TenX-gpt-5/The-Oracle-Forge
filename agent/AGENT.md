# Oracle Forge Agent

## Architecture Overview

Oracle Forge is an orchestrated data-agent runtime for DataAgentBench. It combines:

- an orchestrator that owns the turn lifecycle
- a planner that infers query shape and required sources
- layered context retrieval
- execution routing across available database paths
- validation and repair
- answer synthesis
- experience logging and memory promotion

## Key Design Decisions

- Hybrid runtime:
  Toolbox is present for PostgreSQL, SQLite, and MongoDB, while benchmark-critical DuckDB access currently flows through the remote DAB path.
- Benchmark-first execution:
  The current runtime prioritizes verified end-to-end benchmark execution over premature interface uniformity.
- Layered context:
  The agent separates reusable rules, project memory, schema hints, join-key knowledge, text-field hints, and episodic recall.

## What Worked

- Remote DAB query bundle retrieval
- Yelp query 1 benchmark path with official validation
- Real remote access to SQLite, DuckDB, MongoDB, and PostgreSQL through the working hybrid stack
- Basic architecture tests and harness path

## What Did Not Work Yet

- Full Toolbox-first database execution across all four DAB database types
- Full benchmark submission flow and score logging
- Mature correction-driven learning loop across many benchmark failures
- Full adversarial probe coverage

## Evidence Pointers

- Smoke test: `python run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer`
- KB: [kb/README.md](/Users/gersumasfaw/week8_9/kb/README.md)
- Planning: [planning/README.md](/Users/gersumasfaw/week8_9/planning/README.md)
- Alignment: [MANUAL_ALIGNMENT.md](/Users/gersumasfaw/week8_9/MANUAL_ALIGNMENT.md)
