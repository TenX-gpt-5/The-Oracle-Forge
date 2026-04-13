# Manual Alignment Check

This file compares the current Oracle Forge workspace against the practitioner manual and the Oracle Forge V3 architecture.

## Overall Verdict

Current status:

- aligned with the manual's high-level architecture direction
- aligned with the remote shared-team infrastructure model
- partially aligned with the manual's intended database access model
- not yet aligned with the manual's full KB, evaluation, and submission packaging expectations

Short version:

- V3 architecture: yes
- benchmark starter path: yes
- data loaded remotely: yes
- uniform Toolbox-first DB access across all four DB types: not yet

## Aligned

### Shared server workflow

Aligned with the manual's team-server model.

Implemented:

- shared remote server under `/shared/DataAgentBench`
- Tailscale-based join flow
- shared tmux session workflow
- shared Oracle Forge workspace at `/shared/DataAgentBench/oracle_forge_v3`

Repo evidence:

- [README.md](/Users/gersumasfaw/week8_9/README.md)
- [TEAM_JOIN.md](/Users/gersumasfaw/week8_9/TEAM_JOIN.md)
- [HEALTHCHECK.md](/Users/gersumasfaw/week8_9/HEALTHCHECK.md)

### DAB dataset presence

Aligned.

Verified on the remote server:

- DAB root exists at `/shared/DataAgentBench`
- dataset folders exist including `query_yelp` and `query_crmarenapro`
- dataset assets exist for SQLite, DuckDB, MongoDB dump folders, and PostgreSQL SQL dump

### Yelp-first validation strategy

Aligned.

The manual recommends Yelp as the first validation dataset. The current repo uses:

```bash
python run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer
```

This has already produced:

```text
"is_valid": true
```

### V3 architecture shape

Aligned in structure.

Implemented major runtime layers:

- orchestrator
- planner
- context cortex
- execution router
- validator
- repair loop
- answer synthesizer
- experience logging
- memory consolidation and review

Repo evidence:

- [ARCHITECTURE_V3.md](/Users/gersumasfaw/week8_9/ARCHITECTURE_V3.md)
- [src/agent/orchestrator.py](/Users/gersumasfaw/week8_9/src/agent/orchestrator.py)
- [src/planning/planner.py](/Users/gersumasfaw/week8_9/src/planning/planner.py)
- [src/agent/execution_router.py](/Users/gersumasfaw/week8_9/src/agent/execution_router.py)
- [src/agent/validator.py](/Users/gersumasfaw/week8_9/src/agent/validator.py)
- [src/agent/repair_loop.py](/Users/gersumasfaw/week8_9/src/agent/repair_loop.py)

### Real remote data access

Aligned in practice.

Verified:

- SQLite files are present and populated
- DuckDB files are present and populated
- MongoDB data restores and queries successfully through DAB tooling
- PostgreSQL queries successfully through DAB tooling

## Partially Aligned

### MCP Toolbox as the standard DB interface

Partially aligned.

What exists:

- [mcp/tools.yaml](/Users/gersumasfaw/week8_9/mcp/tools.yaml) defines PostgreSQL, SQLite, and MongoDB sources and tools
- the repo does not currently include a native DuckDB Toolbox source despite the manual's broader wording

What is missing:

- DuckDB is not available as a native Toolbox source in the current setup
- the runtime does not primarily use Toolbox as its execution backbone

Current reality:

- Toolbox is present
- Oracle Forge mostly uses the remote DAB adapter path for benchmark execution
- the current Toolbox runtime is verified via `curl http://127.0.0.1:5000/` and `toolbox invoke ...`

Relevant files:

- [mcp/tools.yaml](/Users/gersumasfaw/week8_9/mcp/tools.yaml)
- [src/dab/remote_dab_adapter.py](/Users/gersumasfaw/week8_9/src/dab/remote_dab_adapter.py)

### Consistent multi-database access model

Partially aligned.

All four DB types are reachable, but not through one stable always-on access pattern.

Current access pattern:

- SQLite: direct file-backed access
- DuckDB: file-backed, but routed through DAB or direct duckdb client
- MongoDB: restored/managed by DAB tooling during access flow
- PostgreSQL: loaded from SQL dump by DAB tooling during access flow

This is functional, but hybrid rather than cleanly unified.

### Context engineering depth

Partially aligned.

The architecture for layered context exists, but the manual expects a much richer Knowledge Base and stronger correction loops than currently exist in the repo.

Current implementation includes:

- global memory
- project memory
- schema index
- join key store
- text inventory
- episodic recall

But not yet:

- the full KB directory and workflow described in the manual
- injection-tested context docs
- mature dataset-specific correction content

### Evaluation harness

Partially aligned.

What exists:

- lightweight harness
- score tracker
- basic architecture tests

What is not there yet:

- broader benchmark harness coverage
- held-out evaluation set
- score progression log
- adversarial probe library tied to fixes

Repo evidence:

- [src/eval/harness.py](/Users/gersumasfaw/week8_9/src/eval/harness.py)
- [src/eval/score_tracker.py](/Users/gersumasfaw/week8_9/src/eval/score_tracker.py)
- [tests/test_architecture.py](/Users/gersumasfaw/week8_9/tests/test_architecture.py)

## Not Yet Aligned

### Persistent direct teammate DB access

Not yet aligned with the spirit of a teammate-friendly direct-access setup.

Important nuance:

- SQLite and DuckDB are directly accessible on disk
- PostgreSQL and MongoDB are currently managed by DAB loader/query tooling
- PostgreSQL is not reliably available as a persistent database for plain `psql`
- MongoDB is verified through DAB tooling, but not yet documented as a persistent teammate-facing direct `mongosh` workflow

### Full manual KB structure

Partially aligned.

The manual expects:

- `kb/architecture/`
- `kb/domain/`
- `kb/evaluation/`
- `kb/corrections/`
- `CHANGELOG.md` files
- injection-test evidence

These directories now exist with templates and starter docs, but they are still early and not yet fully populated with comprehensive injection-tested content.

### Submission packaging

Partially aligned.

The manual expects repo content such as:

- `AGENT.md`
- `planning/`
- `results/`
- `probes/probes.md`
- richer `eval/`
- `signal/`

These directories are now scaffolded in this repo, but completion quality depends on filling them with real benchmark artifacts, final score logs, and submission evidence.

### Adversarial probe library

Not yet aligned.

The manual expects at least 15 probes across multiple failure categories. This repo does not yet contain that probe library.

## Database Approach Assessment

## Is the current DB approach valid?

Yes.

It is valid for proving benchmark execution and validating the remote environment. The current approach is enough to show:

- remote datasets exist
- the Oracle Forge runtime can execute DAB queries
- real benchmark data is being accessed
- the Yelp starter path works end to end

## Is the current DB approach exactly what the manual implies?

No.

The manual points toward a more stable Toolbox-centered database interface. The current implementation is better described as:

`hybrid Toolbox + remote DAB execution`

That means:

- acceptable as a working benchmark architecture
- not yet the cleanest final infrastructure story

## Recommended Next Steps

1. Decide and document the canonical database access model.
2. Either make Toolbox the primary runtime interface or explicitly bless the hybrid model in the architecture docs.
3. Make PostgreSQL and MongoDB persistently accessible if plain teammate CLI access is a team requirement.
4. Add a short architecture note explaining why DuckDB currently uses the remote DAB path instead of native Toolbox.
5. Build the manual-required KB, probes, planning, and results structure.
6. Expand evaluation from smoke-test level to regression and held-out benchmark tracking.

## Suggested Wording For The Team

Oracle Forge currently implements a benchmark-valid hybrid database architecture. SQLite and DuckDB are file-backed on the remote server, while MongoDB and PostgreSQL are managed through the DataAgentBench loading and query flow. This is sufficient for current benchmark execution and remote team collaboration, but it is not yet a full always-on Toolbox-only database architecture.
