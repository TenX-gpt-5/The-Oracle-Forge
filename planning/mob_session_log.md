# Mob Session Log

Use this file to record AI-DLC gate approvals at mob sessions.

## Entries

## 2026-04-11
Sprint: Sprint 1 inception approval
Phase gate: Inception -> Construction
Members present: Gersum, Driver 1, Driver 2, Facilitator
Driver: Gersum (on remote tmux session)
Hardest question asked: "If we use a 3-layer explicit context system, won't we blow through the 128k context window on complex dataset schemas?"
Answer given: "No, because the Context Cortex acts as a filter, only loading the `kb/domain` and `kb/schema` fragments explicitly requested by the Planner. We are not blindly concatenating the entire KB."
Friction/Revision moment: There was significant debate over whether to spend time setting up a local mock server vs. diving straight into the shared `trp-gpt5` remote box. We compromised by mandating remote development but allowing local MCP configurations for syntax testing.
Evidence reviewed:
- `planning/inception/2026-04-11-sprint-1.md`
- `planning/README.md` sprint flow
- `README.md` remote runbook path
Decision:
- approved
Follow-ups:
- ensure score log and held-out artifacts are updated after each rerun

## 2026-04-11
Sprint: Sprint 1 baseline validation
Phase gate: Construction -> Operations
Approved by: Team shared session participants (`oracle-forge-gpt5`)
Driver: shared driver on remote tmux session
Hardest question: Is Toolbox the benchmark-authoritative execution path right now?
Answer: No. The benchmark-authoritative path is the remote DAB flow; Toolbox remains a health-check and partial tool interface in the current hybrid setup.
Evidence reviewed:
- `python run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer`
- validator output contained `"is_valid": true`
- `curl http://127.0.0.1:5000/` returned healthy Toolbox server response
Decision:
- approved
Follow-ups:
- add additional benchmark query validations and log score progression
- expand adversarial probes with observed failures and post-fix outcomes

## 2026-04-13
Sprint: Sprint 2 Scaling the 4 DB types
Phase gate: Inception -> Construction
Approved by: Team
Driver: Gersum
Hardest question: How do we verify SQLite data mapping?
Answer: Through direct local sandbox testing and the eval regression harness.
Evidence reviewed:
- Inception doc 2
Decision:
- approved

## 2026-04-13
Sprint: Sprint 2 Yelp smoke closure
Phase gate: Operations review
Approved by: Team shared session participants (`oracle-forge-gpt5`)
Driver: Gersum
Hardest question: What proves the Yelp path is fully green now?
Answer: The shared-server smoke pass shows all seven Yelp queries (`q1` through `q7`) returning `is_valid: true` in the official remote validator.
Evidence reviewed:
- `eval/score_log.md`
- shared-server remote validator outputs for `q1` through `q7`
Decision:
- approved
Follow-ups:
- expand the same validation pattern to the next dataset


  ## Team Members and Roles

| Name | Role |
|------|------|
| Eyobed Feleke | Driver |
| Gersum Asfaw | Driver |
| Samuel Lachisa| Intelligence Officer |
| Kidane Gebremedhin| Intelligence Officer |
| Bethelhem Abay | Signal Corps |
| Lidya Dagnew | Signal Corps |
