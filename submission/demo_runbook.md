# Oracle Forge Demo Runbook

This is the recording script for the final demo video. It is designed to stay under 8 minutes and to show the shared team setup, two live database families, self-correction, context layers, the evaluator, and the probe library.

## Phase 0. Team Setup

Goal: show the shared team environment first so the recording is clearly reproducible.

Run:

```bash
ssh gersum@100.101.234.123
cd /shared/DataAgentBench/oracle_forge_v3
pwd
tailscale status
tmux -S /shared/tmux/oracle-forge.sock list-sessions
tmux -S /shared/tmux/oracle-forge.sock attach -t oracle-forge-gpt5
```

Say:
- This is the shared team host.
- Tailscale shows the group-connected environment.
- The shared tmux session is the live workspace for the team.

If `sed` is unavailable, use `head` or Python later in the recording.

## Phase 1. Show tmux windows

After attaching:

```bash
tmux list-windows
```

Optional if you need to create labeled windows:

```bash
tmux -S /shared/tmux/oracle-forge.sock new-window -t oracle-forge-gpt5 -n agent
tmux -S /shared/tmux/oracle-forge.sock new-window -t oracle-forge-gpt5 -n score
tmux -S /shared/tmux/oracle-forge.sock new-window -t oracle-forge-gpt5 -n probes
tmux list-windows
```

Say:
- The demo is running inside the shared tmux session.
- The windows separate agent work, scoring, and probe inspection.

## Phase 2. Live Yelp Query

Goal: show a live pass on a multi-database Yelp question.

Run:

```bash
source .venv/bin/activate
REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 \
python3 run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer
```

Say:
- This is Yelp, using MongoDB plus DuckDB.
- The agent is answering a real benchmark question on the shared host.
- The validator accepts the answer.

Fallback if the live run is slow:

```bash
python3 - <<'PY'
import json
from pathlib import Path
d = json.loads(Path("results/gpt5_yelp_q1_trials50.json").read_text())
print(d["results"][0]["trial_results"][0]["answer"])
PY
```

## Phase 3. Live CRM Query

Goal: show a different database family.

Run:

```bash
REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 \
python3 run_benchmark_query.py --dataset crmarenapro --query-id 1 --validate-answer
```

Say:
- This is CRM, which uses a different DB stack.
- The same agent runtime is reading a different set of live records.
- The validator accepts the answer.

## Phase 4. Self-Correction Loop

Goal: show a query that failed before and now passes.

Run:

```bash
grep -n "Missing category: restaurants" eval/score_log.md
REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 \
python3 run_benchmark_query.py --dataset yelp --query-id 6 --validate-answer
```

Say:
- This query used to lose the category field.
- The correction log captured the failure.
- The repaired path now returns a validator-accepted answer.

If you need a faster fallback, cite the entry in `eval/score_log.md` instead of waiting.

## Phase 5. Context Layers

Goal: show the layered context stack.

Run:

```bash
head -n 220 /shared/DataAgentBench/oracle_forge_v3/kb/architecture/context_layers.md
head -n 220 /shared/DataAgentBench/oracle_forge_v3/kb/domain/CHANGELOG.md
head -n 220 /shared/DataAgentBench/oracle_forge_v3/kb/corrections/corrections_log.md
```

Say:
- Layer 1 is schema and metadata.
- Layer 2 is domain knowledge.
- Layer 3 is corrections memory.
- The agent uses those layers before it plans the query.

If `head` is not available:

```bash
python3 - <<'PY'
from pathlib import Path
for path in [
    "/shared/DataAgentBench/oracle_forge_v3/kb/architecture/context_layers.md",
    "/shared/DataAgentBench/oracle_forge_v3/kb/domain/CHANGELOG.md",
    "/shared/DataAgentBench/oracle_forge_v3/kb/corrections/corrections_log.md",
]:
    print(f"\n=== {path} ===\n")
    print("\n".join(Path(path).read_text().splitlines()[:220]))
PY
```

## Phase 6. Evaluation Harness

Goal: show scoring and trace preservation.

Run:

```bash
python3 eval/score.py --results results/gpt5_crmarenapro_50t.json
python3 - <<'PY'
import json
from pathlib import Path
d = json.loads(Path("results/gpt5_crmarenapro_50t.json").read_text())
print(json.dumps(d["results"][0]["trial_results"][0], indent=2))
PY
```

Say:
- This is the evaluation harness.
- It records the trial history and the query trace.
- The CRM family passed 50 trials per query.

## Phase 7. Probe Library

Goal: show that probe failures drove fixes.

Run:

```bash
python3 - <<'PY'
from pathlib import Path
text = Path("probes/probes.md").read_text().splitlines()
print("\n".join(text[:260]))
PY
```

Say:
- The probe library has 20 probes.
- It covers join-key mismatches, multi-database routing, text extraction, and domain knowledge.
- Several fixes in the agent came directly from these probes.

## Phase 8. Submission Artifact

Goal: show the packaged result.

Run:

```bash
python3 - <<'PY'
import json
from pathlib import Path
d = json.loads(Path("submission/gpt-5_result.json").read_text())
print("rows:", len(d))
print("first:", d[0])
PY
```

Say:
- This is the consolidated flattened submission payload.
- It contains the completed 50-trial families already verified in the workspace.

## Suggested order for the recording

1. Team setup: Tailscale + tmux
2. Yelp live pass
3. CRM live pass
4. Self-correction example
5. Context layers
6. Score / trace
7. Probe library
8. Final submission artifact

## If something breaks

- If `sed` is missing, use `head` or the Python fallback.
- If a live query stalls, switch to the already-scored result JSON and narrate the validated output.
- If tmux attach fails, show `tmux list-sessions` first, then reattach.

