# Team Join Guide

This is the fastest way for teammates to join the shared Oracle Forge environment on the team server.

## 1. Join Tailscale

Install Tailscale on your device and sign in to the same tailnet as the team server.

Check that you are connected:

```bash
tailscale status
```

You should be able to see the team server:

```text
100.101.234.123  ip-10-0-10-138-tenai
```

If you do not see that server, you are not yet on the same Tailscale network.

## 2. SSH Into The Team Server

From your machine:

```bash
ssh trp-gpt5
```

If SSH works, you are on the shared server.

## 3. Join The Shared tmux Session

This team session uses a shared tmux socket so multiple Linux users can attach to the same workspace.

List the shared windows:

```bash
tmux -S /shared/tmux/oracle-forge.sock list-windows -t oracle-forge-gpt5
```

Attach to the live team session:

```bash
tmux -S /shared/tmux/oracle-forge.sock attach -t oracle-forge-gpt5
```

## 4. What You Should See

The shared session should contain these windows:

```text
1: dab
2: agent
3: toolbox
```

What each window is for:

- `dab`
  The shared DataAgentBench workspace at `/shared/DataAgentBench`
- `agent`
  The Oracle Forge workspace at `/shared/DataAgentBench/oracle_forge_v3`
- `toolbox`
  The MCP Toolbox server running with the shared config

## 4.1 Switching Windows

To switch between windows after you've attached to the shared session (interactive):

```text
Prefix (default: Ctrl-b), then press the window number (1, 2, 3)
Prefix then `n` : next window
Prefix then `p` : previous window
Prefix then `w` : show window list and choose
```

If you need to switch the active window from another shell (without attaching), use:

```bash
tmux -S /shared/tmux/oracle-forge.sock select-window -t oracle-forge-gpt5:1
# or by window name
tmux -S /shared/tmux/oracle-forge.sock select-window -t oracle-forge-gpt5:agent
```

If your tmux prefix has been changed from the default, substitute that key for `Ctrl-b` above.

## 5. Quick Validation Commands

Inside the server, validate the tmux session:

```bash
tmux -S /shared/tmux/oracle-forge.sock list-windows -t oracle-forge-gpt5
```

Validate the Oracle Forge benchmark starter query:

```bash
cd /shared/DataAgentBench/oracle_forge_v3
source .venv/bin/activate
python3 run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer
```

## 6. Execution Patterns

### Running the Agent
To run a specific benchmark query with the official configuration:
```bash
python3 run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer
```

### Running the Benchmark Harness
To run the full validation harness for a specific query:
```bash
python3 eval/run_benchmark.py --agent src.agent.orchestrator --trials 5 --output results/demo_yelp_q1_trials5.json --datasets yelp --query-ids 1 --remote-host localhost --remote-dab-path /shared/DataAgentBench
```

### Key Parameters
- `--dataset`: dataset family name such as `yelp` or `crmarenapro`
- `--query-id`: the specific query number from the dataset
- `--validate-answer`: compare the agent output against the benchmark validator

## 7. Important Team Rule

Do not use `exit` inside shared tmux windows unless you intentionally want to close that window.

If you only want to stop a running command, use:

```bash
Ctrl-C
```

If the `agent` window disappears, it usually means the shell in that window exited.

## 7. Team Attach Shortcut

For most teammates, the full flow is now:

```bash
ssh trp-gpt5
tmux -S /shared/tmux/oracle-forge.sock attach -t oracle-forge-gpt5
```
