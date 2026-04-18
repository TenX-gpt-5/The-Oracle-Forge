# Submission Results

This folder holds the compact benchmark evidence that backs the final submission payload.

Files:
- `yelp_50_trial_summary.json`: one-line per-query summary derived from the verified Yelp `50`-trial runs
- `github_repos_q3_q4_trial1_summary.json`: compact smoke summary for the verified GitHub Repos `q3` and `q4` runs
- `github_repos_status.json`: strict-mode GitHub Repos status snapshot (`3/4 confirmed`)

The full per-query trial artifacts remain in the top-level `results/` directory.

When the completed 54-query benchmark artifact is available, flatten it with:

```bash
python3 eval/prepare_leaderboard_submission.py --input results/<full_benchmark>.json --output submission/team_gpt5_results.json
```

If you are combining multiple completed family-level runs into one consolidated file, merge them with:

```bash
python3 eval/merge_leaderboard_submissions.py --inputs submission/team_gpt5_yelp_50t.json submission/team_gpt5_crmarenapro_50t.json submission/team_gpt5_github_repos_q1_50t.json --output submission/gpt-5_result.json
```

For CRM `50`-trial evidence, the exact remote command used was:

```bash
ssh gersum@100.101.234.123 "cd /shared/DataAgentBench/oracle_forge_v3 && tmux new-session -d -s gpt5_crm_50t 'source .venv/bin/activate && python3 eval/run_benchmark.py --agent src.agent.orchestrator --trials 50 --output results/gpt5_crmarenapro_50t.json --datasets crmarenapro --remote-host localhost --remote-dab-path /shared/DataAgentBench > results/gpt5_crmarenapro_50t.log 2>&1' && tmux list-sessions"
```
