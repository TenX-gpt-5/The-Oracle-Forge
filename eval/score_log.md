# Score Log

Track score progression from first baseline to final submission.

## 2026-04-11
Run type: Remote DAB smoke validation
Command: `python run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer`
Dataset scope: `yelp` query 1
Iterations: not recorded
Trials: 1
Score: pass (`is_valid: true`)
Notes: Final answer `3.55`; validator reason `Found matching number: 3.55 ~= 3.55`; reported benchmark evidence includes `benchmark_reviews=117`.

## 2026-04-12
Run type: Team `gpt-5` targeted smoke rerun (highest-leverage failures)
Command:
- `python3 run_benchmark_query.py --dataset yelp --query-id 2 --validate-answer`
- `python3 run_benchmark_query.py --dataset yelp --query-id 3 --validate-answer`
- `python3 run_benchmark_query.py --dataset yelp --query-id 6 --validate-answer`
Dataset scope: `yelp` queries 2, 3, 6
Iterations: not recorded
Trials: 1 per query
Score: `0/3` pass (`is_valid: false` for all three)
Notes:
- q2 (`timestamp: 20260412_000838`): output `PA, 3.68`; validator reason `Number near 'PA' does not match ≈3.699395770392749`.
- q3 (latest rerun): validator reason `Number 35 not found in LLM output.`
- q6 (`timestamp: 20260412_000840`): output includes `Coffee House Too Cafe` but categories resolve to `Unknown`; validator reason `Missing category: restaurants`.

## 2026-04-13
Run type: Remote DAB rerun after branch sync and cleanup
Command:
- `python run_benchmark_query.py --dataset yelp --query-id 2 --validate-answer`
- `python run_benchmark_query.py --dataset yelp --query-id 3 --validate-answer`
- `python run_benchmark_query.py --dataset yelp --query-id 6 --validate-answer`
Dataset scope: `yelp` queries 2, 3, 6
Iterations: not recorded
Trials: 1 per query
Score: `2/3` pass (`is_valid: true` for q3 and q6)
Notes:
- q2: output `PA, 3.76`; validator reason `Number near 'PA' does not match ≈3.699395770392749`.
- q3 (`timestamp: 20260413_065351`): validator `is_valid: true`; reason `Found number: 35`.
- q6 (`timestamp: 20260413_065358`): validator `is_valid: true`; reason `Name and all categories are present.`

## 2026-04-13
Run type: Compliance refresh rerun + baseline harness artifact
Command:
- `python eval/run_initial_baseline.py`
- `python run_benchmark_query.py --dataset yelp --query-id 2 --validate-answer`
- `python run_benchmark_query.py --dataset yelp --query-id 3 --validate-answer`
- `python run_benchmark_query.py --dataset yelp --query-id 6 --validate-answer`
Dataset scope: held-out baseline (local harness) + `yelp` queries 2, 3, 6 (remote DAB)
Iterations: baseline harness uses 3 local trials; remote runs not recorded
Trials: baseline 3 local trials + 1 per remote query
Score:
- baseline harness: `pass_at_1=0.0`, `total_trials=3`, `failure_classes={"extraction_failure": 3}`
- remote DAB targeted rerun: `2/3` pass (`is_valid: true` for q3 and q6)
Notes:
- baseline artifact written to `results/initial_baseline_with_trace.json` and includes query trace payloads per trial.
- q2 remains the only blocking query (`benchmark_answer=3.76`, expected near `3.699395770392749`).
- q3 (`timestamp: 20260413_072739`): `is_valid: true`.
- q6 (`timestamp: 20260413_072745`): `is_valid: true`.

## 2026-04-13
Run type: Final Yelp smoke pass after q7 fix
Command:
- `python run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer`
- `python run_benchmark_query.py --dataset yelp --query-id 2 --validate-answer`
- `python run_benchmark_query.py --dataset yelp --query-id 3 --validate-answer`
- `python run_benchmark_query.py --dataset yelp --query-id 4 --validate-answer`
- `python run_benchmark_query.py --dataset yelp --query-id 5 --validate-answer`
- `python run_benchmark_query.py --dataset yelp --query-id 6 --validate-answer`
- `python run_benchmark_query.py --dataset yelp --query-id 7 --validate-answer`
Dataset scope: `yelp` queries 1 through 7
Iterations: not recorded
Trials: 1 per query
Score: `7/7` pass (`is_valid: true` for all seven queries)
Notes:
- q1: `3.55` average rating in Indianapolis validated successfully.
- q2: `PA, 3.70` validated successfully after the state-average fix.
- q3: `35` validated successfully after the 2018 parking count fix.
- q4: `Restaurant, 3.63` validated successfully.
- q5: `PA, 3.48` validated successfully.
- q6: `Coffee House Too Cafe` with `Restaurants, Breakfast & Brunch, American (New), Cafes` validated successfully.
- q7: `Restaurants, Food, American (New), Shopping, Breakfast & Brunch` validated successfully after the q7 parser fallback and shared-server sync.

## 2026-04-17
Run type: Remote-local DAB regression on standalone local DBs
Command:
- `REMOTE_SANDBOX_HOST=127.0.0.1 REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer`
- `REMOTE_SANDBOX_HOST=127.0.0.1 REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset yelp --query-id 2 --validate-answer`
- `REMOTE_SANDBOX_HOST=127.0.0.1 REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset yelp --query-id 3 --validate-answer`
- `REMOTE_SANDBOX_HOST=127.0.0.1 REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset yelp --query-id 4 --validate-answer`
- `REMOTE_SANDBOX_HOST=127.0.0.1 REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset yelp --query-id 5 --validate-answer`
- `REMOTE_SANDBOX_HOST=127.0.0.1 REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset yelp --query-id 6 --validate-answer`
- `REMOTE_SANDBOX_HOST=127.0.0.1 REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset yelp --query-id 7 --validate-answer`
Dataset scope: `yelp` queries 1 through 7
Iterations: `1` per query
Trials: 1 per query
Score: `7/7` pass (`is_valid: true` for all seven queries)
Notes:
- q1: `3.55` average rating in Indianapolis validated successfully.
- q2: `PA, 3.70` validated successfully after the state-average fix on the local standalone DB path.
- q3: `35` validated successfully.
- q4: `Restaurant, 3.63` validated successfully.
- q5: `PA, 3.48` validated successfully.
- q6: `Coffee House Too Cafe` with `Restaurants, Breakfast & Brunch, American (New), Cafes` validated successfully.
- q7: `Restaurants, Food, American (New), Shopping, Breakfast & Brunch` validated successfully.

## 2026-04-17
Run type: Single-query smoke run with longer internal loop
Command: `REMOTE_SANDBOX_HOST=127.0.0.1 REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_agent.py --dataset yelp --query_id 1 --llm google/gemini-2.0-flash-001 --iterations 20 --root_name q1_iter20`
Dataset scope: `yelp` query 1
Iterations: `20`
Trials: 1
Score: pass
Notes: Final answer `3.55`; matched the benchmark-accepted Indianapolis average. This was a single-query agent smoke run, not a full DAB validator invocation.

## 2026-04-18
Run type: Yelp 50-trial benchmark sweep
Command: `ssh gersum@100.101.234.123 "cd /shared/DataAgentBench/oracle_forge_v3 && source .venv/bin/activate && python3 eval/run_benchmark.py --agent src.agent.orchestrator --trials 50 --output results/gpt5_yelp_q1_trials50.json --datasets yelp --query-ids 1 --remote-host localhost --remote-dab-path /shared/DataAgentBench"`
Dataset scope: `yelp` query 1
Iterations: not recorded
Trials: 50
Score: pass (`pass_at_1=1.0`, `trial_pass_rate=1.0`)
Notes: Final answer `3.55`; remote scorer reported `passed_queries=1` and `passed_trials=50`.

## 2026-04-18
Run type: Yelp 50-trial benchmark sweep
Command: `ssh gersum@100.101.234.123 "cd /shared/DataAgentBench/oracle_forge_v3 && source .venv/bin/activate && python3 eval/run_benchmark.py --agent src.agent.orchestrator --trials 50 --output results/gpt5_yelp_q2_trials50.json --datasets yelp --query-ids 2 --remote-host localhost --remote-dab-path /shared/DataAgentBench"`
Dataset scope: `yelp` query 2
Iterations: not recorded
Trials: 50
Score: pass (`pass_at_1=1.0`, `trial_pass_rate=1.0`)
Notes: Final answer `PA, 3.70`; remote scorer reported `passed_queries=1` and `passed_trials=50`.

## 2026-04-18
Run type: Yelp 50-trial benchmark sweep
Command: `ssh gersum@100.101.234.123 "cd /shared/DataAgentBench/oracle_forge_v3 && source .venv/bin/activate && python3 eval/run_benchmark.py --agent src.agent.orchestrator --trials 50 --output results/gpt5_yelp_q3_trials50.json --datasets yelp --query-ids 3 --remote-host localhost --remote-dab-path /shared/DataAgentBench"`
Dataset scope: `yelp` query 3
Iterations: not recorded
Trials: 50
Score: pass (`pass_at_1=1.0`, `trial_pass_rate=1.0`)
Notes: Final answer `35`; remote scorer reported `passed_queries=1` and `passed_trials=50`.

## 2026-04-18
Run type: Yelp 50-trial benchmark sweep
Command: `ssh gersum@100.101.234.123 "cd /shared/DataAgentBench/oracle_forge_v3 && source .venv/bin/activate && python3 eval/run_benchmark.py --agent src.agent.orchestrator --trials 50 --output results/gpt5_yelp_q4_trials50.json --datasets yelp --query-ids 4 --remote-host localhost --remote-dab-path /shared/DataAgentBench"`
Dataset scope: `yelp` query 4
Iterations: not recorded
Trials: 50
Score: pass (`pass_at_1=1.0`, `trial_pass_rate=1.0`)
Notes: Final answer `Restaurant, 3.63`; remote scorer reported `passed_queries=1` and `passed_trials=50`.

## 2026-04-18
Run type: Yelp 50-trial benchmark sweep
Command: `ssh gersum@100.101.234.123 "cd /shared/DataAgentBench/oracle_forge_v3 && source .venv/bin/activate && python3 eval/run_benchmark.py --agent src.agent.orchestrator --trials 50 --output results/gpt5_yelp_q5_trials50.json --datasets yelp --query-ids 5 --remote-host localhost --remote-dab-path /shared/DataAgentBench"`
Dataset scope: `yelp` query 5
Iterations: not recorded
Trials: 50
Score: pass (`pass_at_1=1.0`, `trial_pass_rate=1.0`)
Notes: Final answer `PA, 3.48`; remote scorer reported `passed_queries=1` and `passed_trials=50`.

## 2026-04-18
Run type: Yelp 50-trial benchmark sweep
Command: `ssh gersum@100.101.234.123 "cd /shared/DataAgentBench/oracle_forge_v3 && source .venv/bin/activate && python3 eval/run_benchmark.py --agent src.agent.orchestrator --trials 50 --output results/gpt5_yelp_q6_trials50.json --datasets yelp --query-ids 6 --remote-host localhost --remote-dab-path /shared/DataAgentBench"`
Dataset scope: `yelp` query 6
Iterations: not recorded
Trials: 50
Score: pass (`pass_at_1=1.0`, `trial_pass_rate=1.0`)
Notes: Final answer `Coffee House Too Cafe, Restaurants, Breakfast & Brunch, American (New), Cafes`; remote scorer reported `passed_queries=1` and `passed_trials=50`.

## 2026-04-18
Run type: Yelp 50-trial benchmark sweep
Command: `ssh gersum@100.101.234.123 "cd /shared/DataAgentBench/oracle_forge_v3 && source .venv/bin/activate && python3 eval/run_benchmark.py --agent src.agent.orchestrator --trials 50 --output results/gpt5_yelp_q7_trials50.json --datasets yelp --query-ids 7 --remote-host localhost --remote-dab-path /shared/DataAgentBench"`
Dataset scope: `yelp` query 7
Iterations: not recorded
Trials: 50
Score: pass (`pass_at_1=1.0`, `trial_pass_rate=1.0`)
Notes: Final answer `Restaurants, Food, American (New), Shopping, Breakfast & Brunch`; remote scorer reported `passed_queries=1` and `passed_trials=50`.

## 2026-04-18
Run type: GitHub Repos single-trial benchmark smoke
Command: `ssh gersum@100.101.234.123 "cd /shared/DataAgentBench/oracle_forge_v3 && source .venv/bin/activate && REMOTE_SANDBOX_ENABLED=true REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 timeout 240 python3 eval/run_benchmark.py --agent src.agent.orchestrator --trials 1 --output results/gpt5_github_repos_q3_trial1.json --datasets GITHUB_REPOS --query-ids 3 --remote-host localhost --remote-dab-path /shared/DataAgentBench"`
Dataset scope: `GITHUB_REPOS` query 3
Iterations: not recorded
Trials: 1
Score: pass (`pass_at_1=1.0`, `trial_pass_rate=1.0`)
Notes: Final answer `1077`; remote scorer reported `passed_queries=1` and `passed_trials=1`.

## 2026-04-18
Run type: GitHub Repos single-trial benchmark smoke
Command: `ssh gersum@100.101.234.123 "cd /shared/DataAgentBench/oracle_forge_v3 && source .venv/bin/activate && REMOTE_SANDBOX_ENABLED=true REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 timeout 240 python3 eval/run_benchmark.py --agent src.agent.orchestrator --trials 1 --output results/gpt5_github_repos_q4_trial1.json --datasets GITHUB_REPOS --query-ids 4 --remote-host localhost --remote-dab-path /shared/DataAgentBench"`
Dataset scope: `GITHUB_REPOS` query 4
Iterations: not recorded
Trials: 1
Score: pass (`pass_at_1=1.0`, `trial_pass_rate=1.0`)
Notes: Final answer `Top five repositories by commit count among non-Python main-language GitHub repos are: apple/swift, twbs/bootstrap, Microsoft/vscode, facebook/react, tensorflow/tensorflow.`; remote scorer reported `passed_queries=1` and `passed_trials=1`.

## 2026-04-18
Run type: GitHub Repos 50-trial rerun interrupted
Command: `ssh gersum@100.101.234.123 "cd /shared/DataAgentBench/oracle_forge_v3 && source .venv/bin/activate && REMOTE_SANDBOX_ENABLED=true REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 timeout 7200 python3 eval/run_benchmark.py --agent src.agent.orchestrator --trials 50 --output results/gpt5_github_repos_q2_q4_trials50.json --datasets GITHUB_REPOS --query-ids 2,3,4 --remote-host localhost --remote-dab-path /shared/DataAgentBench"`
Dataset scope: `GITHUB_REPOS` queries 2, 3, and 4
Iterations: not recorded
Trials: 50
Score: interrupted
Notes: Detached tmux run launched on the team host, but SSH connectivity dropped before a result JSON was written. No benchmark artifact was produced for this rerun.

## 2026-04-18
Run type: CRM live verification sweep
Command:
- `REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset crmarenapro --query-id 8 --validate-answer`
- `REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset crmarenapro --query-id 9 --validate-answer`
- `REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset crmarenapro --query-id 10 --validate-answer`
- `REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset crmarenapro --query-id 11 --validate-answer`
- `REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset crmarenapro --query-id 12 --validate-answer`
- `REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset crmarenapro --query-id 13 --validate-answer`
Dataset scope: `crmarenapro` queries 8 through 13
Iterations: not recorded
Trials: 1 per query
Score: `6/6` pass (`is_valid: true` for all six queries)
Notes:
- q8: `005Wt000003NIliIAG`
- q9: `MI`
- q10: `005Wt000003NDqDIAW`
- q11: `01tWt000006hV8LIAU`
- q12: `005Wt000003NDEBIA4`
- q13: `005Wt000003NIXCIA4`

## 2026-04-18
Run type: CRM live verification sweep
Command:
- `REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset crmarenapro --query-id 1 --validate-answer`
- `REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset crmarenapro --query-id 2 --validate-answer`
- `REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset crmarenapro --query-id 3 --validate-answer`
- `REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset crmarenapro --query-id 4 --validate-answer`
- `REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset crmarenapro --query-id 5 --validate-answer`
- `REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset crmarenapro --query-id 6 --validate-answer`
- `REMOTE_SANDBOX_HOST=localhost REMOTE_SANDBOX_PYTHON=/usr/bin/python3 python3 run_benchmark_query.py --dataset crmarenapro --query-id 7 --validate-answer`
Dataset scope: `crmarenapro` queries 1 through 7
Iterations: not recorded
Trials: 1 per query
Score: `7/7` pass (`is_valid: true` for all seven queries)
Notes:
- q1: `Authority`
- q2: `ka0Wt000000Eq0MIAS`
- q3: `Negotiation`
- q4: `November`
- q5: `a03Wt00000JqnHwIAJ`
- q6: `ka0Wt000000EnwvIAC`
- q7: `ka0Wt000000EoD3IAK`

## 2026-04-18
Run type: Final family status snapshot
Scope: BookReview, AGNEWS, DEPS_DEV_V1, GitHub Repos, GEO, MUSIC, PANCANCERATLAS, PATENTS, STOCK_INDEX, STOCK_MARKET, Yelp, CRM
Notes:
- BookReview: all passed
- AGNEWS: partial, `2/4` passing at `50` trials
- DEPS_DEV_V1: not passed
- GitHub Repos: not passed overall
- GEO: all passed
- MUSIC: not passed
- PANCANCERATLAS: not passed
- PATENTS: not passed
- STOCK_INDEX: not passed
- STOCK_MARKET: not passed
- Yelp: all passed
- CRM: all passed (`13/13`, `50` trials each)
- Official benchmark artifact remains `results/dab_results.json` with `pass@1 = 0.42` and `pass@10 = 0.58`
