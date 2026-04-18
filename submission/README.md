# Submission Package

This folder packages the benchmark evidence and report artifacts for Oracle Forge v3.

Contents:
- `final_report.md`: source for the final PDF report
- `final_report.pdf`: rendered submission report
- `AGENT.md`: submission-facing architecture summary
- `results/PR_LINK.md`: PR metadata and submission notes for the DataAgentBench PR
- `team_gpt5_results.json`: compact manifest of the verified Yelp benchmark runs
- `team_gpt5_crmarenapro_50t.json`: flattened CRM leaderboard submission artifact (`13` queries, `50` trials each)
- `gpt-5_result.json`: consolidated flattened leaderboard payload for the completed `50`-trial families currently verified in this workspace
- `results/`: supporting run summaries and per-query references
- GitHub Repos smoke evidence is tracked separately under `results/github_repos_q3_q4_trial1_summary.json`
- GitHub Repos strict-mode status is tracked separately under `results/github_repos_status.json`

The verified benchmark evidence in this package covers:
- `yelp` queries `q1` through `q7`
- `50` trials per query
- `pass_at_1 = 1.0` and `trial_pass_rate = 1.0` for every query

GitHub Repos is currently in strict-mode progress:
- `q2`, `q3`, and `q4` are confirmed
- `q1` is the remaining query under the 50-trial rerun

CRM is fully verified in the live remote-local path:
- `q1` through `q13` pass
- the 50-trial flattened leaderboard artifact is `team_gpt5_crmarenapro_50t.json`

Other families are tracked honestly in the repository but are not all fully passed:
- `BookReview` and `GEO` are passed
- `DEPS_DEV_V1` is passed
- `AGNEWS` is partial at `2/4`
- `GitHub Repos`, `MUSIC`, `PANCANCERATLAS`, `PATENTS`, `STOCK_INDEX`, and `STOCK_MARKET` remain not passed

The consolidated `gpt-5_result.json` currently combines the completed 50-trial families we have in hand:
- Yelp `q1` through `q7`
- CRM `q1` through `q13`
- GitHub Repos `q1`
