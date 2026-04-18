# Engagement Log
Track public posts, links, dates, and any measurable response.

## 2026-04-08
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — Week 8 kickoff
Update:
- Shipped: challenge document studied, team roles assigned, GitHub repo created
- Stuck: tenai-infra server access pending auth key distribution
- Next: each member reads DAB paper and Claude Code architecture docs before Day 2 mob
Response: Internal only
Follow-up needed: None

## 2026-04-09
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — infrastructure setup and architecture study
Update:
- Shipped: tenai-infra running, Tailscale mesh verified for all team devices, DAB repository cloned
- Stuck: DuckDB not exposing natively through Toolbox in current environment
- Next: finalize architecture design and draft Inception document before mob session approval
Response: Internal only
Follow-up needed: None

## 2026-04-10
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — architecture design and KB structure
Update:
- Shipped: V3 architecture finalized (8-component design drawing from Claude Code + OpenAI data agent + Anton), KB directory structure created
- Stuck: KB injection test protocol needs to be agreed before committing any documents
- Next: Sprint 1 Inception document drafted for mob session approval tomorrow
Response: Internal only
Follow-up needed: None

## 2026-04-11
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — Sprint 1 inception approved, KB v1 committed, first benchmark pass
Update:
- Shipped: Sprint 1 Inception approved at mob session, KB v1 (architecture) and KB v2 (domain) committed with injection tests PASS, Yelp query 1 remote DAB validation returned is_valid: true
- Stuck: Toolbox-native DuckDB not yet available, using remote DAB adapter as authoritative path
- Next: run targeted benchmark reruns on q2, q3, q6 — all three identified as highest-leverage failures
Response: Internal only
Follow-up needed: None

## 2026-04-12
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — targeted benchmark reruns, all three failing
Update:
- Shipped: targeted reruns on q2, q3, q6 — failure modes identified and documented in probes/probes.md
- Stuck: q2 correct state (PA) but wrong average (3.68 vs expected 3.699); q3 integer count not emitting; q6 category resolving to Unknown
- Next: fix q2 aggregation semantics, q3 answer format, q6 category extraction
Response: Internal only
Follow-up needed: None

## 2026-04-13
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — q3 and q6 now passing, baseline harness artifact committed
Update:
- Shipped: q3 is_valid: true, q6 is_valid: true after branch sync and fixes; initial_baseline_with_trace.json committed to results/
- Stuck: q2 still blocked — value 3.76 vs validator expectation 3.699, averaging semantics unclear
- Next: Signal Corps posts go live April 14, KB v3 corrections log to be expanded
Response: Internal only
Follow-up needed: None

## 2026-04-14
Platform: X (Twitter)
Link: https://x.com/LidyaDagnew/status/2044044847699329443?s=20
Topic: Multi-database context engineering for AI data agents — what we learned building against DataAgentBench
Evidence referenced: Claude Code 3-layer memory architecture, DAB benchmark 38% ceiling, ill-formatted join keys problem
Response: [to be updated]
Follow-up needed: Tag @ucbepic if they respond

## 2026-04-14
Platform: Reddit — r/LocalLLaMA
Link: https://www.reddit.com/r/LocalLLaMA/comments/1slh1ce/were_building_against_dataagentbench_uc_berkeley/
Topic: Post about why multi-database AI agents are harder than they look
Evidence referenced: DAB benchmark four hard requirements, ill-formatted join keys, 38% ceiling
Response: [to be updated]
Follow-up needed: Monitor for replies

## 2026-04-14
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — shipped/stuck/next
Response: Internal only
Follow-up needed: None

## 2026-04-18
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — Yelp 50-trial regression sweep completed
Update:
- Shipped: remote-local DAB benchmark sweep passed for Yelp q1 through q7 at 50 trials each; local copies of the result JSONs and remote execution logs were synced back into the workspace
- Stuck: none on the Yelp path; remaining work is to repeat the same evidence capture for the other dataset families
- Next: run the remaining one-query smoke tests per dataset family and keep the score log / KB changelogs in sync
Response: Internal only
Follow-up needed: None

## 2026-04-18
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — CRM q8 through q13 verified live
Update:
- Shipped: CRM q8 through q13 were re-verified on the remote-local path after the KB-first cleanup; the score log and KB changelogs were updated to reflect the full q1 through q13 family completion
- Stuck: none on the CRM path; remaining documentation work is to keep the probe library and submission notes aligned with the same live outputs
- Next: continue with any remaining dataset families and keep the evidence trail consistent across probes, KB, and evaluation logs
Response: Internal only
Follow-up needed: None


## 2026-04-14
Platform: LinkedIn
Link: https://www.linkedin.com/posts/eyobed-feleke_agenticai-bigdata-aws-activity-7449744022258163712-T131
Author: Eyobed Feleke
Topic: Oracle Forge — building a production AI data agent
Response: [to be updated]
Follow-up needed: None


## 2026-04-14
Platform: LinkedIn
Link: https://www.linkedin.com/feed/update/urn:li:share:7449902416717742080/
Author: Bethelhem Abay
Topic: The Best AI Model Only Scores 38% on a Data Benchmark. 
       Here's Why That Number Changed How I Think About AI.
Response: [to be updated]
Follow-up needed: Monitor for comments and reactions

## 2026-04-14
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — MCP config changed from file-based 
       to live DB connections, data seeding completed
Response: Internal only
Follow-up needed: Gersum to fix db permissions
