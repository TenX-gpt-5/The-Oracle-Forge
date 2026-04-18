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


## 2026-04-15
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — Week 9 Day 1
Update:
- Shipped: Yelp q1–q7 full validation sweep confirmed on remote host, 50 trials each, all passing. Score logs synced back to local workspace.
- Stuck: Broader dataset coverage beyond Yelp still pending — stockindex and bookreview hitting dependency issues.
- Next: KB changelogs updated to reflect real benchmark evidence, probe library updated with post-fix outcomes.
Response: Internal only
Follow-up needed: None

## 2026-04-16
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — Week 9 Day 2
Update:
- Shipped: KB changelogs aligned with benchmark evidence. Architecture, domain, corrections, and evaluation logs all pointing to same verified Yelp result. Probe library updated — Yelp probes now reflect actual post-fix pass outcomes.
- Stuck: Server intermittently down during iteration runs — team monitoring.
- Next: Signal Corps Week 9 benchmark posts to be drafted. Final engagement portfolio compilation begins.
Response: Internal only
Follow-up needed: None

## 2026-04-17
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — Week 9 Day 3
Update:
- Shipped: Benchmark evidence and KB alignment confirmed in sync. Complete Yelp q1–q7 record with structured score logs, exact commands, and outcomes committed. Adversarial probe library reflects actual repaired behaviors.
- Stuck: Submission portal not yet open — waiting on facilitator announcement.
- Next: Demo video preparation. Signal Corps benchmark posts go live April 18.
Response: Internal only
Follow-up needed: None

## 2026-04-18
Platform: Slack (internal)
Link: #oracle-forge-gpt5
Topic: Daily update — Week 9 Day 4 (submission day)
Update:
- Shipped: Signal Corps Week 9 posts published on LinkedIn and X with live benchmark evidence. Full Yelp run confirmed pass@1=1.0 on 7 queries, DAB official validator verified. Demo video recorded.
- Stuck: Reddit posting blocked — platform issue.
- Next: Final submission by 21:00 UTC — GitHub repo, final report, demo video.
Response: Internal only
Follow-up needed: None

## 2026-04-18
Platform: LinkedIn
Link: https://www.linkedin.com/posts/bethelhem-abay-melaku-618192205_dataagentbench-aiagents-contextengineering-share-7451284760624173056-_12o
Author: Bethelhem Abay
Topic: Oracle Forge v3 benchmark closure — pass@1=1.0 on Yelp, 7 queries, 50 trials each. KB architecture as the unlock between 38% (DAB best) and 100% on validated dataset.
Evidence referenced: Live benchmark run results, DAB official validator confirmation, three KB layers explanation.
Response: Monitoring
Follow-up needed: Monitor for comments and reactions

## 2026-04-18
Platform: X (Twitter)
Link: https://x.com/carinobetty22/status/2045525170090062203
Author: Bethelhem Abay
Topic: Benchmark results post — Yelp pass@1=1.0, KB architecture explanation.
Response: Monitoring
Follow-up needed: None

## 2026-04-14
Platform: X (Twitter)
Link: https://x.com/carinobetty22/status/2044150321446895815
Author: Bethelhem Abay
Topic: Context engineering thread — three KB layers, DAB 38% ceiling argument.
Response: Monitoring
Follow-up needed: None

## 2026-04-14
Platform: X (Twitter)
Link: https://x.com/carinobetty22/status/2044149854306185464
Author: Bethelhem Abay
Topic: Oracle Forge architecture thread — multi-database routing and self-repair loop.
Response: Monitoring
Follow-up needed: None

---

## Final Engagement Portfolio — April 18, 2026

### All Posts with Links

| Date | Platform | Author | Link | Topic |
|---|---|---|---|---|
| Apr 14 | X (Twitter) | Lidya Dagnew | https://x.com/LidyaDagnew/status/2044044847699329443 | 3-layer context engineering, DAB 38% ceiling |
| Apr 14 | X (Twitter) | Bethelhem Abay | https://x.com/carinobetty22/status/2044150321446895815 | Context engineering thread |
| Apr 14 | X (Twitter) | Bethelhem Abay | https://x.com/carinobetty22/status/2044149854306185464 | Oracle Forge architecture |
| Apr 14 | Reddit | Bethelhem Abay | https://www.reddit.com/r/LocalLLaMA/comments/1slh1ce/were_building_against_dataagentbench_uc_berkeley/ | DAB four hard requirements, join key failures |
| Apr 14 | LinkedIn | Eyobed Feleke | https://www.linkedin.com/posts/eyobed-feleke_agenticai-bigdata-aws-activity-7449744022258163712-T131 | Oracle Forge system design |
| Apr 14 | LinkedIn | Bethelhem Abay | https://www.linkedin.com/feed/update/urn:li:share:7449902416717742080/ | 38% benchmark ceiling, architecture gap argument |
| Apr 18 | LinkedIn | Bethelhem Abay | https://www.linkedin.com/posts/bethelhem-abay-melaku-618192205_dataagentbench-aiagents-contextengineering-share-7451284760624173056-_12o | Benchmark closure — pass@1=1.0 |
| Apr 18 | X (Twitter) | Bethelhem Abay | https://x.com/carinobetty22/status/2045525170090062203 | Benchmark results |

### Community Intelligence That Changed Team's Technical Approach

1. **Join key normalization reprioritized** — DAB GitHub Issues surfaced that the dominant practitioner failure is integer vs prefixed-string ID mismatches, not routing. This changed the Reddit post framing and reprioritized the adversarial probe library toward join key failure cases.

2. **Trino MCP server discovered** — Trino Slack community reply from practitioner François AUTAA surfaced a working MCP server (github.com/tuannvm/mcp-trino) active for 3 months. Flagged to Intelligence Officers for Week 9 architecture planning — direct input into broader dataset coverage strategy.

3. **Claude Code autoDream pattern** — Architecture leak surfaced the corrections log consolidation pattern. Directly shaped KB v3 design — our corrections log is the autoDream equivalent.

### Metrics
Reach metrics across all platforms to be updated as available. As of submission date, posts are live and indexed. Community monitoring ongoing.
