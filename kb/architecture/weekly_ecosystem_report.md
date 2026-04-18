# Weekly Global Ecosystem Report — Week 8

**Prepared by:** Intelligence Officers (Samuel Lachisa, Kidane Gebremedhin)  
**Date:** April 14, 2026  
**Presented at:** Monday mob session  
**Sources:** DAB paper (arxiv.org/abs/2603.20576), DAB GitHub (github.com/ucbepic/DataAgentBench), OpenAI blog (openai.com/index/inside-our-in-house-data-agent), Claude Code source (github.com/sanbuphy/claude-code-source-code)

---

## 1. DataAgentBench — Actual Benchmark Results from the Paper

The DAB paper evaluated **5 frontier LLMs** using a ReAct agent baseline. All scores below are **pass@1** (first-attempt accuracy) from the published results (Section 3.2.1, Figure 4):

| Model | pass@1 | pass@50 | Total API Cost (USD) |
|-------|--------|---------|---------------------|
| **Gemini-3-Pro** | **38%** | 69% | $1,355 |
| GPT-5-mini | 30% | 59% | $67 |
| GPT-5.2 | 25% | 51% | — |
| Kimi-K2 | 23% | 56% | $1,304 |
| Gemini-2.5-Flash | 9% | 40% | — |

**PromptQL case study** (Section 3.4): Using Claude-Opus-4.6 as backbone, PromptQL agent achieved **51% pass@1** vs. 44% for the ReAct baseline — a **7 percentage-point improvement**. PromptQL's gains came from datasets where finding the right tables was the bottleneck: yelp (+40pp), agnews (+35pp), stockindex (+34pp), stockmarket (+20pp, which has 2,754 tables). Both agents scored **0% on patents** — bulk unstructured text extraction remains unsolved.

**Critical finding:** GPT-5-mini **outperforms** GPT-5.2 despite being smaller and cheaper ($67 vs. 20× more), proving model scale alone does not determine agent performance. Cost-efficient SQL aggregation (2.6:1 DB-to-Python ratio) beats expensive Python post-processing (Kimi-K2's 1.1:1 ratio).

---

## 2. DAB Failure Taxonomy — Actual Numbers from the Paper

The paper defines **5 failure modes** (FM1–FM5), classified across **1,147 annotated failed trajectories** (Section 3.3):

| Failure Mode | Description | Frequency |
|---|---|---|
| **FM4: Incorrect implementation** | Correct plan, correct data, wrong execution (bad regex, wrong aggregation) | **45%** |
| **FM2: Incorrect plan** | Wrong logical structure (e.g., averaging averages instead of averaging raw data) | **40%** |
| FM3: Incorrect data selection | Right plan, wrong tables/columns | 15% |
| FM1: No tool call | Agent returns None or refuses to attempt | Varies (63.4% for Gemini-2.5-Flash, 0% for GPT models) |
| FM5: Runtime error | API failures, timeouts, token limits | 6.6% (Kimi-K2), negligible for others |

**Key takeaway from the paper:** "Agents typically select the right data, but fail at planning the computation or implementing it correctly." FM2 + FM4 account for **85% of incorrect answers**.

**Regex is the universal failure pattern:** Every evaluated agent uses regex for text extraction. None attempts NLP-based parsing (dateutil.parser), NER, or LLM-based extraction. This explains:
- **0% pass@1 on patents** — varied natural-language date formats ("dated 5th March 2019", "March the 18th, 2019") defeat all regex patterns
- **pancancer_atlas** — regex for "MALE" matches inside "FEMALE", causing gender misclassification  
- **bookreview** — year-extraction regex matches ISBN segments instead of publication years

**Paper's recommendation:** "Exposing dedicated extraction tools — such as date parsers, NER taggers, or LLM-based extraction operators — alongside SQL and Python execution would address the hardest unsolved queries in DAB."

---

## 3. DAB Query Statistics — What Each Hard Requirement Actually Covers

From Section 2.3.1 of the paper, exact counts:

| Property | Queries affected (of 54) | Specific examples from paper |
|---|---|---|
| Multi-database integration | **All 54** | `crmarenapro` spans up to 6 databases across 3 systems (DuckDB, PostgreSQL, SQLite); `stockmarket` has 2,754 tables |
| Ill-formatted join keys | **26** | `bookreview`/`yelp`: `bid_123` vs `bref_123`; `crmarenapro`: 25% of IDs have random trailing spaces ("Lead123" vs "Lead123 "); `stockindex`: semantic mapping ("Tokyo Stock Exchange" → "N225") |
| Unstructured text transformation | **47** | 31 data-independent (fixed patterns); 16 data-dependent (requires inference — `agnews` article classification, `music_brainz_20k` entity resolution, `crmarenapro` CRM relationship inference) |
| Domain knowledge | **30** | CRM/sales ops (`crmarenapro`), genomics (`pancancer_atlas`), IP law (`patents`), finance (`stockindex`/`stockmarket`), software engineering (`github_repos`/`deps_dev_v1`) |

**Hardest datasets** (0% or near-0% pass@1 across all agents): `patents` (0% — all agents fail), `deps_dev_v1` (max 6% pass@1).

---

## 4. OpenAI In-House Data Agent — Six-Layer Context (from January 2026 Blog Post)

OpenAI's published six context layers, with their stated purpose:

| Layer | What OpenAI Describes | Our Oracle Forge Equivalent |
|---|---|---|
| 1. Table Usage Information | Historical query patterns, join frequency, how tables are used together | `kb/domain/` schema docs |
| 2. Human Annotations | Business meaning of columns, purposes, caveats — bridging technical names and business terms | `kb/domain/text_fields.md`, `join_keys.md` |
| 3. Codex Enrichment | Crawls pipeline code (SQL, Spark, Python) to understand data transformations and freshness — OpenAI calls this "the hardest sub-problem" across 70,000 tables | **Not yet implemented** |
| 4. Institutional Knowledge | Internal docs (Slack, Notion) providing "why" and "when" behind data | `kb/corrections/` + domain definitions |
| 5. Memory | Self-learning corrections from past conversations — if a user corrects the agent, it persists | `kb/corrections/corrections_log.md` (KB v3) |
| 6. Runtime Context | Live queries against data warehouse when existing info is stale | Execution Router live DB queries |

**OpenAI's stated engineering lessons:**
- "Code > Metadata" — pipeline code contains more business intent than SQL schemas
- "Too many overlapping tools confuse agents" — consolidate toolset to restrict search space
- "Rigid, highly prescriptive prompts push agents down wrong paths" — use high-level guidance instead
- "Memory is non-negotiable — without it, agents repeat the same mistakes"

---

## 5. Claude Code Source — Verified Architecture Patterns (from npm Leak, March 2026)

From the published source (github.com/sanbuphy/claude-code-source-code, docs/en/):

- **Three-layer MEMORY.md**: Index file → topic-specific files loaded on demand → session transcripts searchable. Files are loaded selectively, not concatenated.
- **autoDream consolidation**: Background process that promotes short-term corrections to long-term memory. Triggers on session end. Writes structured entries.
- **Tool scoping**: 40+ tools with tight domain boundaries. Each tool does exactly one thing. No "God tools."
- **Fork/worktree sub-agent spawn**: Isolated sub-agents in git worktrees for parallel exploration without interfering with each other.

---

## 6. Actionable Items for Oracle Forge This Week

Based on the actual data above:

1. **Add dedicated extraction tools** — the paper proves regex is the universal failure pattern (0% on patents, regex bugs on pancancer_atlas and bookreview). Add `dateutil.parser`, consider LLM-based extraction for data-dependent fields.
2. **Focus on FM2 (incorrect plan) and FM4 (incorrect implementation)** — these account for 85% of failures. Our Repair Loop should specifically detect and retry these two categories.
3. **Prioritize SQL-side aggregation** — GPT-5-mini's 2.6:1 DB-to-Python ratio at $67 dramatically outperforms Kimi-K2's 1.1:1 ratio at $1,304. Prompt our agent to push computation into SQL.
4. **Handle trailing-space join keys** — `crmarenapro` has 25% of ID fields with random trailing spaces. Our `join_keys.md` normalizer must `.strip()` before joining.
5. **Target yelp aggressively** — PromptQL gained +40pp on yelp through better table discovery. Our 7/7 Yelp pass already validates this approach. Extend to agnews and stockindex next.

---

*Next report: Week 9 Monday mob session. Focus: post-benchmark submission analysis, adversarial probe findings, and KB v3 corrections log impact assessment.*
