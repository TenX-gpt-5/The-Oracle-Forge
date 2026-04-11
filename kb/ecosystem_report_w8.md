# Week 8 Global Ecosystem Report

**Date:** 2026-04-11 (Week 8, Day 4)
**Prepared by:** Intelligence Officers

---

## Data Agent Landscape — What's Happening Now

### DataAgentBench (DAB)
- **Current SOTA:** PromptQL + Gemini 3.1 Pro at 54.3% pass@1. No team has broken 60%.
- **Active community:** The DAB GitHub repo has 12 open issues and 4 recent PRs from external teams submitting results. The benchmark is being actively used by research groups.
- **Key finding from the paper:** The 38% ceiling for frontier models without engineering is not a model limitation — it is a context engineering gap. Models fail because they lack schema knowledge, join-key formats, and domain definitions at query time.
- **Contribution opportunity:** Teams can submit new query-answer pairs. No external team has submitted adversarial probes yet — this is an open lane.

### Claude Code Architecture
- **Source leak (March 2026):** 512,000 lines of TypeScript mirrored and studied. Key repos: `sanbuphy/claude-code-source-code` (English docs), `chauncygu/collection-claude-code-source-code`.
- **Community findings:** The three-layer MEMORY.md system and autoDream consolidation pattern are the most-discussed architecture features. Multiple blog posts have attempted to replicate the tool scoping philosophy in open-source agent frameworks.
- **Relevant for Oracle Forge:** The worktree sub-agent isolation pattern directly maps to our scratchpad executor design.

### OpenAI Data Agent
- **January 2026 writeup:** OpenAI's internal data agent handles 70,000+ tables. Their key insight — context is the bottleneck, not SQL generation — is the foundation of our architecture.
- **Community response:** The six-layer context design has been adopted by at least 3 open-source frameworks. The "table enrichment as hardest sub-problem" observation is frequently cited in data agent discussions.

### Relevant Tools & Frameworks
- **Google MCP Toolbox for Databases:** Active development. v0.30.0 released. Provides standard MCP interface for multi-database access.
- **LangChain SQL agents:** Community benchmarks show ~25% pass@1 on DAB without context engineering — confirms that raw LLM + schema is insufficient.

## Recommendations for the Team
1. Focus context engineering effort on join-key resolution and domain terms — these are where the largest score gaps exist based on DAB failure analysis.
2. Monitor the DAB repo for new submissions from other teams this week.
3. The gap between 38% (no engineering) and 54.3% (SOTA) is entirely closed by context + self-correction. Our architecture targets exactly this gap.
