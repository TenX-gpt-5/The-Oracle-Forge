# Claude Code Memory Architecture

**Injection Test Evidence:**
*   **Test Query:** "What architectural pattern should I use to break up the memory context?"
*   **Expected Answer:** "You must use the Three-Layer Memory System (Index, Topic Files, Session Transcripts) to prevent context window overflow."

Here is how Claude Code manages memory and tools for reliable agent execution.

**Three-Layer Memory System:**
1. **MEMORY.md (Index):** A persistent index file loaded into every session's default prompt. Contains topic pointers (e.g., "database_patterns → memory/topics/database_patterns.md") and global directives ("Always validate join overlap before merging"). The index is small — typically under 200 lines — so it fits in every context window without truncation.
2. **Topic Files (`memory/topics/*.md`):** Detailed knowledge organized by domain. Each file covers one topic (e.g., join key formats, schema quirks, known failure patterns). Retrieved dynamically: the system matches the current task context against topic pointers in MEMORY.md and injects only relevant topic files. Not all topics are loaded — only those matching the active problem.
3. **Session Transcripts:** Granular turn-by-turn logs of tool calls, outputs, and decisions. Searchable by timestamp or semantic content. Used for episodic recall when the agent encounters a problem similar to a previously solved one.

**autoDream Consolidation Pattern:**
Triggered automatically when a session or task ends. The consolidator reads the session's scratchpad execution trace and extracts two types of durable knowledge:
- **Directives:** Behavioral rules like "Always cast provider IDs to string before joining" or "Never use calendar year for fiscal calculations."
- **Factual lessons:** Specific discoveries like "Yelp MongoDB reviews use CUST- prefix on customer_id."

These are written into the appropriate topic files and the MEMORY.md index is updated with new pointers. The key discipline: consolidation only promotes verified lessons — if a fix worked once but wasn't validated, it stays in the session transcript, not in the topic files.

**Tool Scoping Philosophy:**
Tools are explicitly bounded by database type and operation (e.g., `run_sql_postgres` is distinct from `run_sql_sqlite`). Claude Code uses ~40+ narrow tools rather than one monolithic "execute" tool. This improves determinism: the model selects the right tool by name rather than guessing parameters for a generic tool. For Oracle Forge, this means one tool per database type, not a single `run_query` tool with a `db_type` parameter.

**Worktree Sub-agent Spawn Modes:**
When work is parallelizable or requires risky exploration, the agent spawns a sub-agent in its own git worktree. The sub-agent has an isolated filesystem, its own tool context, and a bounded budget. Only successful paths return a summarized trace pattern to the parent agent. Failed explorations are discarded without polluting the main state.

## Injection Test
**Q:** "What happens during autoDream consolidation and what two types of knowledge does it extract?"
**Expected:** "autoDream is triggered when a session ends. It extracts directives (behavioral rules) and factual lessons (specific discoveries) from the session trace, writing them into topic files."
**Result:** PASS
**Date:** 2026-04-11
