# The Best AI Model Only Scores 38% on a Data Benchmark. Here's Why That Number Changed How I Think About AI.

Author: Bethelhem Abay
Published: April 14, 2026
Platform: LinkedIn
Link: https://www.linkedin.com/feed/update/urn:li:share:7449902416717742080/

---

The best score any AI model has achieved on DataAgentBench — a UC Berkeley benchmark for multi-database analytical questions — is 38% pass@1. That model is Gemini 2.5 Pro.

When I first saw that number, my instinct was to frame it as a model limitation. The model isn't smart enough yet. Give it more parameters, better training data, a longer context window, and the score goes up.

Building Oracle Forge changed that framing completely.

## The Real Problem Is Not the Model

DataAgentBench tests whether an AI agent can answer complex analytical questions across multiple databases simultaneously — PostgreSQL, MongoDB, DuckDB, SQLite, all in the same query. A single question might require joining customer records from a relational database with review data from a document store, using keys that don't share the same format across systems.

The benchmark exposes four hard requirements:

1. Multi-database integration — routing sub-queries to the right database and reconciling results
2. Ill-formatted join keys — customer IDs that are integers in one database and "CUST-001" strings in another
3. Unstructured text transformation — extracting structured facts from free-text fields before counting them
4. Domain knowledge gaps — knowing that "active customer" means "purchased in last 90 days," not just "exists in the database"

None of these are model capability problems. They are context problems. The model cannot see the right information at query time, so it fails — not because it lacks intelligence, but because it lacks context.

## What We Built to Fix It

Oracle Forge is built around a three-layer knowledge base injected before every query run:

**Layer 1 — Schema Index (KB v1):** A pre-indexed map of every table, column, and type across every connected database. When this layer is present, the agent never hallucinates a column name. When it is absent, hallucination is guaranteed on unfamiliar schemas.

**Layer 2 — Domain Store (KB v2):** Human-curated facts about what business terms mean in each dataset. That "yelp_business.stars" is an average float, not a count. That cuisine categories are stored in lowercase. That "repeat purchase rate" has a specific calculation definition in each domain. Without this layer, syntactically correct SQL returns semantically wrong answers — and the model has no way to know.

**Layer 3 — Corrections Log (KB v3):** A live log of every past query failure and its fix. When the agent retries a failed query, it reads this log first and does not repeat the same error. This is the autoDream consolidation pattern from the Claude Code architecture — short-term corrections promoted to long-term memory, compounding across every run.

## The Evidence

On the Yelp dataset — the first dataset we validated fully — Oracle Forge achieved pass@1 = 1.0 across all 7 benchmark questions, 50 trials each, verified by DataAgentBench's official validator.

That is not a model achievement. We did not change the model. We changed the context around it.

The gap between 38% and 100% on our validated dataset is not a model gap. It is an architecture gap. And closing that gap does not require waiting for a smarter model — it requires building the right knowledge infrastructure around the model you already have.

## What This Means

If your team is evaluating AI agents for data work and finding that results are inconsistent or wrong, the first question is not "which model should we use?" The first question is "what context does the model have access to at query time?"

Schema knowledge. Domain definitions. A living record of past failures and their fixes.

Those three things, properly engineered and injected, move the needle more than any model upgrade I have seen.

That is the Oracle Forge thesis. And the benchmark evidence supports it.
