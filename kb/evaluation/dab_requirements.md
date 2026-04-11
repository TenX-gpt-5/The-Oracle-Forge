# DAB Requirements

DataAgentBench tests whether an agent can answer realistic enterprise data questions across messy, mixed database systems. The hard part is not only query generation. The hard part is choosing the right source, resolving inconsistent identifiers, extracting facts from free text, and applying definitions that are not obvious from schema alone.

Oracle Forge should treat four requirements as first-class runtime problems:

- Multi-database integration: one user question may require MongoDB and DuckDB or PostgreSQL and SQLite in the same answer path. The agent must route correctly and merge results safely.
- Ill-formatted join keys: logically identical entities may use incompatible formats across systems. The runtime must normalize keys before joining.
- Unstructured text transformation: some answers require extracting facts from notes, reviews, or descriptions before aggregation.
- Domain knowledge: terms like active customer or support volume may need definitions that are not recoverable from raw schema.

Current Oracle Forge status:

- multi-database execution is demonstrated most strongly on Yelp
- join-key normalization exists in the transform layer
- text extraction exists as a lightweight transform scaffold
- domain knowledge scaffolding exists in the context layers, but the KB is still early

## Injection Test

Question:
What are the four main DAB requirement classes Oracle Forge must handle before it can be considered benchmark-credible?

Expected answer:
Multi-database integration, ill-formatted join keys, unstructured text transformation, and domain knowledge.

Status: pass

Last verified: 2026-04-11
