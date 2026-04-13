# Context Layers

Oracle Forge uses layered context because DAB failures are often context failures disguised as reasoning failures.

The current architecture separates context into reusable layers:

- global memory for durable agent rules
- project memory for DAB-specific corrections and definitions
- schema and usage context for tables, collections, and likely keys
- join-key intelligence for normalization logic
- text-field inventory for extraction hints
- episodic recall for similar past traces

The runtime should load only the minimum useful context for the current turn. This means the context system should select relevant fragments instead of dumping the whole KB into every prompt.

In practice, this architecture matters because DAB questions often need more than schema. They need known mappings, prior corrections, and domain definitions before the agent can route and answer correctly.

## Injection Test

Question:
Why does Oracle Forge use multiple context layers instead of one large memory dump?

Expected answer:
Because the runtime needs only the relevant rules, schema fragments, join-key mappings, text hints, and prior corrections for each turn. Loading everything creates noise and weakens answer quality.

Status: pass

Last verified: 2026-04-11
