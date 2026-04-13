# Knowledge Base

This directory holds the Oracle Forge team Knowledge Base.

Principles:

- injected, not summarised
- specific to DAB, these datasets, and this agent
- maintained, not archived
- verified before committing

Each document should be short, concrete, and directly usable in an LLM context.

Required subdirectories:

- [architecture](/Users/gersumasfaw/week8_9/kb/architecture)
- [domain](/Users/gersumasfaw/week8_9/kb/domain)
- [evaluation](/Users/gersumasfaw/week8_9/kb/evaluation)
- [corrections](/Users/gersumasfaw/week8_9/kb/corrections)

Each subdirectory should keep:

- a `CHANGELOG.md`
- small focused documents
- an injection test section in every document

Suggested workflow:

1. Add only knowledge that the agent cannot safely infer.
2. Keep documents under roughly 400 words when possible.
3. Include one explicit injection test per document.
4. Revise or remove documents that fail injection tests.
