# Text Fields

Oracle Forge should treat free-text fields as structured-data opportunities, not only as opaque strings.

In DAB-style workloads, text fields may contain facts that are required before the final calculation can be performed. Examples include support notes, review text, and business descriptions.

Current Oracle Forge evidence:

- Yelp business descriptions are used to identify location matches such as `Indianapolis, IN`
- the transform layer includes a lightweight fact-extraction scaffold for note-like text

Working rule:

- if the answer depends on meaning inside text, extraction must happen before counting, grouping, or joining
- the KB should list which fields are likely to require extraction
- the runtime should record the extraction path in the trace

This document should grow into a dataset inventory of fields that are likely to contain sentiment, location, issue type, urgency, or product/domain facts.

## Injection Test

Question:
Why are text fields first-class inputs in Oracle Forge instead of just extra columns?

Expected answer:
Because some DAB answers depend on facts embedded in free text, so Oracle Forge must extract structure from notes, reviews, or descriptions before aggregation or joining.

Status: pass

Last verified: 2026-04-11
