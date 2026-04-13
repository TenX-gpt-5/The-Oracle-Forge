# Domain KB Changelog

This log tracks schema details, query patterns across DataAgentBench (DAB), join key glossary updates, and domain term definitions.

## [2026-04-11]
- Initialized CHANGELOG.md
- Added `unstructured_fields.md`: per-dataset inventory of free-text fields with extraction recipes. Injection test: PASS.
- Added `domain_terms.md`: 11 business term definitions (active customer, churn, repeat customer, MRR, fiscal year, etc.).
- [2026-04-11] Updated definitions for Active Customer and Churn to match DAB specifications.
- [2026-04-11] Verified join key formats for Yelp and Retail datasets.
- [2026-04-11] Injection Test PASS: All domain files verified against fresh context.
- Expanded `join_keys.md`: from 2 examples to 6 dataset-specific format mappings (Yelp, support tickets, healthcare, phone, product codes, finance). Injection test: PASS.
- Refocused `dab_schema.md`: removed domain terms and unstructured fields (now in dedicated files), added DuckDB/SQLite query patterns, schema navigation rules. Injection test: PASS.
## 2026-04-11

- Initialized domain KB directory and changelog.
- Added `join_keys.md`.
- Added `text_fields.md`.
