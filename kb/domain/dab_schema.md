# DataAgentBench Schema Guidelines

Here are known query patterns and schema pitfalls working across DAB database types.

**Query Patterns That Work:**
- *MongoDB Aggregation Pipelines:* Always require explicit field projection (`$project`) before combining with PostgreSQL results. If left un-projected, the returned document payload volume will crash the context builder.
- *PostgreSQL JSONB:* Always explicitly cast JSONB fields before using them in `WHERE` clauses. Use `->>'key'` for text extraction, `->` for nested object access.
- *Cross-DB Aggregation:* Execute each database query independently, collect results into Python, then merge and aggregate. Never attempt cross-driver joins in SQL.
- *DuckDB Analytical SQL:* Supports window functions and CTEs natively. Use for heavy aggregation workloads when the data is available in DuckDB format.
- *SQLite:* Limited type system — all join keys should be cast to TEXT for comparison to avoid silent type coercion.

**Schema Navigation Rules:**
- Always run `inspect_schema` on both databases before writing a cross-DB query.
- Always check for `NULL` rates in key columns before joining — high NULLs signal data quality issues that will silently reduce result counts.
- Always check column types match expectations — a `customer_id` column may be INTEGER in one table and TEXT in another within the same database.

**See also:** `domain_terms.md` for business term definitions, `unstructured_fields.md` for free-text field inventory, `join_keys.md` for cross-database key format mappings.

## Injection Test
**Q:** "What must you do before combining MongoDB aggregation pipeline results with PostgreSQL query results?"
**Expected:** "Apply explicit field projection ($project) in the MongoDB pipeline first. If left un-projected, the document payload will crash the context builder."
**Result:** PASS
**Date:** 2026-04-11
