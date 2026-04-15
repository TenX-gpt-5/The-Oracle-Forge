# Oracle Forge Tool Definitions

**Injection Test Evidence:**
*   **Test Query:** "What tool should I use to fix a mismatch in Yelp keys between Mongo and DuckDB?"
*   **Expected Answer:** "You must use the `normalize_join_key` tool and then the `run_python_transform` tool."

Here is the complete tool surface for Oracle Forge. Each tool has one purpose. Do not use a tool outside its scope.

**Database Query Tools (one per DB type — never mix):**
- `run_sql_postgres`: Execute read-only SQL against PostgreSQL. Use for transactional data, user tables, revenue tables. Supports JSONB operators (`->`, `->>`), CTEs, window functions.
- `run_sql_sqlite`: Execute read-only SQL against SQLite. Limited type system — always cast join keys to TEXT. No native JSON functions in older versions.
- `run_sql_duckdb`: Execute analytical SQL against DuckDB. Optimized for aggregation, window functions, and CTEs. Use for analytics exports and large scans.
- `run_mongo_pipeline`: Execute a MongoDB aggregation pipeline. Always include an explicit `$project` stage before returning results. Returns documents, not rows — flatten before merging with SQL results.

**Inspection Tools (use before writing queries):**
- `inspect_schema`: Returns table/collection names, column names and types, primary keys, and row count estimates for a given database. Always call this before writing a query against an unfamiliar table.
- `inspect_sample_values`: Returns 3-5 sample values for a specified column. Use to detect format mismatches (e.g., integer vs CUST-prefixed string) before attempting joins.

**Transformation Tools:**
- `normalize_join_key`: Detects and normalizes key format mismatches between two databases. Input: raw keys from both sides. Output: normalized keys + overlap validation. Must be called before any cross-database join.
- `run_python_transform`: Executes Python code in the sandbox for data merging, cleaning, or complex transformations that cannot be done in SQL. Use for: cross-DB merges, phone number normalization, date parsing.
- `extract_structured_facts`: Extracts structured data from free-text fields (support notes, reviews, descriptions). Input: raw text + target schema. Output: structured JSON. Never use `LIKE '%keyword%'` for text extraction — use this tool instead.

**Validation Tool:**
- `validate_answer_contract`: Checks the final answer against the expected output shape: non-empty result, correct data types, join cardinality plausible, answer grounded in query trace. Call before returning any answer to the user.

**Tool Selection Rules:**
1. Never query two database types in a single tool call. Use one tool per database, merge in Python.
2. Always call `inspect_schema` + `inspect_sample_values` before writing a cross-DB query.
3. Always call `normalize_join_key` before any cross-database join.
4. Always call `validate_answer_contract` before returning a final answer.
5. When in doubt about which database has the data, call `inspect_schema` on both — do not guess.

## Injection Test
**Q:** "Which tool should the agent use to combine revenue data from PostgreSQL with review data from MongoDB?"
**Expected:** "Use `run_sql_postgres` for revenue, `run_mongo_pipeline` for reviews (with `$project`), then `run_python_transform` to merge the results. Never attempt a cross-driver join in a single query."
**Result:** PASS
**Date:** 2026-04-11
