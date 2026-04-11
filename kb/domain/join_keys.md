# DataAgentBench Join Key Glossary

Here is how customer IDs and canonical entity identifiers appear differently across DAB datasets and database types. The agent must normalize keys before any cross-database join.

**Rule:** Never assume a primary key in an SQL database maps 1:1 without formatting to a NoSQL document ID. Always run `join_key_resolver.validate_overlap()` before joining.

---

## Yelp Dataset
- **PostgreSQL (`transactions.customer_id`):** Integer (e.g., `12345`)
- **MongoDB (`reviews.customer_id`):** String with `CUST-` prefix (e.g., `"CUST-12345"`)
- **Resolution:** Strip `CUST-` prefix from MongoDB, or format PostgreSQL integers as `f"CUST-{id}"`. Use `join_key_resolver.normalize()`.
- **Overlap expected:** ~95% after normalization. If <90%, check for customers in one system but not the other.

## Support Ticket IDs
- **SQLite (logs):** UUID format (e.g., `"550e8400-e29b-41d4-a716-446655440000"`)
- **Redshift/DuckDB (analytics):** Truncated hash (e.g., `"550e8400"`)
- **Resolution:** Truncate UUID to first 8 characters, or expand hash to full UUID via lookup table.

## Healthcare Provider IDs
- **PostgreSQL (directory):** Zero-padded string (e.g., `"00456"`)
- **MongoDB (credentialing):** Integer (e.g., `456`) — leading zeros dropped
- **Resolution:** Force string casting on both sides. Never cast to integer — information loss is irreversible. Use `str(id).zfill(5)` for normalization.

## Phone Numbers (CRM Datasets)
- **Source A:** Dashed format (e.g., `"123-456-7890"`)
- **Source B:** E.164 international format (e.g., `"+11234567890"`)
- **Resolution:** Normalize to E.164 using `join_key_resolver.normalize_key(phone, "phone_dashed", "phone_e164")`.

## Product Codes (Retail Datasets)
- **PostgreSQL (catalog):** Alphanumeric SKU (e.g., `"SKU-A1234"`)
- **MongoDB (inventory):** Numeric-only code (e.g., `1234`)
- **Resolution:** Strip alphabetic prefix, compare numeric portion. Validate overlap before joining.

## User/Account IDs (Finance Datasets)
- **PostgreSQL (accounts):** Sequential integer (e.g., `10001`)
- **DuckDB (analytics export):** Hex-encoded hash of the integer (e.g., `"2711"` for `10001`)
- **Resolution:** Convert hex to decimal or decimal to hex before comparison. Validate with sample checks.

## Injection Test
**Q:** "How is CustomerID formatted in the Yelp PostgreSQL database versus the MongoDB reviews collection?"
**Expected:** "PostgreSQL stores customer IDs as integers (e.g., 12345). MongoDB stores them as strings with CUST- prefix (e.g., 'CUST-12345'). Strip CUST- or format integer to match."
**Result:** PASS
**Date:** 2026-04-11
