# Adversarial Probes

This file tracks adversarial probes designed to expose benchmark failure modes.

Target:

- minimum 15 probes
- minimum 3 failure categories

## Probe Template

```md
## Probe N
Failure category:
Query:
Databases involved:
Expected failure:
Observed failure:
Fix applied:
Post-fix score or outcome:
```

## Initial Probes

## Probe 1
Failure category: Multi-database routing failure
Query: Compare customer revenue from PostgreSQL with support ticket counts from MongoDB for the same customer set.
Databases involved: PostgreSQL, MongoDB
Expected failure: Agent routes to only one source or fails to merge.
Observed failure: Not yet logged.
Fix applied: Pending.
Post-fix score or outcome: Pending.

## Probe 2
Failure category: Ill-formatted key mismatch
Query: Join Yelp business metadata from MongoDB with DuckDB review aggregates using business identity.
Databases involved: MongoDB, DuckDB
Expected failure: Agent attempts direct join without converting identifier format.
Observed failure: Known architectural risk; corrected in Yelp query 1 benchmark path.
Fix applied: Convert `businessid_*` to `businessref_*` before aggregation.
Post-fix score or outcome: Yelp query 1 passes remote validation.

## Probe 3
Failure category: Unstructured text extraction failure
Query: Count negative support-note mentions by segment.
Databases involved: MongoDB, SQLite or PostgreSQL
Expected failure: Agent returns raw text or counts without extraction.
Observed failure: Not yet logged.
Fix applied: Pending.
Post-fix score or outcome: Pending.

## Probe 4
Failure category: Multi-database aggregation mismatch
Query: Yelp q2 — "Which U.S. state has the highest number of reviews, and what is the average rating of businesses in that state?"
Databases involved: MongoDB (`business` metadata), DuckDB (`review`)
Expected failure: Correct state identified but wrong averaging rule (review-weighted vs business-level average) causes numeric mismatch.
Observed failure: Latest run output `PA, 3.68`; remote validator rejected with `Number near 'PA' does not match ≈3.699395770392749`.
Fix applied: Partial. Added explicit state-resolution and cross-db aggregation path; still needs benchmark-aligned average semantics.
Post-fix score or outcome: `is_valid: false` (2026-04-12 targeted rerun).

## Probe 5
Failure category: Count answer synthesis failure
Query: Yelp q3 — "During 2018, how many businesses that received reviews offered either business parking or bike parking?"
Databases involved: MongoDB (`attributes` parking fields), DuckDB (`review` dates/business_ref)
Expected failure: Pipeline computes intermediate artifacts but final answer text does not include required integer token (`35`) in validator-recognized form.
Observed failure: Latest rerun rejected with `Number 35 not found in LLM output.`
Fix applied: Partial. Added parking-aware execution branch and extraction artifacts; final answer formatting still not consistently emitting validated integer.
Post-fix score or outcome: `is_valid: false` (2026-04-12 targeted rerun).

## Probe 6
Failure category: Unstructured text extraction failure
Query: Yelp q6 — "Which business received the highest average rating between January 1, 2016 and June 30, 2016, and what category does it belong to?"
Databases involved: DuckDB (`review` date/rating window), MongoDB (`business` description/category text)
Expected failure: Correct business selected, but category extraction degrades to placeholder/unknown.
Observed failure: Output includes business name `Coffee House Too Cafe` but categories show `Unknown`; validator rejected with `Missing category: restaurants`.
Fix applied: Partial. Added date-windowed top-business path with category hook; category parser fallback still insufficient for this business row.
Post-fix score or outcome: `is_valid: false` (2026-04-12 targeted rerun).
