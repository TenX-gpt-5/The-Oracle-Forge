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
