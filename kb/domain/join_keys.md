# Join Keys

Oracle Forge should assume that shared business or customer identity may be represented differently across databases even when the entities are logically the same.

The strongest verified example in the current repo is the Yelp path:

- MongoDB business metadata uses `business_id` values like `businessid_52`
- DuckDB review rows use `business_ref` values like `businessref_52`

Those values do not match directly. Oracle Forge must normalize them before joining or filtering across sources. In the current implementation, the working Yelp strategy converts `businessid_*` to `businessref_*` before running the DuckDB aggregation.

The general rule for Oracle Forge is:

- never assume raw identifiers align across systems
- inspect format before joining
- document reusable mappings in this KB

This should expand over time into a dataset-by-dataset glossary of join-key transformations.

## Injection Test

Question:
How does Oracle Forge reconcile Yelp business identity across MongoDB and DuckDB in the current verified path?

Expected answer:
It maps MongoDB `business_id` values from the `businessid_*` format to DuckDB `business_ref` values in the `businessref_*` format before aggregation.

Status: pass

Last verified: 2026-04-11
