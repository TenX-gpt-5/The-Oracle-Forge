# Yelp Query 1 Correction Log

## Query

What is the average rating of all businesses located in Indianapolis, Indiana?

## What Went Wrong

A generic benchmark runtime can fail this query if it treats the problem as a single-source lookup or if it does not reconcile the business identity format across MongoDB and DuckDB. It can also fail if it assumes direct Toolbox-native DuckDB access is available in this repo when the working path currently goes through the remote DAB adapter.

## Correct Approach

Use the Yelp MongoDB business metadata to find businesses whose descriptions match the target location, map each `business_id` value like `businessid_52` to the DuckDB review reference format `businessref_52`, and then aggregate ratings from the DuckDB `review` table for the matched business references.

## Evidence

- command:
  `python run_benchmark_query.py --dataset yelp --query-id 1 --validate-answer`
- result:
  official remote validation returned `"is_valid": true`

## Injection Test

Question:
How should Oracle Forge solve Yelp query 1 in the current working architecture?

Expected answer:
It should read business metadata from MongoDB, match Indianapolis businesses by description, convert `businessid_*` to `businessref_*`, and aggregate ratings from DuckDB reviews before validating the final answer.

Status: pass

Last verified: 2026-04-11
