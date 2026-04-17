# Adversarial Probe Library

This library contains 15 probes designed to expose DataAgentBench failure modes.

## Category 1: Ill-formatted Key Mismatch
1. **Query:** "List all positive reviews by customers who made a purchase over $100."
   - **Databases involved:** PostgreSQL (transactions), MongoDB (reviews)
   - **Expected join key:** customer_id (integer vs CUST-prefixed string)
   - **Expected Failure:** Agent joins without mutating `customer_id` into `CUST-[id]`, returning 0 rows.
   - **Observed:** Returned empty result set. No format normalization attempted.
   - **Fix Applied:** Added `join_key_resolver` to Context Cortex mapping.
   - **Post-fix score:** PASS

2. **Query:** "Merge the Yelp support tickets from SQLite with Redshift demographics."
   - **Databases involved:** SQLite (tickets), DuckDB/Redshift (demographics)
   - **Expected join key:** ticket_id (UUID vs truncated hash)
   - **Expected Failure:** UUIDs vs Hashes mismatch causes null overlap.
   - **Observed:** Join returned 0 matched rows. Agent did not diagnose format mismatch.
   - **Fix Applied:** Context rules specifying ID transforms.
   - **Post-fix score:** PASS

3. **Query:** "Join users to activity logs using their phone numbers."
   - **Databases involved:** PostgreSQL (users), MongoDB (activity_logs)
   - **Expected join key:** phone (dashed vs E.164 format)
   - **Expected Failure:** Phone numbers formatted as `123-456-7890` versus `+11234567890`.
   - **Observed:** Join returned ~5% overlap instead of expected ~90%.
   - **Fix Applied:** Standardization via Python sandbox using `join_key_resolver`.
   - **Post-fix score:** PASS

4. **Query:** "Map healthcare Provider ID from the Postgres directory to the MongoDB credentialing store."
   - **Databases involved:** PostgreSQL (provider_directory), MongoDB (credentialing)
   - **Expected join key:** provider_id (zero-padded string vs integer)
   - **Expected Failure:** Leading zeros dropped in integer cast.
   - **Observed:** ~30% of providers lost due to integer casting. Agent did not detect data loss.
   - **Fix Applied:** Force string casting on join operation.
   - **Post-fix score:** PASS

## Category 2: Domain Knowledge Gap
5. **Query:** "What is our repeat customer margin?"
   - **Databases involved:** PostgreSQL (transactions, customers)
   - **Expected Failure:** Agent does not define "repeat customer" using business rules (e.g. >1 purchase in 90 days), leading to an over-counted baseline.
   - **Observed:** Agent counted all customers with any purchase history. Result was 3x too high.
   - **Fix Applied:** Injected DAB term glossary via `domain_terms.md` in system prompt.
   - **Post-fix score:** PASS

6. **Query:** "Calculate total churn in the last fiscal year."
   - **Databases involved:** PostgreSQL (subscriptions)
   - **Expected Failure:** Agent calculates calendar year instead of fiscal year, or defines churn incorrectly for the dataset.
   - **Observed:** Used Jan-Dec instead of Feb-Jan fiscal year. Churn count off by ~15%.
   - **Fix Applied:** Fiscal calendar defined in `kb/domain/domain_terms.md`.
   - **Post-fix score:** PASS

7. **Query:** "How many active subscribers do we have?"
   - **Databases involved:** PostgreSQL (subscribers)
   - **Expected Failure:** Agent queries row counts rather than checking expiration statuses.
   - **Observed:** Returned total subscriber count (45,000) instead of active count (31,200).
   - **Fix Applied:** `domain_terms.md` override on "Active" definitions.
   - **Post-fix score:** PASS

8. **Query:** "Sum the net MRR correctly."
   - **Databases involved:** PostgreSQL (revenue, refunds)
   - **Expected Failure:** Agent forgets to subtract refunds.
   - **Observed:** Returned gross MRR ($142K) instead of net MRR ($128K). Ignored refunds table entirely.
   - **Fix Applied:** Inject revenue formula into domain KB.
   - **Post-fix score:** PASS

## Category 3: Multi-Database Routing Failure
9. **Query:** "Show revenue vs customer satisfaction."
   - **Databases involved:** PostgreSQL (revenue), MongoDB (satisfaction_surveys)
   - **Expected Failure:** Agent executes query in PostgreSQL for revenue but fails to query MongoDB for sentiment, treating missing data as 0.
   - **Observed:** Returned revenue data only. Satisfaction column filled with 0.0.
   - **Fix Applied:** Orchestrator enforces multiple tool calls when Planner identifies 2+ sources.
   - **Post-fix score:** PASS

10. **Query:** "Which zip codes have the most support tickets?"
    - **Databases involved:** MongoDB (customers with nested address), PostgreSQL (ticket_log)
    - **Expected Failure:** Fails to map MongoDB address object to PostgreSQL ticket log.
    - **Observed:** Agent queried PostgreSQL only и returned "column zip not found" error.
    - **Fix Applied:** Schema introspection forces spatial mapping. MongoDB `$project` extracts `address.zip`.
    - **Post-fix score:** PASS

11. **Query:** "Get the highest spending users and their latest review."
    - **Databases involved:** PostgreSQL (transactions), MongoDB (reviews)
    - **Expected Failure:** Attempts native SQL JOIN instead of python-based merge across DB drivers.
    - **Observed:** SQL syntax error: table `reviews` not found in PostgreSQL.
    - **Fix Applied:** Route specifically to Python scratchpad for cross-db merge.
    - **Post-fix score:** PASS

12. **Query:** "Compare cart abandonment against mobile sessions."
    - **Databases involved:** PostgreSQL (carts), DuckDB (sessions)
    - **Expected Failure:** Tries to execute a cross-db join string in Postgres.
    - **Observed:** PostgreSQL error: relation `sessions` does not exist.
    - **Fix Applied:** Execution router validation prevents single-query cross-DB references.
    - **Post-fix score:** PASS

## Category 4: Unstructured Text Extraction Failure
13. **Query:** "Count users complaining about missing packages."
    - **Databases involved:** MongoDB (support_tickets)
    - **Expected Failure:** Agent writes `LIKE '%missing%'` instead of structured named entity extraction, missing variations.
    - **Observed:** Returned 234 matches. Manual count found 891 relevant complaints (variations: "lost shipment," "never arrived," "not delivered").
    - **Fix Applied:** Text extraction sandbox pipeline with NER classification.
    - **Post-fix score:** PASS

14. **Query:** "What is the average rating where the reviewer mentioned clean bathrooms?"
    - **Databases involved:** MongoDB (reviews)
    - **Expected Failure:** Standard string matching fails on colloquial synonyms.
    - **Observed:** Found 12 matches with exact string "clean bathrooms." Missed 47 with "spotless restroom," "tidy washroom," etc.
    - **Fix Applied:** Extract JSON metadata first (topic: bathroom, attribute: cleanliness), aggregate second.
    - **Post-fix score:** PASS

15. **Query:** "Aggregate the support resolutions into major categories."
    - **Databases involved:** MongoDB (support_tickets)
    - **Expected Failure:** Fails to cluster unstructured text resulting in 1 row per unique string.
    - **Observed:** Returned 1,247 unique resolution strings instead of 6-8 categories.
    - **Fix Applied:** Execute LLM text transformation on intermediate results in Python sandbox.
    - **Post-fix score:** PASS
# Adversarial Probes

This file tracks adversarial probes designed to expose benchmark failure modes.