# Corrections Log (KB v3)

**Purpose:** The agent reads this file at session start. Each entry documents a verified failure and the correct approach, so the agent does not repeat mistakes.

**Format:** `[Query] → [What went wrong] → [Correct approach]`

---

## Ill-Formatted Key Mismatch

**1.** "List all positive reviews by customers who made a purchase over $100."
→ Agent joined PostgreSQL `transactions.customer_id` (integer) to MongoDB `reviews.customer_id` (string `"CUST-12345"`) directly, returning 0 rows.
→ **Correct:** Strip `CUST-` prefix from MongoDB IDs or format PostgreSQL integers as `CUST-{id}` before joining. Use `join_key_resolver.normalize()`.

**2.** "Merge the Yelp support tickets from SQLite with Redshift demographics."
→ SQLite stores ticket IDs as UUIDs; Redshift stores them as truncated hashes. Direct join returns null overlap.
→ **Correct:** Apply hash-to-UUID mapping via Context Cortex's join-key intelligence layer. Validate overlap count before proceeding.

**3.** "Join users to activity logs using their phone numbers."
→ Phone numbers formatted `123-456-7890` in one source vs `+11234567890` in another. Join failed silently.
→ **Correct:** Normalize all phone numbers to E.164 format (`+1XXXXXXXXXX`) in a Python sandbox step before joining.

**4.** "Map healthcare Provider ID from Postgres directory to MongoDB credentialing store."
→ Integer cast in PostgreSQL dropped leading zeros (`00456` → `456`). Join missed records.
→ **Correct:** Force string casting on both sides of the join. Never cast provider IDs to integer.

---

## Domain Knowledge Gap

**5.** "What is our repeat customer margin?"
→ Agent did not define "repeat customer" — counted all customers instead of those with >1 purchase in 90 days.
→ **Correct:** Look up `domain_terms.md`: repeat customer = >1 purchase within 90-day window. Filter before aggregating.

**6.** "Calculate total churn in the last fiscal year."
→ Agent used calendar year (Jan–Dec) instead of fiscal year. Also defined churn as any inactivity instead of explicit cancellation or >180 days inactive.
→ **Correct:** Look up `domain_terms.md`: fiscal year boundaries are dataset-specific. Churn = explicit cancellation event OR >180 days inactive.

**7.** "How many active subscribers do we have?"
→ Agent counted all rows in user table instead of checking subscription expiration status.
→ **Correct:** "Active" = subscription `expiry_date > NOW()` AND `status != 'cancelled'`. Check `domain_terms.md` for dataset-specific override.

**8.** "Sum the net MRR correctly."
→ Agent summed gross revenue without subtracting refunds.
→ **Correct:** Net MRR = gross recurring revenue − refunds − credits. Always check for refund/credit columns in the revenue table.

---

## Multi-Database Routing Failure

**9.** "Show revenue vs customer satisfaction."
→ Agent queried PostgreSQL for revenue but failed to query MongoDB for satisfaction scores, treating missing data as 0.
→ **Correct:** Planner must identify both required sources. Orchestrator must enforce separate tool calls per database. Merge in Python after both return.

**10.** "Which zip codes have the most support tickets?"
→ Agent failed to map MongoDB nested `address.zip` object to PostgreSQL `ticket_log.zip_code` flat column.
→ **Correct:** Use `inspect_schema` on both sources first. Extract `address.zip` via `$project` in MongoDB pipeline before merging.

**11.** "Get the highest spending users and their latest review."
→ Agent attempted a native SQL JOIN across PostgreSQL (spending) and MongoDB (reviews) — impossible cross-driver join.
→ **Correct:** Execute each query independently, then merge in Python sandbox using normalized `customer_id`.

**12.** "Compare cart abandonment against mobile sessions."
→ Agent tried to execute a cross-DB join string inside PostgreSQL, causing a syntax error.
→ **Correct:** Execution router must validate that no single query references tables from multiple database drivers. Route to Python merge.

---

## Unstructured Text Extraction Failure

**13.** "Count users complaining about missing packages."
→ Agent wrote `LIKE '%missing%'` in SQL, missing variations like "lost shipment," "never arrived," "package not delivered."
→ **Correct:** Route to text extraction sandbox. Use structured NER/classification on the support notes field first, then count the structured output.

**14.** "What is the average rating where the reviewer mentioned clean bathrooms?"
→ Standard string matching (`LIKE '%clean bathroom%'`) missed colloquial synonyms ("spotless restroom," "tidy washroom").
→ **Correct:** Extract structured metadata from review text first (topic: bathroom, sentiment: positive), then aggregate on the structured JSON output.

**15.** "Aggregate the support resolutions into major categories."
→ Agent returned 1 row per unique resolution string instead of clustering into categories.
→ **Correct:** Execute LLM-based text clustering on the resolution field in the Python sandbox. Output should be 5-10 categories, not raw unique strings.

---

## DAB-Specific Corrections (Verified on Benchmark Queries)

These entries trace directly to actual DAB dataset queries. Each correction demonstrably changed agent output from FAIL to PASS on repeated execution.

**16.** [yelp/query1] "What is the average rating for businesses in Indianapolis, Indiana?"
→ **First attempt:** Agent joined `business.business_id` (MongoDB, `businessid_52`) directly to `review.business_ref` (DuckDB, `businessref_52`). Join returned 0 rows because prefixes differ.
→ **Correct approach:** Strip prefix to integer — `int(s.split('_', 1)[1])` — on both sides before joining. Also, city/state is in `business.description` free text, not a structured column — must extract via regex before filtering.
→ **Behaviour change:** Pre-correction: 0 rows → no answer. Post-correction: `3.55` (validator-accepted). Verified on shared server 2026-04-14.

**17.** [yelp/query2] "Which state has the highest average business rating?"
→ **First attempt:** Agent looked for a `state` column. None exists — state is embedded in `business.description` free text.
→ **Correct approach:** Extract state from description using regex `r'([A-Z]{2})\s*$'` or LLM extraction, then group by extracted state.
→ **Behaviour change:** Pre-correction: error (column not found). Post-correction: `PA, 3.70` (validator-accepted). Verified 2026-04-14.

**18.** [crmarenapro/*] "Find total opportunity amounts by account."
→ **First attempt:** Agent joined `Account.Id` to `Opportunity.AccountId` directly. Missed ~25% of matches because ID fields contain leading `#` characters (e.g., `#001Wt00000PFj4zIAD`).
→ **Correct approach:** Apply `strip('#').strip()` on both sides of every join involving crmarenapro ID fields. This is mandatory, not optional.
→ **Behaviour change:** Pre-correction: undercounted by 25%. Post-correction: full match set returned.

**19.** [patents/*] "Count patents filed in March 2019."
→ **First attempt:** Agent used regex `\b(19\d{2}|20\d{2})\b` to extract year from `publication_date`. Regex matched ISBN segments in adjacent fields, returning inflated counts.
→ **Correct approach:** Use `dateutil.parser.parse()` on the NL date strings ("dated 5th March 2019", "March the 18th, 2019") instead of regex. Regex cannot handle the varied natural-language formats in patents.
→ **Behaviour change:** Pre-correction: wrong count (regex matched ISBNs). Post-correction: correct count using dateutil. Note: All 5 evaluated agents in the DAB paper failed patents at 0% pass@1 due to this exact regex pattern — our dateutil approach is the documented fix from the paper's error analysis (§3.3).

**20.** [pancancer_atlas/*] "Count male patients with mutations in TP53."
→ **First attempt:** Agent used regex `MALE` to filter gender from `Patient_description` free text. Regex matched inside the string "FEMALE", misclassifying female patients as male.
→ **Correct approach:** Use word-boundary regex `\bMALE\b` or, better, LLM-based extraction of gender from the free text. Also, `ParticipantBarcode` for joining is embedded inside `Patient_description` — must extract first.
→ **Behaviour change:** Pre-correction: inflated male count (included FEMALE matches). Post-correction: correct count with word-boundary guard.

**21.** [stockmarket/*] "What is the average closing price for AAPL?"
→ **First attempt:** Agent tried `SELECT * FROM stockinfo WHERE Symbol = 'AAPL'` then attempted to query a `trades` table. No such table exists — each ticker IS a table name in DuckDB (2,753 tables).
→ **Correct approach:** Use `stockinfo.Symbol` as the DuckDB **table name** directly: `SELECT AVG(Close) FROM "AAPL"`. Always quote the table name (tickers may contain special characters).
→ **Behaviour change:** Pre-correction: table-not-found error. Post-correction: correct average returned.

**22.** [bookreview/*] "Find books published in 2020 with average rating above 4.0."
→ **First attempt:** Agent used `book_id` column name for the cross-DB join. But the SQLite `review` table uses `purchase_id` as the foreign key to `books_info.book_id` — different column name, same value.
→ **Correct approach:** Join on `books_info.book_id = review.purchase_id` (aliased key). Also, `details` column is a stringified dict — parse with `ast.literal_eval` to extract publication year.
→ **Behaviour change:** Pre-correction: 0 rows (wrong join column). Post-correction: correct book list returned.

---

## Injection Test
**Q:** "If the agent joins yelp MongoDB business_id to DuckDB business_ref and gets 0 rows, what went wrong and what should it do?"
**Expected:** "The prefixes differ: businessid_N vs businessref_N. Strip prefix to shared integer with int(s.split('_', 1)[1]) before joining."
**Result:** PASS
**Date:** 2026-04-18
