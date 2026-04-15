# Adversarial Probe Library (v2)

A curated set of **16 probes** designed to expose agent failures aligned with
DataAgentBench's four core failure categories:

1. **Ill-formatted Key Joins** — Keys exist in both stores but differ in format
   (casing, padding, prefixes, type, encoding). Naive joins silently drop rows.
2. **Multi-Database Integration** — Required facts live in ≥2 stores with
   different dialects. Agent must plan, route, fetch, and merge in a scratchpad.
3. **Unstructured Text Transformation** — Required answer lives in free text
   (reviews, tickets, descriptions). String matching is insufficient; the agent
   must classify, extract, or cluster.
4. **Domain Knowledge** — The query uses a term whose operational definition is
   not recoverable from the schema alone (e.g. "active", "repeat", "fiscal").

Each probe below follows the schema:

```
Probe N — <Category>
Query:
Databases involved:
Expected failure mode:
Observed agent response:
Fix that worked:
```

The **Observed** and **Fix** fields are intentionally blank — they are filled in
only after the probe has been run against the live Oracle Forge agent.

---

## Category 1 — Ill-formatted Key Joins

### Probe 1 — Prefix-Mismatched Business Identifier
- **Query:** "For every Yelp business with at least 5 reviews, return the
  business name and its average rating, sourced from MongoDB metadata and
  DuckDB review aggregates."
- **Databases involved:** MongoDB (`business` — `business_id` stored as
  `businessid_<hash>`), DuckDB (`review` — `business_ref` stored as
  `businessref_<hash>`).
- **Expected failure mode:** Agent joins on the raw identifier columns without
  rewriting the `businessid_` prefix to `businessref_`, producing zero matches
  and a silently empty result. Aligns with DAB FM4 (correct plan, wrong
  implementation — missing identifier normalization).
- **Observed agent response:**
- **Fix that worked:**

### Probe 2 — Zero-Padded Provider ID Collapsed to Integer
- **Query:** "Show the credentialing status of every provider in the directory,
  joining the Postgres `provider_directory` to the MongoDB `credentialing`
  collection on `provider_id`."
- **Databases involved:** PostgreSQL (`provider_directory.provider_id` is
  `VARCHAR` with leading zeros, e.g. `000412`), MongoDB
  (`credentialing.provider_id` stored as `int`, e.g. `412`).
- **Expected failure mode:** Agent casts the Postgres string to `int` (or the
  Mongo int to string) losing leading zeros on ~30% of rows that share a
  numeric suffix with another provider. Joins succeed but conflate distinct
  providers. FM4 — implementation error.
- **Observed agent response:**
- **Fix that worked:**

### Probe 3 — Phone Number Encoding Drift
- **Query:** "Join CRM users to their activity logs using phone number and
  return the 10 most active users."
- **Databases involved:** PostgreSQL (`users.phone` as `123-456-7890`),
  MongoDB (`activity_logs.phone` as E.164 `+11234567890`).
- **Expected failure mode:** Agent executes the join on raw text, matching <5%
  of rows, and reports the resulting ranking as correct without flagging the
  dropped volume. FM4 — missing normalization.
- **Observed agent response:**
- **Fix that worked:**

### Probe 4 — Case-Folded Email Key
- **Query:** "For each subscriber in the Postgres billing table, list the
  support tickets they have opened in the SQLite helpdesk store, joining on
  email."
- **Databases involved:** PostgreSQL (`billing.email` — mixed case, as entered
  by the user), SQLite (`tickets.email` — lowercased on ingest).
- **Expected failure mode:** Case-sensitive join misses ~40% of the user base.
  The agent returns the partial result without detecting an unrealistically
  low match rate. FM4.
- **Observed agent response:**
- **Fix that worked:**

### Probe 5 — UUID vs. Truncated Hash on Ticket Identifier
- **Query:** "Merge the support tickets from SQLite with the demographics in
  DuckDB, keyed on ticket_id, and report resolution time by region."
- **Databases involved:** SQLite (`tickets.ticket_id` is a full UUID),
  DuckDB (`demographics.ticket_id` is the first 8 hex chars of the UUID).
- **Expected failure mode:** Direct equality join returns 0 rows. Agent
  interprets the empty merge as "no overlap" rather than a format mismatch. FM4.
- **Observed agent response:**
- **Fix that worked:**

---

## Category 2 — Multi-Database Integration

### Probe 6 — Revenue vs. Sentiment Split Across Stores
- **Query:** "Show monthly revenue alongside average customer satisfaction for
  the last six months."
- **Databases involved:** PostgreSQL (`transactions`), MongoDB
  (`satisfaction_surveys`).
- **Expected failure mode:** Agent fetches revenue from Postgres but skips the
  Mongo call entirely, returning a table with `satisfaction` as `NULL` or `0`
  and claiming the dataset is complete. FM2 — incomplete plan.
- **Observed agent response:**
- **Fix that worked:**

### Probe 7 — Cross-DB JOIN Attempted in SQL Engine
- **Query:** "Return the top 20 highest-spending users together with the text
  of their most recent review."
- **Databases involved:** PostgreSQL (`transactions`), MongoDB (`reviews`).
- **Expected failure mode:** Agent writes a single SQL statement referencing
  both `transactions` and `reviews` and ships it to Postgres, which errors with
  `relation "reviews" does not exist`. Agent does not retry via the Python
  scratchpad merge path. FM4 — implementation error.
- **Observed agent response:**
- **Fix that worked:**

### Probe 8 — Nested MongoDB Address Not Projected for Join
- **Query:** "Which ZIP codes have the most support tickets?"
- **Databases involved:** MongoDB (`customers` with nested
  `address.postal_code`), PostgreSQL (`ticket_log`).
- **Expected failure mode:** Agent queries Postgres for `zip` and fails with
  `column "zip" not found`, never reaching the Mongo path. When it does reach
  Mongo, it forgets to `$project` the nested field, returning the full document
  to the scratchpad and timing out. FM3 — correct plan, wrong data selection.
- **Observed agent response:**
- **Fix that worked:**

### Probe 9 — Cart Abandonment vs. Session Telemetry
- **Query:** "For the last fiscal quarter, compute cart-abandonment rate by
  device class using Postgres cart events and DuckDB session telemetry."
- **Databases involved:** PostgreSQL (`carts`), DuckDB (`sessions`).
- **Expected failure mode:** Agent attempts a Postgres-side join that
  references the DuckDB-only `sessions` table. Falls back to a single-source
  answer using only `carts.device`, which under-represents mobile because most
  mobile abandonment events only exist in DuckDB. FM3.
- **Observed agent response:**
- **Fix that worked:**

### Probe 10 — Book Reviews vs. Catalog Metadata
- **Query:** "Among books with more than 1,000 reviews, which author has the
  highest average review rating?" (adapted from DAB `bookreview`.)
- **Databases involved:** PostgreSQL (`books_info` — author, title), SQLite
  (`review_query` — review rows).
- **Expected failure mode:** Agent aggregates review counts and ratings inside
  SQLite but forgets to join back to Postgres for the `author` column, and
  returns `book_id` strings instead of author names. FM2 — stops early.
- **Observed agent response:**
- **Fix that worked:**

### Probe 11 — Multi-Dialect Time Window Alignment
- **Query:** "How many businesses that received at least one review in the
  first half of 2016 advertise either business parking or bike parking?"
  (adapted from DAB `yelp` query 3.)
- **Databases involved:** DuckDB (`review.date`, `review.business_ref`),
  MongoDB (`business.attributes.BusinessParking`,
  `business.attributes.BikeParking`).
- **Expected failure mode:** Agent filters DuckDB reviews to 2016-H1 and
  fetches Mongo `attributes` — but the Mongo parking fields are
  nested JSON strings (`"{'garage': True, ...}"`), not booleans. The boolean
  test fails for all rows, returning `0`. FM4.
- **Observed agent response:**
- **Fix that worked:**

---

## Category 3 — Unstructured Text Transformation

### Probe 12 — "Missing Package" Complaint Volume
- **Query:** "How many support tickets describe a missing, lost, or undelivered
  package?"
- **Databases involved:** MongoDB (`support_tickets.description`).
- **Expected failure mode:** Agent writes `LIKE '%missing%'` (or its Mongo
  `$regex` equivalent) and returns a count that under-reports the true volume
  by ~4x, missing phrasings such as "never arrived," "lost shipment," "not
  delivered yet," "package went astray." FM4 — brittle pattern.
- **Observed agent response:**
- **Fix that worked:**

### Probe 13 — Colloquial Attribute Extraction in Reviews
- **Query:** "What is the average star rating across reviews that mention clean
  bathrooms?"
- **Databases involved:** MongoDB (`reviews.text`).
- **Expected failure mode:** Agent matches only the literal phrase "clean
  bathrooms" and misses paraphrases ("spotless restrooms," "tidy washroom,"
  "bathroom was immaculate"), biasing the average toward a small, non-random
  subset. FM4.
- **Observed agent response:**
- **Fix that worked:**

### Probe 14 — Resolution-Category Clustering
- **Query:** "Aggregate support ticket resolutions into the top 6–8 categories
  and report the count per category."
- **Databases involved:** MongoDB (`support_tickets.resolution_note`).
- **Expected failure mode:** Agent returns `GROUP BY resolution_note` and
  produces ~1,200 unique single-count rows instead of clustering semantically
  similar notes ("refund issued," "refund processed," "money returned") into
  coherent categories. FM2 — plan is missing the clustering step.
- **Observed agent response:**
- **Fix that worked:**

### Probe 15 — Category Extraction for Top Business
- **Query:** "Which business received the highest average rating between
  January 1, 2016 and June 30, 2016, and what category does it belong to?"
  (adapted from DAB `yelp` query 6.)
- **Databases involved:** DuckDB (`review`), MongoDB (`business.categories` —
  stored as a comma-delimited string such as `"Coffee & Tea, Cafes,
  Restaurants"`).
- **Expected failure mode:** Agent selects the correct top business but emits
  `category = "Unknown"` because it reads `business.categories` as an array,
  fails the isinstance check, and falls through to the default. Validator
  rejects with "Missing category: restaurants." FM4.
- **Observed agent response:**
- **Fix that worked:**

### Probe 16 — Negative-Sentiment Filtering in Free Text
- **Query:** "Count the negative support notes per customer segment over the
  last 30 days."
- **Databases involved:** MongoDB (`notes.body`), PostgreSQL (`customers.segment`).
- **Expected failure mode:** Agent either (a) returns the raw note text
  ungrouped, or (b) keyword-matches on "bad"/"angry"/"upset" and misses
  sarcasm, implicit negativity ("still waiting", "third time I've called"),
  and domain-specific negativity ("charged twice"). FM2/FM4.
- **Observed agent response:**
- **Fix that worked:**

---

## Category 4 — Domain Knowledge

### Probe 17 — "Repeat Customer" Without a Definition
- **Query:** "What is the margin contribution of repeat customers this quarter?"
- **Databases involved:** PostgreSQL (`transactions`, `customers`).
- **Expected failure mode:** Agent invents its own definition of "repeat"
  (any customer with ≥2 purchases ever) instead of the company's rule (≥2
  purchases within a rolling 90-day window). Baseline ends up ~3x inflated.
  FM2 — plan built on a wrong definition.
- **Observed agent response:**
- **Fix that worked:**

### Probe 18 — Fiscal vs. Calendar Year
- **Query:** "Calculate total churn for the last fiscal year."
- **Databases involved:** PostgreSQL (`subscriptions`).
- **Expected failure mode:** Agent uses Jan 1 – Dec 31 instead of the business's
  Feb 1 – Jan 31 fiscal calendar, yielding a churn number ~15% off the correct
  figure. FM2.
- **Observed agent response:**
- **Fix that worked:**

### Probe 19 — "Active" Subscriber Semantics
- **Query:** "How many active subscribers do we have today?"
- **Databases involved:** PostgreSQL (`subscribers`).
- **Expected failure mode:** Agent returns `COUNT(*)` from the subscribers
  table (45,000) instead of filtering to `status = 'active' AND
  expires_at > NOW() AND NOT is_paused` (31,200). The word "active" has an
  operational definition absent from the schema. FM3.
- **Observed agent response:**
- **Fix that worked:**

### Probe 20 — Net MRR Must Subtract Refunds
- **Query:** "Report this month's net MRR."
- **Databases involved:** PostgreSQL (`revenue`, `refunds`).
- **Expected failure mode:** Agent returns gross MRR and never touches the
  `refunds` table because the schema does not signal that the two must be
  combined. FM2 — missing operation.
- **Observed agent response:**
- **Fix that worked:**

---

## Coverage Summary

| Category                            | Probes                                   | Count |
| ----------------------------------- | ---------------------------------------- | ----- |
| Ill-formatted Key Joins             | 1, 2, 3, 4, 5                            | 5     |
| Multi-Database Integration          | 6, 7, 8, 9, 10, 11                       | 6     |
| Unstructured Text Transformation    | 12, 13, 14, 15, 16                       | 5     |
| Domain Knowledge                    | 17, 18, 19, 20                           | 4     |
| **Total**                           |                                          | **20**|

All four DAB categories are represented; the minimum-three-category bar is
cleared with margin. Observed responses and fixes are populated as each probe
is executed against the agent.
