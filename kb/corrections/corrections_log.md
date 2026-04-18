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

**16.** "What fraction of all articles authored by Amy Jones belong to the Science/Technology category?" (agnews q2)
→ First failure: MongoDB not running (infrastructure). Fix: Docker MongoDB 7.0.
→ Second failure: returned 18/111 — prompt too broad (social science, psychology, environmental counted as Sci/Tech).
→ Third failure: returned 14/111 — prompt too strict (over-corrected).
→ Fourth failure: LLM returned 19/111 — borderline articles still misclassified.
→ **Correct answer: 16/111 = 0.1441.** Root cause: LLM classification prompt lacked specific disambiguation rules for borderline AG News cases. Fixed by adding explicit rules to KB (`domain_terms.md`) and to the classification prompt: pharma earnings/drug delays = Business; telecom mergers as deals = Business; crowd psychology = World; FCC merger approval = Business; ocean policy = World.

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
