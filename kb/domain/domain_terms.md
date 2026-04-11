# DataAgentBench Domain Term Definitions

Here are the authoritative definitions for business terms used in DAB queries. The agent must use these definitions, not guess from schema.

**Active Customer:** A customer who has completed at least one purchase within the last 90 days. Do NOT use row existence in the user table as a proxy. Check `orders.order_date >= NOW() - INTERVAL '90 days'`.

**Churn:** A customer who has either (a) explicitly cancelled via a cancellation event, or (b) had zero activity for >180 days. Definition varies by dataset — always check the dataset-specific override. Never guess.

**Repeat Customer:** A customer with more than one purchase in a 90-day window. Not the same as "returning customer" (which may use a different time window per dataset).

**Repeat Purchase Rate:** Count of repeat customers / count of total customers in the period. Express as a percentage.

**MRR (Monthly Recurring Revenue):** Gross recurring subscription revenue for the month MINUS refunds MINUS credits. Never sum gross revenue alone. Check for `refunds` and `credits` columns in the revenue table.

**Net Revenue:** Gross revenue − refunds − discounts − chargebacks. If any of these columns exist in the table, they must be subtracted.

**Fiscal Year:** Not calendar year. Varies by dataset: retail datasets often use Feb 1 – Jan 31; healthcare may use Oct 1 – Sep 30. Check `kb/domain/dab_schema.md` for dataset-specific boundaries. If unknown, flag as ambiguous — do not default to calendar year.

**Retention Rate:** (Customers active at start of period who are still active at end) / (customers active at start of period). Requires defining "active" using the Active Customer definition above.

**Status Codes:** Meaning is dataset-specific. Common examples:
- Retail: `status=1` (pending), `status=2` (shipped), `status=3` (delivered), `status=4` (returned)
- Healthcare: `status=1` (admitted), `status=2` (in treatment), `status=3` (discharged), `status=4` (deceased)
- Telecom: `status=A` (active), `status=S` (suspended), `status=C` (cancelled)
Never interpret status codes without checking the dataset's status mapping.

**Cart Abandonment:** A session where items were added to cart but no checkout event occurred within the session. Requires joining cart events with checkout events by session ID.

**Customer Lifetime Value (CLV):** Sum of all revenue from a customer across their entire history. Often requires aggregating across multiple tables and database types.

## Injection Test
**Q:** "How should the agent define 'active customer' when querying the retail dataset?"
**Expected:** "A customer who completed at least one purchase within the last 90 days. Check orders.order_date >= NOW() - INTERVAL '90 days'. Do not use row existence."
**Result:** PASS
**Date:** 2026-04-11
