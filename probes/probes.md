# Adversarial Probe Library

A run-tracker of adversarial probes designed to expose DataAgentBench (DAB)
failure modes against the Oracle Forge agent. Every database, collection,
table, and field referenced below is drawn from a real DAB `db_description.txt`
([DataAgentBench-main](../../../../Downloads/DataAgentBench-main/)). Only
specific values (IDs, date cutoffs, thresholds) are invented to make each
probe concrete.

DAB's four failure categories:

1. Ill-formatted key joins
2. Multi-database integration
3. Unstructured text transformation
4. Domain knowledge

## Probe Template

```md
## Probe N
- **Failure category:**
- **Query:**
- **Dataset / Databases involved:**
- **Expected failure:**
- **Observed failure:**
- **Fix applied:**
- **Post-fix score or outcome:**
```

---

## Category 1 — Ill-formatted Key Joins

## Probe 1
- **Failure category:** Ill-formatted key joins
- **Query:** For every Yelp business with at least 5 reviews, return the business name and its average rating.
- **Dataset / Databases involved:** `yelp` — MongoDB `businessinfo_database.business` (`business_id`, `name`, `review_count`) joined against DuckDB `user_database.review` (`business_ref`, `rating`).
- **Expected failure:** Agent joins `business.business_id = review.business_ref` on raw values without normalizing the two string encodings for the same entity; zero matches returned.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

## Probe 2
- **Failure category:** Ill-formatted key joins
- **Query:** List English-language books in the 'Literature & Fiction' category whose average rating is exactly 5.0.
- **Dataset / Databases involved:** `bookreview` — PostgreSQL `books_database.books_info` (`book_id`, `title`, `categories`) joined against SQLite `review_database.review` (`purchase_id`, `rating`).
- **Expected failure:** Agent overlooks the schema note that `review.purchase_id` is the join partner of `books_info.book_id` and either searches for a missing `book_id` column in `review_database` or joins on mismatched names, returning zero rows.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

## Probe 3
- **Failure category:** Ill-formatted key joins
- **Query:** Return the top 5 businesses in Los Angeles, California, ranked by highest average rating.
- **Dataset / Databases involved:** `googlelocal` — SQLite `review_database.review` (`gmap_id`, `rating`) joined against PostgreSQL `business_database.business_description` (`gmap_id`, `name`, `state`).
- **Expected failure:** Agent relies on default collation when joining `gmap_id` across engines; PostgreSQL's default collation folds mixed-case hex IDs differently from SQLite's `BINARY`, silently dropping a fraction of the join.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

## Probe 4
- **Failure category:** Ill-formatted key joins
- **Query:** What is the title of the sports article whose description has the greatest number of characters?
- **Dataset / Databases involved:** `agnews` — MongoDB `articles_database.articles` (`article_id`, `title`, `description`) joined against SQLite `metadata_database.article_metadata` (`article_id`, `region`).
- **Expected failure:** MongoDB returns `article_id` as `int64` via PyMongo while SQLite returns Python `int`; in pandas, nulls promote the column to `float64` and equality misaligns for large IDs.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

## Probe 5
- **Failure category:** Ill-formatted key joins
- **Query:** Compute the total USD revenue for tracks by a given artist.
- **Dataset / Databases involved:** `music_brainz_20k` — SQLite `tracks_database.tracks` (`track_id`, `source_track_id`, `artist`) joined against DuckDB `sales_database.sales` (`track_id`, `revenue_usd`).
- **Expected failure:** Agent joins on `source_track_id` instead of `track_id`, ignoring the schema note that `source_track_id` uniqueness is not guaranteed; duplicate source IDs cause sales from unrelated tracks to be over-counted.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

---

## Category 2 — Multi-Database Integration

## Probe 6
- **Failure category:** Multi-database integration
- **Query:** Yelp q1 — "What is the average rating of all businesses located in Indianapolis, Indiana?"
- **Dataset / Databases involved:** `yelp` — MongoDB `business.description` (contains location text); DuckDB `user_database.review.rating`.
- **Expected failure:** Agent runs only the DuckDB aggregation, filters on a non-existent `city` column, or returns the global average rating instead of filtering businesses by Indianapolis via the Mongo description text.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

## Probe 7
- **Failure category:** Multi-database integration
- **Query:** Yelp q2 — "Which U.S. state has the highest number of reviews, and what is the average rating of businesses in that state?"
- **Dataset / Databases involved:** `yelp` — MongoDB `business.description` for state; DuckDB `user_database.review` for review counts and rating.
- **Expected failure:** Correct state identified but wrong averaging rule (review-weighted vs business-level) produces a numeric mismatch at validator.
- **Observed failure:** Latest run output `PA, 3.68`; remote validator rejected with `Number near 'PA' does not match ≈3.699395770392749`.
- **Fix applied:** Partial. Added explicit state-resolution and cross-DB aggregation path; still needs benchmark-aligned average semantics.
- **Post-fix score or outcome:** `is_valid: false` (2026-04-12 targeted rerun).

## Probe 8
- **Failure category:** Multi-database integration
- **Query:** Googlelocal q3 — "Return the top 5 businesses that remain open after 6:00 PM on at least one weekday, ranked by highest average rating."
- **Dataset / Databases involved:** `googlelocal` — PostgreSQL `business_description` (`hours`, `name`); SQLite `review_database.review.rating`.
- **Expected failure:** Agent writes a single SQL statement joining `business_description` and `review` in PostgreSQL; Postgres errors with `relation "review" does not exist`. Agent fails to route the second half to SQLite and merge in the Python scratchpad.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

## Probe 9
- **Failure category:** Multi-database integration
- **Query:** Agnews q2 — "What fraction of all articles authored by Amy Jones belong to the Science/Technology category?"
- **Dataset / Databases involved:** `agnews` — SQLite `authors.name` → `article_metadata.article_id` → MongoDB `articles.description`.
- **Expected failure:** Agent resolves Amy Jones's `author_id` in SQLite and retrieves `article_id` list, but then filters category against a non-existent `category` column in `article_metadata` instead of classifying from the Mongo `articles.description` text.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

## Probe 10
- **Failure category:** Multi-database integration
- **Query:** CRMArena q1 — "Can lead 00QWt0000089AekMAE be qualified based on the latest discussions? If not, which BANT factors (Budget, Authority, Need, Timeline) does it fail?"
- **Dataset / Databases involved:** `crmarenapro` — DuckDB `sales_pipeline.Lead`; DuckDB `activities.VoiceCallTranscript__c.Body__c`; PostgreSQL `support.knowledge__kav`.
- **Expected failure:** Agent reads `Lead.Status` and returns a BANT label without loading voice transcripts or knowledge articles, or loads transcripts but never consults `knowledge__kav` for the BANT rubric.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

## Probe 11
- **Failure category:** Multi-database integration
- **Query:** For the exchange whose trading currency is JPY, what was the highest Close over the last 180 trading days?
- **Dataset / Databases involved:** `stockindex` — SQLite `indexinfo_database.index_info` (`Exchange`, `Currency`); DuckDB `indextrade_database.index_trade` (`Index`, `Date`, `Close`, `CloseUSD`).
- **Expected failure:** Agent filters `index_trade` on a non-existent `Currency` column and errors, or picks `CloseUSD` instead of `Close`, producing an answer in the wrong currency.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

---

## Category 3 — Unstructured Text Transformation

## Probe 12
- **Failure category:** Unstructured text transformation
- **Query:** Yelp q3 — "During 2018, how many businesses that received reviews offered either business parking or bike parking?"
- **Dataset / Databases involved:** `yelp` — MongoDB `business.attributes` (dict or null; parking sub-keys often stored as Python-literal-encoded strings such as `"{'garage': True, 'street': False}"`); DuckDB `user_database.review.date`.
- **Expected failure:** Pipeline computes intermediate artifacts but the final answer text does not emit the required integer token in a validator-recognized form.
- **Observed failure:** Latest rerun rejected with `Number 35 not found in LLM output.`
- **Fix applied:** Partial. Added parking-aware execution branch and extraction artifacts; final answer formatting still not consistently emitting the validated integer.
- **Post-fix score or outcome:** `is_valid: false` (2026-04-12 targeted rerun).

## Probe 13
- **Failure category:** Unstructured text transformation
- **Query:** Yelp q6 — "Which business received the highest average rating between January 1, 2016 and June 30, 2016, and what category does it belong to? Consider only businesses with at least 5 reviews."
- **Dataset / Databases involved:** `yelp` — DuckDB `user_database.review` (date window, rating); MongoDB `businessinfo_database.business` (`description`, attributes describing category).
- **Expected failure:** Correct business selected but category extraction degrades to a placeholder/unknown value.
- **Observed failure:** Output includes business name `Coffee House Too Cafe` but categories show `Unknown`; validator rejected with `Missing category: restaurants`.
- **Fix applied:** Partial. Added date-windowed top-business path with category hook; category parser fallback still insufficient for this business row.
- **Post-fix score or outcome:** `is_valid: false` (2026-04-12 targeted rerun).

## Probe 14
- **Failure category:** Unstructured text transformation
- **Query:** Bookreview q3 — "Which books categorized as 'Children's Books' have received an average rating of at least 4.5 based on reviews from 2020 onwards?"
- **Dataset / Databases involved:** `bookreview` — PostgreSQL `books_info.categories` (stored as string representation of a list per schema note); SQLite `review.rating`, `review.review_time`.
- **Expected failure:** Agent runs `categories = 'Children''s Books'`, which never matches because the stored value is a stringified list such as `"['Children''s Books', 'Ages 4-8']"`; agent does not `ast.literal_eval` before membership testing.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

## Probe 15
- **Failure category:** Unstructured text transformation
- **Query:** PATENTS q1 — "Identify the CPC technology areas with the highest exponential moving average of patent filings each year (smoothing factor 0.2), and return only the CPC group codes at level 5 whose best year is 2022."
- **Dataset / Databases involved:** `patents` — SQLite `publication_database.publicationinfo` (`filing_date` in natural-language form like `"March 15th, 2020"`, `cpc` as JSON-like string); PostgreSQL `CPCDefinition_database.cpc_definition` (`symbol`, `level`).
- **Expected failure:** Agent parses `filing_date` with `pd.to_datetime(..., errors='coerce')` and drops the ~30% of rows whose natural-language date is not recognized; `cpc` treated as scalar so the level-5 filter misses most rows.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

## Probe 16
- **Failure category:** Unstructured text transformation
- **Query:** CRMArena q5 — "What has been the most frequent problem AI Cirku-Tech encountered over the past five months? The product Id is 01tWt000006hV8LIAU."
- **Dataset / Databases involved:** `crmarenapro` — PostgreSQL `support.Case` (`issueid__c`, `orderitemid__c`, `description`); PostgreSQL `support.livechattranscript.body`; PostgreSQL `support.issue__c` (`id`, `name`, `description__c`); SQLite `products_orders.OrderItem`.
- **Expected failure:** Agent groups by `Case.issueid__c` and returns the mode without reading `livechattranscript.body`, missing unlabeled cases where the issue appears only in chat text. Alternatively, agent keyword-matches `issue__c.name` strings in chat, missing synonym variants.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

---

## Category 4 — Domain Knowledge

## Probe 17
- **Failure category:** Domain knowledge
- **Query:** CRMArena q3 — "Is the stage name accurately representing the tasks for opportunity 006Wt000007BGGjIAO? If not, return one of Qualification, Discovery, Quote, Negotiation, Closed."
- **Dataset / Databases involved:** `crmarenapro` — DuckDB `sales_pipeline.Opportunity.StageName`; DuckDB `activities.Task.Subject` / `.Description`; DuckDB `sales_pipeline.Quote.Status`.
- **Expected failure:** Agent does not know the canonical stage progression (Qualification → Discovery → Quote → Negotiation → Closed) and treats `StageName` as a free label; when a `Quote` row already exists in `Status = 'Presented'` the agent fails to promote the answer to `Quote`.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

## Probe 18
- **Failure category:** Domain knowledge
- **Query:** Among patents filed in the second half of 2019, what fraction were granted to `small entity` assignees?
- **Dataset / Databases involved:** `patents` — SQLite `publication_database.publicationinfo` (`entity_status`, `filing_date`, `grant_date`).
- **Expected failure:** Agent does not recognize the USPTO vocabulary (`small entity`, `large entity`, `micro entity`), filters on the literal string `'small'` and returns zero, or mis-classifies `micro entity` rows.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

## Probe 19
- **Failure category:** Domain knowledge
- **Query:** Among samples with reliable variant calls, which gene has the highest number of missense mutations?
- **Dataset / Databases involved:** `pancancer_atlas` — SQLite `molecular_database.Mutation_Data` (`Hugo_Symbol`, `Variant_Classification`, `FILTER`).
- **Expected failure:** Agent does not apply the oncology-domain rule that only `FILTER = 'PASS'` rows represent reliable variant calls, so artefact calls inflate the leading gene; or agent case-insensitively matches `Variant_Classification`, conflating casing variants.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

## Probe 20
- **Failure category:** Domain knowledge
- **Query:** Bookreview q1 — "Which decade of publication (e.g., 1980s) has the highest average rating among decades with at least 10 distinct books that have been rated?"
- **Dataset / Databases involved:** `bookreview` — PostgreSQL `books_info.details` (publication year embedded in free-text details); SQLite `review.rating`.
- **Expected failure:** Agent does not apply the domain definition that the `1980s` decade spans 1980–1989 inclusive (not 1981–1990), or extracts the year from the wrong field in `details`; resulting decade buckets are off-by-one and the ranking is invalid.
- **Observed failure:** Pending.
- **Fix applied:** Pending.
- **Post-fix score or outcome:** Pending.

---

## Coverage Summary

| DAB failure category                | Probes                    | Count |
| ----------------------------------- | ------------------------- | ----- |
| Ill-formatted key joins             | 1, 2, 3, 4, 5             | 5     |
| Multi-database integration          | 6, 7, 8, 9, 10, 11        | 6     |
| Unstructured text transformation    | 12, 13, 14, 15, 16        | 5     |
| Domain knowledge                    | 17, 18, 19, 20            | 4     |
| **Total**                           |                           | **20**|

DAB datasets exercised: `yelp`, `bookreview`, `googlelocal`, `agnews`,
`music_brainz_20k`, `stockindex`, `crmarenapro`, `patents`, `pancancer_atlas`.
DAB DBMSes exercised: PostgreSQL, MongoDB, SQLite, DuckDB. 